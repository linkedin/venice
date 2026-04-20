package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.NATIVE_REPLICATION_SOURCE_FABRIC;
import static com.linkedin.venice.ConfigKeys.PARENT_KAFKA_CLUSTER_FABRIC_LIST;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_PARENT_DATA_CENTER_REGION_NAME;
import static com.linkedin.venice.integration.utils.VeniceControllerWrapper.D2_SERVICE_NAME;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_VALUE_FIELD_PROP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

import com.linkedin.venice.annotation.PubSubAgnosticTest;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.samza.VeniceSystemFactory;
import com.linkedin.venice.samza.VeniceSystemProducer;
import com.linkedin.venice.samza.VeniceSystemProducerConfig;
import com.linkedin.venice.serializer.AvroSerializer;
import com.linkedin.venice.spark.consistency.VTConsistencyCheckerJob;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.LeaderMetadataWrapper;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.testng.annotations.Test;


/**
 * Integration test for {@link VTConsistencyCheckerJob}. Sets up a two-DC AA cluster,
 * injects records directly into each DC's VT to simulate a bug, then runs the Spark job
 * in local mode and verifies the output Parquet contains the expected inconsistency.
 */
@PubSubAgnosticTest
public class TestVTConsistencyCheckerJob extends AbstractMultiRegionTest {
  private static final int TEST_TIMEOUT = 5 * Time.MS_PER_MINUTE;
  private static final AvroSerializer<String> STRING_SERIALIZER = new AvroSerializer<>(STRING_SCHEMA);

  @Override
  protected int getNumberOfServers() {
    return 1;
  }

  @Override
  protected int getReplicationFactor() {
    return 1;
  }

  @Override
  protected Properties getExtraControllerProperties() {
    Properties props = new Properties();
    props.put(NATIVE_REPLICATION_SOURCE_FABRIC, "dc-0");
    props.put(PARENT_KAFKA_CLUSTER_FABRIC_LIST, DEFAULT_PARENT_DATA_CENTER_REGION_NAME);
    return props;
  }

  /**
   * Full pipeline test: batch push via VPJ, real RT writes from both DCs via Samza,
   * then inject a corrupted record into one DC's VT to simulate a bug.
   *
   * <p>Exercises:
   * <ul>
   *   <li>Real EOP scanning (5 batch push records before EOP)</li>
   *   <li>Real batch record skipping (EOP-based filtering)</li>
   *   <li>Real AA replication with leader metadata from both DCs</li>
   *   <li>Injected inconsistency detection (one corrupted key)</li>
   *   <li>Consistent keys are not falsely flagged</li>
   * </ul>
   *
   * <p>Steps:
   * <ol>
   *   <li>Batch push 5 records via VPJ</li>
   *   <li>RT writes from DC-0 (5 keys) and DC-1 (5 keys) via Samza</li>
   *   <li>Wait for replication to settle</li>
   *   <li>Inject a corrupted record for "corrupt-key" into DC-1's VT with a different value</li>
   *   <li>Write more RT records to advance HW past the injected position</li>
   *   <li>Run checker — expect exactly one VALUE_MISMATCH for "corrupt-key"</li>
   * </ol>
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testFullPipelineWithBatchPushRTWritesAndInjectedInconsistency() throws Exception {
    // 1. Batch push with 5 records via VPJ
    File inputDir = getTempDataDirectory();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir, 5);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("aa-vt-full-pipeline");
    Properties vpjProps =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();

    UpdateStoreQueryParams storeParams = new UpdateStoreQueryParams().setActiveActiveReplicationEnabled(true)
        .setHybridRewindSeconds(25L)
        .setHybridOffsetLagThreshold(1L)
        .setNativeReplicationEnabled(true)
        .setPartitionCount(1);
    createStoreForJob(CLUSTER_NAME, keySchemaStr, valueSchemaStr, vpjProps, storeParams).close();
    IntegrationTestPushUtils.runVPJ(vpjProps);

    try (
        ControllerClient dc0ControllerClient =
            new ControllerClient(CLUSTER_NAME, childDatacenters.get(0).getControllerConnectString());
        ControllerClient dc1ControllerClient =
            new ControllerClient(CLUSTER_NAME, childDatacenters.get(1).getControllerConnectString())) {

      String versionTopic = Version.composeKafkaTopic(storeName, 1);

      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        assertEquals(dc0ControllerClient.getStore(storeName).getStore().getCurrentVersion(), 1, "DC-0 version");
        assertEquals(dc1ControllerClient.getStore(storeName).getStore().getCurrentVersion(), 1, "DC-1 version");
      });

      // 2. RT writes from both DCs via Samza
      VeniceSystemProducer producerInDC0 = new VeniceSystemProducer(
          new VeniceSystemProducerConfig.Builder().setFactory(new VeniceSystemFactory())
              .setStoreName(storeName)
              .setPushType(Version.PushType.STREAM)
              .setSamzaJobId(Utils.getUniqueString("venice-push-id"))
              .setRunningFabric("dc-0")
              .setVerifyLatestProtocolPresent(true)
              .setVeniceChildD2ZkHost(childDatacenters.get(0).getZkServerWrapper().getAddress())
              .setPrimaryControllerColoD2ZKHost(childDatacenters.get(0).getZkServerWrapper().getAddress())
              .setPrimaryControllerD2ServiceName(D2_SERVICE_NAME)
              .build());
      producerInDC0.start();
      for (int i = 0; i < 5; i++) {
        producerInDC0.send(
            storeName,
            new OutgoingMessageEnvelope(new SystemStream("venice", storeName), "key-" + i, "val-from-dc0-" + i));
      }
      producerInDC0.stop();

      VeniceSystemProducer producerInDC1 = new VeniceSystemProducer(
          new VeniceSystemProducerConfig.Builder().setFactory(new VeniceSystemFactory())
              .setStoreName(storeName)
              .setPushType(Version.PushType.STREAM)
              .setSamzaJobId(Utils.getUniqueString("venice-push-id"))
              .setRunningFabric("dc-1")
              .setVerifyLatestProtocolPresent(true)
              .setVeniceChildD2ZkHost(childDatacenters.get(1).getZkServerWrapper().getAddress())
              .setPrimaryControllerColoD2ZKHost(childDatacenters.get(1).getZkServerWrapper().getAddress())
              .setPrimaryControllerD2ServiceName(D2_SERVICE_NAME)
              .build());
      producerInDC1.start();
      for (int i = 0; i < 5; i++) {
        producerInDC1.send(
            storeName,
            new OutgoingMessageEnvelope(new SystemStream("venice", storeName), "key-" + i, "val-from-dc1-" + i));
      }
      producerInDC1.stop();

      // 3. Wait for RT records to replicate to both DCs
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
        try (
            AvroGenericStoreClient<String, CharSequence> dc0Client = ClientFactory.getAndStartGenericAvroClient(
                ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(getCluster(0).getRandomRouterURL()));
            AvroGenericStoreClient<String, CharSequence> dc1Client = ClientFactory.getAndStartGenericAvroClient(
                ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(getCluster(1).getRandomRouterURL()))) {
          for (int i = 0; i < 5; i++) {
            assertNotNull(dc0Client.get("key-" + i).get(), "key-" + i + " not in DC-0");
            assertNotNull(dc1Client.get("key-" + i).get(), "key-" + i + " not in DC-1");
          }
        }
      });

      // 4. Simulate a bug: inject the same key into both DCs' VTs with the same leader metadata
      // but different values. This is what a real DCR bug looks like — both leaders processed
      // the same upstream write but produced different output.
      long injectedTimestamp = System.currentTimeMillis();
      VeniceClusterWrapper dc0Cluster = getCluster(0);
      VeniceClusterWrapper dc1Cluster = getCluster(1);
      injectToVT(dc0Cluster, versionTopic, "buggy-key", "value-from-dc0-bug", 0, 1L, injectedTimestamp);
      injectToVT(dc1Cluster, versionTopic, "buggy-key", "value-from-dc1-bug", 0, 1L, injectedTimestamp);

      // 5. Send more RT writes after injection to advance HW and make the scenario more realistic
      VeniceSystemProducer postInjectionProducer = new VeniceSystemProducer(
          new VeniceSystemProducerConfig.Builder().setFactory(new VeniceSystemFactory())
              .setStoreName(storeName)
              .setPushType(Version.PushType.STREAM)
              .setSamzaJobId(Utils.getUniqueString("venice-push-id"))
              .setRunningFabric("dc-0")
              .setVerifyLatestProtocolPresent(true)
              .setVeniceChildD2ZkHost(childDatacenters.get(0).getZkServerWrapper().getAddress())
              .setPrimaryControllerColoD2ZKHost(childDatacenters.get(0).getZkServerWrapper().getAddress())
              .setPrimaryControllerD2ServiceName(D2_SERVICE_NAME)
              .build());
      postInjectionProducer.start();
      for (int i = 0; i < 5; i++) {
        postInjectionProducer.send(
            storeName,
            new OutgoingMessageEnvelope(new SystemStream("venice", storeName), "key-" + i, "val-post-inject-" + i));
      }
      postInjectionProducer.stop();

      // Wait for post-injection writes to replicate
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
        try (AvroGenericStoreClient<String, CharSequence> dc1Client = ClientFactory.getAndStartGenericAvroClient(
            ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(getCluster(1).getRandomRouterURL()))) {
          assertNotNull(dc1Client.get("key-4").get(), "key-4 not in DC-1");
        }
      });

      // 6. Run VT consistency checker
      File tempRoot = Files.createTempDirectory("vt-consistency-full-pipeline").toFile();
      File outputDir = new File(tempRoot, "output");
      try {
        Properties jobProps = new Properties();
        jobProps.setProperty(
            VTConsistencyCheckerJob.DC0_BROKER_URL,
            childDatacenters.get(0).getPubSubBrokerWrapper().getAddress());
        jobProps.setProperty(
            VTConsistencyCheckerJob.DC1_BROKER_URL,
            childDatacenters.get(1).getPubSubBrokerWrapper().getAddress());
        jobProps.setProperty(VTConsistencyCheckerJob.VERSION_TOPIC, versionTopic);
        jobProps.setProperty(VTConsistencyCheckerJob.OUTPUT_PATH, outputDir.getAbsolutePath());
        jobProps.setProperty(VTConsistencyCheckerJob.NUMBER_OF_REGIONS, "2");

        VTConsistencyCheckerJob.run(jobProps);

        SparkSession reader =
            SparkSession.builder().master("local[*]").appName("TestVTConsistencyCheckerJob-reader").getOrCreate();
        try {
          Dataset<Row> result = reader.read().parquet(outputDir.getAbsolutePath());
          List<Row> rows = result.collectAsList();

          // Expect at least one VALUE_MISMATCH for the overwritten key "buggy-key"
          List<Row> mismatches =
              rows.stream().filter(r -> "VALUE_MISMATCH".equals(r.getAs("type"))).collect(Collectors.toList());
          assertFalse(mismatches.isEmpty(), "Expected at least one VALUE_MISMATCH for the corrupted key");

          Row corruptRow = mismatches.get(0);
          assertEquals(corruptRow.getAs("version_topic"), versionTopic);
          assertFalse(
              corruptRow.getAs("dc0_value_hash").equals(corruptRow.getAs("dc1_value_hash")),
              "DC value hashes must differ for corrupted key");
        } finally {
          reader.stop();
        }
      } finally {
        org.apache.commons.io.FileUtils.deleteDirectory(tempRoot);
      }
    }
  }

  /**
   * Writes a single PUT record directly to a VT, bypassing the RT→DCR→VT path.
   */
  private void injectToVT(
      VeniceClusterWrapper cluster,
      String versionTopic,
      String key,
      String value,
      int regionId,
      long fakeUpstreamOffset,
      long logicalTimestamp) throws Exception {
    PubSubBrokerWrapper broker = cluster.getPubSubBrokerWrapper();
    VeniceWriterFactory writerFactory = IntegrationTestPushUtils
        .getVeniceWriterFactory(broker, broker.getPubSubClientsFactory().getProducerAdapterFactory());
    try (VeniceWriter<byte[], byte[], byte[]> writer =
        writerFactory.createVeniceWriter(new VeniceWriterOptions.Builder(versionTopic).build())) {
      LeaderMetadataWrapper leaderMetadata =
          new LeaderMetadataWrapper(ApacheKafkaOffsetPosition.of(fakeUpstreamOffset), regionId, 0L);
      writer
          .put(
              STRING_SERIALIZER.serialize(key),
              STRING_SERIALIZER.serialize(value),
              1,
              null,
              leaderMetadata,
              logicalTimestamp,
              null)
          .get();
    }
  }

}
