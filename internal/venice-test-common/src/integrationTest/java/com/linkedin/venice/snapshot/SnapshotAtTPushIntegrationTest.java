package com.linkedin.venice.snapshot;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.hadoop.snapshot.SnapshotAtTPushExecutor;
import com.linkedin.venice.hadoop.snapshot.SnapshotAtTRecordMerger;
import com.linkedin.venice.hadoop.snapshot.SnapshotAtTRtReader;
import com.linkedin.venice.hadoop.snapshot.SnapshotAtTRtRecord;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.samza.system.SystemProducer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * End-to-end multi-region test of the snapshot-at-T data plane: produce real-time data to TWO regions of an
 * Active-Active hybrid store, then run the new-mode push (read both regions' RT, merge with a batch dataset via
 * {@link SnapshotAtTRecordMerger}, produce the merged value + RMD to a new version with a shortened rewind), and
 * validate the served data equals the correct merged result (the same state the traditional batch-push + full
 * rewind would have produced).
 */
public class SnapshotAtTPushIntegrationTest {
  private static final int NUMBER_OF_REGIONS = 2;
  private static final String STRING_SCHEMA = "\"string\"";
  private static final long REGION_0_TS = 1000L;
  private static final long REGION_1_TS = 2000L;

  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegion;
  private List<VeniceMultiClusterWrapper> childRegions;
  private String clusterName;
  private ControllerClient parentControllerClient;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    VeniceMultiRegionClusterCreateOptions options =
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(NUMBER_OF_REGIONS)
            .numberOfClusters(1)
            .numberOfParentControllers(1)
            .numberOfChildControllers(1)
            .numberOfServers(2)
            .numberOfRouters(1)
            .replicationFactor(1)
            .forkServer(false)
            .build();
    multiRegion = ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(options);
    childRegions = multiRegion.getChildRegions();
    clusterName = multiRegion.getClusterNames()[0];
    parentControllerClient = new ControllerClient(clusterName, multiRegion.getControllerConnectString());
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    Utils.closeQuietlyWithErrorLogged(parentControllerClient);
    Utils.closeQuietlyWithErrorLogged(multiRegion);
  }

  @Test(timeOut = 300_000)
  public void testSnapshotAtTMergeAcrossTwoRegions() throws Exception {
    String storeName = Utils.getUniqueString("snapshot_at_t");
    int partitionCount = 2;

    // 1. Create an A/A hybrid store (string -> string).
    TestUtils.assertCommand(parentControllerClient.createNewStore(storeName, "owner", STRING_SCHEMA, STRING_SCHEMA));
    TestUtils.assertCommand(
        parentControllerClient.updateStore(
            storeName,
            new UpdateStoreQueryParams().setHybridRewindSeconds(3600L)
                .setHybridOffsetLagThreshold(2L)
                .setPartitionCount(partitionCount)
                .setReplicationFactor(1)
                .setNativeReplicationEnabled(true)
                .setActiveActiveReplicationEnabled(true)));

    // Version 1 (empty push) to materialize the store version + RT topics, then wait for it to come online.
    VersionCreationResponse v1 = TestUtils.assertCommand(parentControllerClient.emptyPush(storeName, "v1-init", 1000));
    TestUtils.waitForNonDeterministicPushCompletion(v1.getKafkaTopic(), parentControllerClient, 60, TimeUnit.SECONDS);

    StoreInfo storeInfo = TestUtils.assertCommand(parentControllerClient.getStore(storeName)).getStore();
    String rtTopicName = Utils.getRealTimeTopicName(storeInfo);

    // 2. Produce RT to BOTH regions. Region 1's writes use a newer timestamp, so for the key both regions touch
    // (key 5) region 1 wins under Active-Active conflict resolution.
    SystemProducer region0Producer = IntegrationTestPushUtils.getSamzaProducerForStream(multiRegion, 0, storeName);
    SystemProducer region1Producer = IntegrationTestPushUtils.getSamzaProducerForStream(multiRegion, 1, storeName);
    try {
      for (int k: new int[] { 3, 4, 5 }) {
        IntegrationTestPushUtils
            .sendStreamingRecord(region0Producer, storeName, Integer.toString(k), "r0_" + k, REGION_0_TS);
      }
      for (int k: new int[] { 5, 6, 7 }) {
        IntegrationTestPushUtils
            .sendStreamingRecord(region1Producer, storeName, Integer.toString(k), "r1_" + k, REGION_1_TS);
      }
    } finally {
      region0Producer.stop();
      region1Producer.stop();
    }

    // Age the RT past the short rewind window (set to 5s on v2 below) so the served data reflects only the
    // snapshot-at-T merge, not the server's post-EOP rewind re-applying the RT.
    Utils.sleep(10_000L);

    // 3. Batch dataset: keys 1..8 all carry a batch value.
    VeniceAvroKafkaSerializer stringSerializer = new VeniceAvroKafkaSerializer(STRING_SCHEMA);
    Map<ByteBuffer, ByteBuffer> batchValuesByKey = new HashMap<>();
    for (int k = 1; k <= 8; k++) {
      batchValuesByKey.put(
          ByteBuffer.wrap(stringSerializer.serialize(null, Integer.toString(k))),
          ByteBuffer.wrap(stringSerializer.serialize(null, "batch_" + k)));
    }

    // 4. Read RT from both regions up to now (cutoff <= 0 => read to the current tail), tagging each region's
    // records with its colo id.
    SnapshotAtTRtReader rtReader = new SnapshotAtTRtReader();
    List<SnapshotAtTRtRecord> rtRecords = new ArrayList<>();
    for (int region = 0; region < NUMBER_OF_REGIONS; region++) {
      PubSubBrokerWrapper broker = childRegions.get(region).getClusters().get(clusterName).getPubSubBrokerWrapper();
      VeniceProperties consumerProps = brokerConsumerProps(broker);
      rtRecords
          .addAll(rtReader.readRegion(consumerProps, broker.getAddress(), rtTopicName, partitionCount, 0L, region));
    }
    // Sanity: 3 records from region 0 + 3 from region 1.
    assertEquals(rtRecords.size(), 6, "Expected to read 6 RT records across both regions");

    // 5. Request a new version with a short rewind override (the snapshot-at-T optimization), then produce the
    // merged value + RMD to it with SOP/EOP.
    int rmdVersion = RmdSchemaGenerator.getLatestVersion();
    SnapshotAtTRecordMerger merger =
        new SnapshotAtTRecordMerger(buildSchemaRepository(rmdVersion), storeName, rmdVersion, false);

    VersionCreationResponse v2 = TestUtils.assertCommand(
        parentControllerClient.requestTopicForWrites(
            storeName,
            1024 * 1024,
            Version.PushType.BATCH,
            Utils.getUniqueString("snapshot-at-t-push"),
            false, // sendStartOfPush: we send SOP ourselves
            false, // sorted
            false,
            java.util.Optional.empty(),
            java.util.Optional.empty(),
            java.util.Optional.empty(),
            false,
            5L, // rewindTimeInSecondsOverride: short rewind, the snapshot-at-T optimization
            false,
            null,
            -1,
            false,
            -1));
    String versionTopic = v2.getKafkaTopic();

    PubSubBrokerWrapper destBroker = childRegions.get(0).getClusters().get(clusterName).getPubSubBrokerWrapper();
    PubSubProducerAdapterFactory producerFactory = destBroker.getPubSubClientsFactory().getProducerAdapterFactory();
    try (VeniceWriter<byte[], byte[], byte[]> veniceWriter =
        IntegrationTestPushUtils.getVeniceWriterFactory(destBroker, producerFactory)
            .createVeniceWriter(new VeniceWriterOptions.Builder(versionTopic).build())) {
      veniceWriter.broadcastStartOfPush(false, Collections.emptyMap());
      VeniceCompressor compressor = new CompressorFactory().getCompressor(CompressionStrategy.NO_OP);
      SnapshotAtTPushExecutor.Stats stats =
          new SnapshotAtTPushExecutor().execute(veniceWriter, batchValuesByKey, 1, rtRecords, merger, compressor);
      assertEquals(stats.getTotalProduced(), 8, "Expected 8 merged records produced");
      veniceWriter.broadcastEndOfPush(Collections.emptyMap());
    }

    TestUtils.waitForNonDeterministicPushCompletion(versionTopic, parentControllerClient, 90, TimeUnit.SECONDS);

    // 6. Validate the served data equals the correct merged result in each region.
    Map<String, String> expected = new HashMap<>();
    expected.put("1", "batch_1");
    expected.put("2", "batch_2");
    expected.put("3", "r0_3"); // region 0 RT wins over batch sentinel
    expected.put("4", "r0_4");
    expected.put("5", "r1_5"); // region 1 (ts 2000) wins over region 0 (ts 1000) and batch
    expected.put("6", "r1_6");
    expected.put("7", "r1_7");
    expected.put("8", "batch_8");

    for (VeniceMultiClusterWrapper region: childRegions) {
      VeniceClusterWrapper cluster = region.getClusters().get(clusterName);
      try (AvroGenericStoreClient<String, Object> client = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(cluster.getRandomRouterURL()))) {
        TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
          for (Map.Entry<String, String> entry: expected.entrySet()) {
            Object value = client.get(entry.getKey()).get();
            assertNotNull(value, "Key " + entry.getKey() + " missing in region " + region.getRegionName());
            assertEquals(
                value.toString(),
                entry.getValue(),
                "Key " + entry.getKey() + " wrong value in region " + region.getRegionName());
          }
        });
      }
    }
  }

  @DataProvider(name = "compressionStrategies")
  public static Object[][] compressionStrategies() {
    return new Object[][] { { CompressionStrategy.NO_OP }, { CompressionStrategy.ZSTD_WITH_DICT } };
  }

  /**
   * The same scenario driven end to end by a single {@link com.linkedin.venice.hadoop.VenicePushJob} run, for both
   * NO_OP and ZSTD_WITH_DICT compression: the snapshot-at-T mode (enabled with a 0s threshold so it triggers on the
   * store's 3600s rewind) reads the batch Avro input and both regions' RT, merges, compresses, and produces the new
   * version.
   */
  @Test(timeOut = 300_000, dataProvider = "compressionStrategies")
  public void testSnapshotAtTViaVenicePushJob(CompressionStrategy compressionStrategy) throws Exception {
    String storeName = Utils.getUniqueString("snapshot_at_t_vpj");
    int partitionCount = 2;
    TestUtils.assertCommand(parentControllerClient.createNewStore(storeName, "owner", STRING_SCHEMA, STRING_SCHEMA));
    TestUtils.assertCommand(
        parentControllerClient.updateStore(
            storeName,
            new UpdateStoreQueryParams().setHybridRewindSeconds(3600L)
                .setHybridOffsetLagThreshold(2L)
                .setPartitionCount(partitionCount)
                .setReplicationFactor(1)
                .setNativeReplicationEnabled(true)
                .setActiveActiveReplicationEnabled(true)
                .setCompressionStrategy(compressionStrategy)));
    VersionCreationResponse v1 = TestUtils.assertCommand(parentControllerClient.emptyPush(storeName, "v1-init", 1000));
    TestUtils.waitForNonDeterministicPushCompletion(v1.getKafkaTopic(), parentControllerClient, 60, TimeUnit.SECONDS);

    SystemProducer region0Producer = IntegrationTestPushUtils.getSamzaProducerForStream(multiRegion, 0, storeName);
    SystemProducer region1Producer = IntegrationTestPushUtils.getSamzaProducerForStream(multiRegion, 1, storeName);
    try {
      for (int k: new int[] { 3, 4, 5 }) {
        IntegrationTestPushUtils
            .sendStreamingRecord(region0Producer, storeName, Integer.toString(k), "r0_" + k, REGION_0_TS);
      }
      for (int k: new int[] { 5, 6, 7 }) {
        IntegrationTestPushUtils
            .sendStreamingRecord(region1Producer, storeName, Integer.toString(k), "r1_" + k, REGION_1_TS);
      }
    } finally {
      region0Producer.stop();
      region1Producer.stop();
    }

    // Let the RT writes age past the short rewind window the snapshot mode applies, so the served data comes
    // purely from the snapshot-at-T merge and not from the server's post-EOP rewind re-applying the RT. If the
    // merge were broken (RT not folded in), the served values would be the batch ones and the test would fail.
    Utils.sleep(10_000L);

    // Batch Avro input: keys 1..100 -> "test_name_<k>".
    File inputDir = TestWriteUtils.getTempDataDirectory();
    TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();

    String broker0 = childRegions.get(0).getClusters().get(clusterName).getPubSubBrokerWrapper().getAddress();
    String broker1 = childRegions.get(1).getClusters().get(clusterName).getPubSubBrokerWrapper().getAddress();
    Properties vpjProps = IntegrationTestPushUtils.defaultVPJProps(multiRegion, inputDirPath, storeName);
    vpjProps.setProperty("snapshot.at.t.rewind.enabled", "true");
    vpjProps.setProperty("snapshot.at.t.min.rewind.threshold.seconds", "0");
    vpjProps.setProperty("snapshot.at.t.rewind.buffer.seconds", "2");
    vpjProps.setProperty("snapshot.at.t.rt.region.brokers", "0=" + broker0 + ";1=" + broker1);

    IntegrationTestPushUtils.runVPJ(vpjProps, 2, parentControllerClient);

    // Prove the snapshot-at-T mode actually triggered: v2's rewind was overridden to the short window (well
    // below the store's 3600s). A normal push would have left it at 3600s. This is also why the RT, aged out
    // above, is not re-applied by the rewind.
    StoreInfo afterPush = TestUtils.assertCommand(parentControllerClient.getStore(storeName)).getStore();
    long v2RewindSeconds = afterPush.getVersion(2).get().getHybridStoreConfig().getRewindTimeInSeconds();
    org.testng.Assert.assertTrue(
        v2RewindSeconds < 60,
        "Snapshot-at-T should have shortened v2's rewind, but it is " + v2RewindSeconds + "s (store is 3600s)");

    Map<String, String> expected = new HashMap<>();
    expected.put("1", "test_name_1");
    expected.put("2", "test_name_2");
    expected.put("3", "r0_3");
    expected.put("4", "r0_4");
    expected.put("5", "r1_5");
    expected.put("6", "r1_6");
    expected.put("7", "r1_7");
    expected.put("8", "test_name_8");

    for (VeniceMultiClusterWrapper region: childRegions) {
      VeniceClusterWrapper cluster = region.getClusters().get(clusterName);
      try (AvroGenericStoreClient<String, Object> client = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(cluster.getRandomRouterURL()))) {
        TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
          for (Map.Entry<String, String> entry: expected.entrySet()) {
            Object value = client.get(entry.getKey()).get();
            assertNotNull(value, "Key " + entry.getKey() + " missing in region " + region.getRegionName());
            assertEquals(
                value.toString(),
                entry.getValue(),
                "Key " + entry.getKey() + " wrong value in region " + region.getRegionName());
          }
        });
      }
    }
  }

  private VeniceProperties brokerConsumerProps(PubSubBrokerWrapper broker) {
    Properties properties = new Properties();
    properties.setProperty(KAFKA_BOOTSTRAP_SERVERS, broker.getAddress());
    properties.putAll(PubSubBrokerWrapper.getBrokerDetailsForClients(Collections.singletonList(broker)));
    return new VeniceProperties(properties);
  }

  /** A minimal schema repository over the store's value + RMD schemas, enough for the value-level merge. */
  private ReadOnlySchemaRepository buildSchemaRepository(int rmdVersion) {
    Schema valueSchema = new Schema.Parser().parse(STRING_SCHEMA);
    Schema rmdSchema = RmdSchemaGenerator.generateMetadataSchema(valueSchema, rmdVersion);
    ReadOnlySchemaRepository repository = mock(ReadOnlySchemaRepository.class);
    when(repository.getValueSchema(anyString(), anyInt())).thenReturn(new SchemaEntry(1, valueSchema));
    when(repository.getSupersetOrLatestValueSchema(anyString())).thenReturn(new SchemaEntry(1, valueSchema));
    when(repository.getSupersetSchema(anyString())).thenReturn(new SchemaEntry(1, valueSchema));
    when(repository.getReplicationMetadataSchema(anyString(), anyInt(), anyInt()))
        .thenReturn(new RmdSchemaEntry(1, rmdVersion, rmdSchema));
    return repository;
  }
}
