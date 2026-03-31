package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.NATIVE_REPLICATION_SOURCE_FABRIC;
import static com.linkedin.venice.ConfigKeys.PARENT_KAFKA_CLUSTER_FABRIC_LIST;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_PARENT_DATA_CENTER_REGION_NAME;
import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static com.linkedin.venice.utils.TestUtils.updateStoreToHybrid;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_SCHEMA;
import static org.testng.Assert.*;

import com.linkedin.venice.acl.VeniceComponent;
import com.linkedin.venice.annotation.PubSubAgnosticTest;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterContext;
import com.linkedin.venice.pubsub.PubSubPositionDeserializer;
import com.linkedin.venice.pubsub.PubSubPositionTypeRegistry;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.pubsub.manager.TopicManagerContext;
import com.linkedin.venice.pubsub.manager.TopicManagerRepository;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.spark.consistency.VTConsistencyChecker;
import com.linkedin.venice.spark.consistency.VTConsistencyCheckerJob;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.vpj.pubsub.input.PubSubPartitionSplit;
import com.linkedin.venice.vpj.pubsub.input.PubSubSplitIterator;
import com.linkedin.venice.writer.LeaderMetadataWrapper;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.testng.annotations.Test;


/**
 * Integration tests for Active-Active replication VT (Version Topic) consistency in a two-datacenter setup.
 *
 * <p>In a two-colo AA setup, the leader in each DC reads from both local and remote real-time topics
 * and forwards winning records (after DCR) to the local VT so followers can ingest them. Both DCs'
 * VTs should therefore converge to the same final value for every key.
 *
 * <p>Records are injected directly into each DC's VT (bypassing the RT→DCR path) to simulate a bug
 * where the two leaders disagree. Two keys are used:
 * <ul>
 *   <li><b>hawk</b> — written with large upstream offsets purely to advance the running
 *       high-watermark to a known value before the wolf records land.</li>
 *   <li><b>wolf — value mismatch bug:</b> both VTs contain writes from <em>both</em> colos
 *       (no {@code -1} in the offset vector), meaning each leader had full information, yet they
 *       end up with different final values ({@code arctic-wolf} vs {@code dire-wolf}).</li>
 * </ul>
 *
 * <p>Expected per-DC final state for wolf:
 * <pre>
 *   DC-0: value=arctic-wolf  offsetVector=[5, 10]  highWatermark=[50, 60]
 *   DC-1: value=dire-wolf    offsetVector=[10, 15]  highWatermark=[20, 30]
 * </pre>
 */
@PubSubAgnosticTest
public class TestAAReplicationVTConsistency extends AbstractMultiRegionTest {
  private static final int TEST_TIMEOUT = 5 * Time.MS_PER_MINUTE;

  // HW-advancing helper: injected with large offsets so the running high-watermark reaches a known
  // value before the wolf records land, making HW visibly larger than per-key offset vectors.
  private static final String HAWK = "hawk";
  // Subject of the inconsistency: both VTs show writes from both colos yet disagree on the winner.
  private static final String WOLF = "wolf";

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
   * Creates a two-DC AA store, runs an empty push, then injects records directly into each DC's VT
   * to simulate a bug where the two leaders disagree on the final value for {@code wolf} despite
   * both having seen data from both colos.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testVTConsistencyInTwoColoAASetup() throws Exception {
    String storeName = Utils.getUniqueString("aa-vt-consistency");

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, getParentControllerUrl());
        ControllerClient dc0ControllerClient =
            new ControllerClient(CLUSTER_NAME, childDatacenters.get(0).getControllerConnectString());
        ControllerClient dc1ControllerClient =
            new ControllerClient(CLUSTER_NAME, childDatacenters.get(1).getControllerConnectString())) {

      // Create store with AA + native replication enabled
      assertCommand(
          parentControllerClient
              .createNewStore(storeName, "test-owner", STRING_SCHEMA.toString(), STRING_SCHEMA.toString()));
      assertCommand(parentControllerClient.updateStore(storeName, new UpdateStoreQueryParams().setPartitionCount(1)));
      updateStoreToHybrid(storeName, parentControllerClient, Optional.of(true), Optional.of(true), Optional.empty());

      // Wait for store to be visible in both DCs
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        assertNotNull(dc0ControllerClient.getStore(storeName).getStore(), "Store not visible in DC-0");
        assertNotNull(dc1ControllerClient.getStore(storeName).getStore(), "Store not visible in DC-1");
      });

      // Empty push to create v1 VT in both DCs
      VersionCreationResponse versionResponse =
          assertCommand(parentControllerClient.emptyPush(storeName, Utils.getUniqueString("push-id"), 1L));
      String versionTopic = versionResponse.getKafkaTopic();

      TestUtils.waitForNonDeterministicAssertion(
          60,
          TimeUnit.SECONDS,
          () -> assertEquals(
              parentControllerClient.queryJobStatus(versionTopic).getStatus(),
              ExecutionStatus.COMPLETED.toString(),
              "Push did not complete"));

      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        assertEquals(dc0ControllerClient.getStore(storeName).getStore().getCurrentVersion(), 1, "DC-0 version");
        assertEquals(dc1ControllerClient.getStore(storeName).getStore().getCurrentVersion(), 1, "DC-1 version");
      });

      VeniceClusterWrapper dc0Cluster = getCluster(0);
      VeniceClusterWrapper dc1Cluster = getCluster(1);

      // Inject hawk first into each VT to push the running high-watermark to a known value.
      // Wolf's per-key offsets are intentionally smaller than hawk's, so HW >> offsetVector.
      //
      // Inject hawk first into each VT to push the running high-watermark to a known value.
      // Wolf's per-key offsets are intentionally smaller than hawk's, so HW >> offsetVector.
      //
      // DC-0: hawk advances HW to [50, 60]; wolf lands with offsetVector=[5, 10]
      // logicalTimestamp=200 → "arctic-wolf" has the higher DCR timestamp → DC-0 is correct
      injectBugToVT(dc0Cluster, versionTopic, HAWK, "hawk", 0, 50L, 100L);
      injectBugToVT(dc0Cluster, versionTopic, HAWK, "hawk", 1, 60L, 110L);
      injectBugToVT(dc0Cluster, versionTopic, WOLF, "grey-wolf", 0, 5L, 150L);
      injectBugToVT(dc0Cluster, versionTopic, WOLF, "arctic-wolf", 1, 10L, 200L); // correct winner

      // DC-1: hawk advances HW to [20, 30]; wolf lands with offsetVector=[10, 15]
      // logicalTimestamp=180 → "dire-wolf" has a lower DCR timestamp → DC-1 is wrong
      injectBugToVT(dc1Cluster, versionTopic, HAWK, "hawk", 0, 20L, 100L);
      injectBugToVT(dc1Cluster, versionTopic, HAWK, "hawk", 1, 30L, 110L);
      injectBugToVT(dc1Cluster, versionTopic, WOLF, "grey-wolf", 0, 10L, 150L);
      injectBugToVT(dc1Cluster, versionTopic, WOLF, "dire-wolf", 1, 15L, 180L); // wrong winner — lower logicalTs

      verifyVTConsistency(
          childDatacenters.get(0).getPubSubBrokerWrapper(),
          childDatacenters.get(1).getPubSubBrokerWrapper(),
          storeName,
          versionTopic);
    }
  }

  /**
   * Builds snapshots for both DCs using {@link VTConsistencyChecker}, runs the lily-pad algorithm,
   * and asserts that no inconsistencies are found.
   */
  private void verifyVTConsistency(
      PubSubBrokerWrapper dc0Broker,
      PubSubBrokerWrapper dc1Broker,
      String storeName,
      String versionTopic) throws Exception {
    Properties dc0Props = new Properties();
    dc0Props.setProperty(KAFKA_BOOTSTRAP_SERVERS, dc0Broker.getAddress());
    Properties dc1Props = new Properties();
    dc1Props.setProperty(KAFKA_BOOTSTRAP_SERVERS, dc1Broker.getAddress());

    PubSubTopicRepository topicRepository = new PubSubTopicRepository();
    PubSubTopic topic = topicRepository.getTopic(versionTopic);
    PubSubTopicPartition tp = new PubSubTopicPartitionImpl(topic, 0);

    try (TopicManager dc0TopicManager = createTopicManager(dc0Broker, new VeniceProperties(dc0Props), topicRepository);
        TopicManager dc1TopicManager = createTopicManager(dc1Broker, new VeniceProperties(dc1Props), topicRepository)) {
      PubSubPositionDeserializer positionDeserializer =
          new PubSubPositionDeserializer(dc0Broker.getPubSubPositionTypeRegistry());

      VTConsistencyChecker.Snapshot dc0Snapshot = buildSnapshotForPartition(
          dc0Broker,
          new VeniceProperties(dc0Props),
          topicRepository,
          tp,
          "DC-0-vtConsumer",
          dc0TopicManager,
          positionDeserializer);
      VTConsistencyChecker.Snapshot dc1Snapshot = buildSnapshotForPartition(
          dc1Broker,
          new VeniceProperties(dc1Props),
          topicRepository,
          tp,
          "DC-1-vtConsumer",
          dc1TopicManager,
          positionDeserializer);

      List<VTConsistencyChecker.Inconsistency> inconsistencies =
          VTConsistencyChecker.findInconsistencies(dc0Snapshot, dc1Snapshot, dc0TopicManager, dc1TopicManager, tp);

      System.out.println("\n########## LILY-PAD VT CONSISTENCY CHECK ##########");
      System.out.printf("  store=%s  topic=%s  partition=0%n", storeName, versionTopic);
      for (VTConsistencyChecker.Inconsistency inc: inconsistencies) {
        switch (inc.type) {
          case MISSING_IN_DC0:
            System.out.printf(
                "[MISSING]       key=%-12s absent in DC-0 | DC-1 final: value=%s OV=%s%n",
                inc.key,
                inc.dc1Record.valueHash,
                inc.dc1Record.upstreamRTOffset);
            break;
          case MISSING_IN_DC1:
            System.out.printf(
                "[MISSING]       key=%-12s absent in DC-1 | DC-0 final: value=%s OV=%s%n",
                inc.key,
                inc.dc0Record.valueHash,
                inc.dc0Record.upstreamRTOffset);
            break;
          case VALUE_MISMATCH:
            String expectedWinner = inc.dc0Record.logicalTimestamp >= inc.dc1Record.logicalTimestamp ? "DC-0" : "DC-1";
            System.out.printf(
                "[INCONSISTENCY] key=%-12s  expected winner by logicalTimestamp: %s%n",
                inc.key,
                expectedWinner);
            System.out.printf(
                "  DC-0: value=%-16s OV=%-12s HW=%-12s logicalTs=%d%n",
                inc.dc0Record.valueHash,
                inc.dc0Record.upstreamRTOffset,
                inc.dc0Record.highWatermark,
                inc.dc0Record.logicalTimestamp);
            System.out.printf(
                "  DC-1: value=%-16s OV=%-12s HW=%-12s logicalTs=%d%n",
                inc.dc1Record.valueHash,
                inc.dc1Record.upstreamRTOffset,
                inc.dc1Record.highWatermark,
                inc.dc1Record.logicalTimestamp);
            break;
        }
      }
      System.out.println("########## LILY-PAD CHECK END ##########\n");

      // The test deliberately injects a disagreement on WOLF: DC-0 stores "arctic-wolf"
      // (logicalTimestamp=200) and DC-1 stores "dire-wolf" (logicalTimestamp=180).
      // The lily-pad algorithm must detect exactly this one inconsistency.
      assertFalse(inconsistencies.isEmpty(), "Expected lily-pad to detect the injected wolf inconsistency");
      assertEquals(inconsistencies.size(), 1, "Expected exactly one inconsistency");

      VTConsistencyChecker.Inconsistency wolfInc = inconsistencies.get(0);
      assertNotNull(wolfInc.key, "Inconsistency key must be non-null");
      assertEquals(wolfInc.type, VTConsistencyChecker.InconsistencyType.VALUE_MISMATCH);
      assertNotEquals(
          wolfInc.dc0Record.valueHash,
          wolfInc.dc1Record.valueHash,
          "DC-0 and DC-1 value hashes must differ");
      // DC-0 has the higher logical timestamp so it is the correct winner
      assertEquals(
          wolfInc.dc0Record.logicalTimestamp > wolfInc.dc1Record.logicalTimestamp,
          true,
          "DC-0 logicalTimestamp should be higher (correct winner)");
    } // close TopicManager try-with-resources
  }

  /**
   * Creates a consumer against the given broker, fetches the beginning/end positions for a single
   * partition, builds a {@link PubSubPartitionSplit}, and runs {@link VTConsistencyChecker#buildSnapshot}.
   */
  private static VTConsistencyChecker.Snapshot buildSnapshotForPartition(
      PubSubBrokerWrapper broker,
      VeniceProperties props,
      PubSubTopicRepository topicRepository,
      PubSubTopicPartition tp,
      String consumerName,
      TopicManager topicManager,
      PubSubPositionDeserializer positionDeserializer) throws Exception {
    PubSubConsumerAdapter consumer = broker.getPubSubClientsFactory()
        .getConsumerAdapterFactory()
        .create(
            new PubSubConsumerAdapterContext.Builder().setPubSubBrokerAddress(broker.getAddress())
                .setVeniceProperties(props)
                .setPubSubTopicRepository(topicRepository)
                .setPubSubMessageDeserializer(PubSubMessageDeserializer.createDefaultDeserializer())
                .setPubSubPositionTypeRegistry(broker.getPubSubPositionTypeRegistry())
                .setConsumerName(consumerName)
                .build());
    Duration timeout = Duration.ofSeconds(30);
    List<PubSubTopicPartition> partitions = Collections.singletonList(tp);
    PubSubPosition start = consumer.beginningPositions(partitions, timeout).get(tp);
    PubSubPosition end = consumer.endPositions(partitions, timeout).get(tp);
    long estimatedRecords = Math.max(0, consumer.positionDifference(tp, end, start));
    PubSubPartitionSplit split = new PubSubPartitionSplit(topicRepository, tp, start, end, estimatedRecords, 0, 0);
    try (PubSubSplitIterator iterator = new PubSubSplitIterator(consumer, split, false)) {
      return VTConsistencyChecker.buildSnapshot(iterator, topicManager, positionDeserializer);
    }
  }

  private static TopicManager createTopicManager(
      PubSubBrokerWrapper broker,
      VeniceProperties props,
      PubSubTopicRepository topicRepository) {
    PubSubPositionTypeRegistry positionTypeRegistry = broker.getPubSubPositionTypeRegistry();
    TopicManagerContext context = new TopicManagerContext.Builder().setPubSubPropertiesSupplier(k -> props)
        .setPubSubConsumerAdapterFactory(broker.getPubSubClientsFactory().getConsumerAdapterFactory())
        .setPubSubAdminAdapterFactory(broker.getPubSubClientsFactory().getAdminAdapterFactory())
        .setPubSubTopicRepository(topicRepository)
        .setPubSubPositionTypeRegistry(positionTypeRegistry)
        .setTopicMetadataFetcherConsumerPoolSize(1)
        .setTopicMetadataFetcherThreadPoolSize(1)
        .setVeniceComponent(VeniceComponent.UNSPECIFIED)
        .build();
    return new TopicManagerRepository(context, broker.getAddress()).getLocalTopicManager();
  }

  /**
   * Writes a single PUT record directly to a VT, bypassing the RT→DCR→VT path.
   * This simulates a bug where a leader made a DCR decision that the other DC's leader did not.
   */
  private void injectBugToVT(
      VeniceClusterWrapper cluster,
      String versionTopic,
      String key,
      String value,
      int coloId,
      long fakeUpstreamOffset,
      long logicalTimestamp) throws Exception {
    PubSubBrokerWrapper broker = cluster.getPubSubBrokerWrapper();
    VeniceWriterFactory writerFactory = IntegrationTestPushUtils
        .getVeniceWriterFactory(broker, broker.getPubSubClientsFactory().getProducerAdapterFactory());
    try (VeniceWriter<byte[], byte[], byte[]> writer =
        writerFactory.createVeniceWriter(new VeniceWriterOptions.Builder(versionTopic).build())) {
      LeaderMetadataWrapper leaderMetadata =
          new LeaderMetadataWrapper(ApacheKafkaOffsetPosition.of(fakeUpstreamOffset), coloId, 0L);
      writer.put(avroBytes(key), avroBytes(value), 1, null, leaderMetadata, logicalTimestamp, null).get();
    }
  }

  /** Encodes a Java string as Avro binary bytes. */
  private byte[] avroBytes(String str) throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    GenericDatumWriter<Object> writer = new GenericDatumWriter<>(Schema.create(Schema.Type.STRING));
    org.apache.avro.io.BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    writer.write(str, encoder);
    encoder.flush();
    return out.toByteArray();
  }

  /**
   * Runs {@link VTConsistencyCheckerJob} in local Spark mode against the same injected
   * hawk+wolf inconsistency and asserts the output Parquet contains exactly the expected row.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testSparkJobDetectsInjectedInconsistency() throws Exception {
    String storeName = Utils.getUniqueString("aa-vt-checker-job");

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, getParentControllerUrl());
        ControllerClient dc0ControllerClient =
            new ControllerClient(CLUSTER_NAME, childDatacenters.get(0).getControllerConnectString());
        ControllerClient dc1ControllerClient =
            new ControllerClient(CLUSTER_NAME, childDatacenters.get(1).getControllerConnectString())) {

      assertCommand(
          parentControllerClient
              .createNewStore(storeName, "test-owner", STRING_SCHEMA.toString(), STRING_SCHEMA.toString()));
      assertCommand(parentControllerClient.updateStore(storeName, new UpdateStoreQueryParams().setPartitionCount(1)));
      updateStoreToHybrid(storeName, parentControllerClient, Optional.of(true), Optional.of(true), Optional.empty());

      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        assertNotNull(dc0ControllerClient.getStore(storeName).getStore(), "Store not visible in DC-0");
        assertNotNull(dc1ControllerClient.getStore(storeName).getStore(), "Store not visible in DC-1");
      });

      VersionCreationResponse versionResponse =
          assertCommand(parentControllerClient.emptyPush(storeName, Utils.getUniqueString("push-id"), 1L));
      String versionTopic = versionResponse.getKafkaTopic();

      TestUtils.waitForNonDeterministicAssertion(
          60,
          TimeUnit.SECONDS,
          () -> assertEquals(
              parentControllerClient.queryJobStatus(versionTopic).getStatus(),
              ExecutionStatus.COMPLETED.toString(),
              "Push did not complete"));

      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        assertEquals(dc0ControllerClient.getStore(storeName).getStore().getCurrentVersion(), 1, "DC-0 version");
        assertEquals(dc1ControllerClient.getStore(storeName).getStore().getCurrentVersion(), 1, "DC-1 version");
      });

      VeniceClusterWrapper dc0Cluster = getCluster(0);
      VeniceClusterWrapper dc1Cluster = getCluster(1);

      injectBugToVT(dc0Cluster, versionTopic, HAWK, "hawk", 0, 50L, 100L);
      injectBugToVT(dc0Cluster, versionTopic, HAWK, "hawk", 1, 60L, 110L);
      injectBugToVT(dc0Cluster, versionTopic, WOLF, "grey-wolf", 0, 5L, 150L);
      injectBugToVT(dc0Cluster, versionTopic, WOLF, "arctic-wolf", 1, 10L, 200L);

      injectBugToVT(dc1Cluster, versionTopic, HAWK, "hawk", 0, 20L, 100L);
      injectBugToVT(dc1Cluster, versionTopic, HAWK, "hawk", 1, 30L, 110L);
      injectBugToVT(dc1Cluster, versionTopic, WOLF, "grey-wolf", 0, 10L, 150L);
      injectBugToVT(dc1Cluster, versionTopic, WOLF, "dire-wolf", 1, 15L, 180L);

      // createTempDirectory creates the dir itself; pass a child path so Spark's
      // ErrorIfExists check doesn't fire on the pre-existing parent directory.
      File tempRoot = Files.createTempDirectory("vt-consistency-job-test").toFile();
      File outputDir = new File(tempRoot, "output");
      SparkSession spark =
          SparkSession.builder().master("local[*]").appName("VTConsistencyCheckerJobTest").getOrCreate();
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

        VTConsistencyCheckerJob.run(jobProps);

        Dataset<Row> result = spark.read().parquet(outputDir.getAbsolutePath());
        List<Row> rows = result.collectAsList();

        assertFalse(rows.isEmpty(), "Expected at least one inconsistency row in the output");
        assertEquals(rows.size(), 1, "Expected exactly one inconsistency row");

        Row wolfRow = rows.get(0);
        assertNotNull(wolfRow.getAs("key"), "key must be non-null");
        assertEquals(wolfRow.getAs("type"), "VALUE_MISMATCH");
        assertNotEquals(
            (String) wolfRow.getAs("dc0_value"),
            (String) wolfRow.getAs("dc1_value"),
            "DC values must differ");
        assertEquals(wolfRow.getAs("version_topic"), versionTopic);
        assertEquals((int) wolfRow.getAs("vt_partition"), 0);
        assertTrue(
            (long) wolfRow.getAs("dc0_logical_ts") > (long) wolfRow.getAs("dc1_logical_ts"),
            "DC-0 logicalTimestamp should be higher (correct winner)");
      } finally {
        spark.stop();
        deleteRecursively(tempRoot);
      }
    }
  }

  private static void deleteRecursively(File file) {
    if (file.isDirectory()) {
      File[] children = file.listFiles();
      if (children != null) {
        for (File child: children) {
          deleteRecursively(child);
        }
      }
    }
    file.delete();
  }

}
