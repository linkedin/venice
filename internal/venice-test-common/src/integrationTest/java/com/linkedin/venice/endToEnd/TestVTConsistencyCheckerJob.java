package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.NATIVE_REPLICATION_SOURCE_FABRIC;
import static com.linkedin.venice.ConfigKeys.PARENT_KAFKA_CLUSTER_FABRIC_LIST;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_PARENT_DATA_CENTER_REGION_NAME;
import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static com.linkedin.venice.utils.TestUtils.updateStoreToHybrid;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_SCHEMA;
import static org.testng.Assert.*;

import com.linkedin.venice.annotation.PubSubAgnosticTest;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.serializer.AvroSerializer;
import com.linkedin.venice.spark.consistency.VTConsistencyCheckerJob;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.LeaderMetadataWrapper;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
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
   * Creates a two-DC AA store, runs an empty push, injects records directly into each DC's VT
   * to simulate a bug where the two leaders disagree, then runs VTConsistencyCheckerJob
   * and verifies the output.
   *
   * <p>Injected state:
   * <ul>
   *   <li>hawk — advances HW to known values in both DCs</li>
   *   <li>wolf — value mismatch: DC-0 has "arctic-wolf" (logicalTs=200),
   *       DC-1 has "dire-wolf" (logicalTs=180)</li>
   * </ul>
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

      // Inject hawk to push HW, then wolf with disagreeing values
      // DC-0: hawk advances HW to [50, 60]; wolf has logicalTs=200 → correct winner
      injectToVT(dc0Cluster, versionTopic, "hawk", "hawk", 0, 50L, 100L);
      injectToVT(dc0Cluster, versionTopic, "hawk", "hawk", 1, 60L, 110L);
      injectToVT(dc0Cluster, versionTopic, "wolf", "grey-wolf", 0, 5L, 150L);
      injectToVT(dc0Cluster, versionTopic, "wolf", "arctic-wolf", 1, 10L, 200L);

      // DC-1: hawk advances HW to [20, 30]; wolf has logicalTs=180 → wrong winner
      injectToVT(dc1Cluster, versionTopic, "hawk", "hawk", 0, 20L, 100L);
      injectToVT(dc1Cluster, versionTopic, "hawk", "hawk", 1, 30L, 110L);
      injectToVT(dc1Cluster, versionTopic, "wolf", "grey-wolf", 0, 10L, 150L);
      injectToVT(dc1Cluster, versionTopic, "wolf", "dire-wolf", 1, 15L, 180L);

      // Run the Spark job — run() creates and stops its own SparkSession
      File tempRoot = Files.createTempDirectory("vt-consistency-job-test").toFile();
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

        // Create a new SparkSession to read the output (the job's session was stopped in run())
        SparkSession reader =
            SparkSession.builder().master("local[*]").appName("TestVTConsistencyCheckerJob-reader").getOrCreate();
        try {
          Dataset<Row> result = reader.read().parquet(outputDir.getAbsolutePath());
          List<Row> rows = result.collectAsList();

          assertFalse(rows.isEmpty(), "Expected at least one inconsistency row in the output");
          assertEquals(rows.size(), 1, "Expected exactly one inconsistency row");

          Row wolfRow = rows.get(0);
          assertEquals(wolfRow.getAs("type"), "VALUE_MISMATCH");
          assertEquals(wolfRow.getAs("version_topic"), versionTopic);
          assertEquals((int) wolfRow.getAs("vt_partition"), 0);
          assertFalse(wolfRow.getAs("dc0_value_hash").equals(wolfRow.getAs("dc1_value_hash")), "DC values must differ");
          assertTrue(
              (long) wolfRow.getAs("dc0_logical_ts") > (long) wolfRow.getAs("dc1_logical_ts"),
              "DC-0 logicalTimestamp should be higher (correct winner)");
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
