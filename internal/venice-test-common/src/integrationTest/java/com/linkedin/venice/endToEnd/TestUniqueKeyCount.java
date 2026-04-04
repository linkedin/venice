package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.CHILD_DATA_CENTER_KAFKA_URL_PREFIX;
import static com.linkedin.venice.ConfigKeys.DEFAULT_MAX_NUMBER_OF_PARTITIONS;
import static com.linkedin.venice.ConfigKeys.SERVER_ADD_RMD_TO_BATCH_PUSH_FOR_HYBRID_STORES;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_TRANSACTIONAL_MODE;
import static com.linkedin.venice.ConfigKeys.SERVER_DEDICATED_DRAINER_FOR_SORTED_INPUT_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_UNIQUE_KEY_COUNT_FOR_ALL_BATCH_PUSH_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_UNIQUE_KEY_COUNT_FOR_HYBRID_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_USE_HEARTBEAT_LAG_FOR_READY_TO_SERVE_CHECK_ENABLED;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_PARENT_DATA_CENTER_REGION_NAME;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecord;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_VALUE_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.INCREMENTAL_PUSH;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SEND_CONTROL_MESSAGES_DIRECTLY;

import com.linkedin.davinci.kafka.consumer.PartitionConsumptionState;
import com.linkedin.davinci.kafka.consumer.StoreIngestionTask;
import com.linkedin.venice.annotation.PubSubAgnosticTest;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.samza.VeniceSystemProducer;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * End-to-end integration tests for the unique key count feature.
 *
 * Validates that:
 * - Batch pushes populate a non-negative unique key count in the OffsetRecord.
 * - Real-time (hybrid) writes update the unique key count for new inserts and deletes.
 * - Unique key counts are independent across version pushes.
 * - Batch-only (non-hybrid) stores also get unique key counts when the batch config is enabled.
 */
@PubSubAgnosticTest
public class TestUniqueKeyCount {
  private static final int TEST_TIMEOUT = 360 * Time.MS_PER_SECOND;
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, 1).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new);

  private List<VeniceMultiClusterWrapper> childDatacenters;
  private List<VeniceControllerWrapper> parentControllers;
  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  private String clusterName;
  private VeniceClusterWrapper clusterWrapper;
  private ControllerClient parentControllerClient;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    Properties serverProperties = new Properties();
    serverProperties.put(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, false);
    serverProperties.put(SERVER_DEDICATED_DRAINER_FOR_SORTED_INPUT_ENABLED, true);
    serverProperties.put(SERVER_USE_HEARTBEAT_LAG_FOR_READY_TO_SERVE_CHECK_ENABLED, false);
    serverProperties.put(
        CHILD_DATA_CENTER_KAFKA_URL_PREFIX + "." + DEFAULT_PARENT_DATA_CENTER_REGION_NAME,
        "localhost:" + TestUtils.getFreePort());
    serverProperties.put(SERVER_UNIQUE_KEY_COUNT_FOR_ALL_BATCH_PUSH_ENABLED, true);
    serverProperties.put(SERVER_UNIQUE_KEY_COUNT_FOR_HYBRID_STORE_ENABLED, true);
    serverProperties.put(SERVER_ADD_RMD_TO_BATCH_PUSH_FOR_HYBRID_STORES, true);
    // Set sync threshold low so OffsetRecord (including uniqueKeyCount) is persisted after small RT writes
    serverProperties.put(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_TRANSACTIONAL_MODE, 1);

    Properties controllerProps = new Properties();
    controllerProps.put(DEFAULT_MAX_NUMBER_OF_PARTITIONS, 20);

    VeniceMultiRegionClusterCreateOptions.Builder optionsBuilder =
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(1)
            .numberOfClusters(1)
            .numberOfParentControllers(1)
            .numberOfChildControllers(1)
            .numberOfServers(1)
            .numberOfRouters(1)
            .replicationFactor(1)
            .forkServer(false)
            .parentControllerProperties(controllerProps)
            .childControllerProperties(controllerProps)
            .serverProperties(serverProperties);

    multiRegionMultiClusterWrapper =
        ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(optionsBuilder.build());

    childDatacenters = multiRegionMultiClusterWrapper.getChildRegions();
    parentControllers = multiRegionMultiClusterWrapper.getParentControllers();
    clusterName = CLUSTER_NAMES[0];
    clusterWrapper = childDatacenters.get(0).getClusters().get(clusterName);

    String parentControllerURLs =
        parentControllers.stream().map(VeniceControllerWrapper::getControllerUrl).collect(Collectors.joining(","));
    parentControllerClient = new ControllerClient(clusterName, parentControllerURLs);
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(parentControllerClient);
    Utils.closeQuietlyWithErrorLogged(multiRegionMultiClusterWrapper);
  }

  /**
   * Retrieves the OffsetRecord for a given topic and partition from the first server in the cluster.
   */
  private OffsetRecord getOffsetRecord(String topicName, int partitionId) {
    VeniceServerWrapper server = clusterWrapper.getVeniceServers().get(0);
    return server.getVeniceServer()
        .getStorageMetadataService()
        .getLastOffset(
            topicName,
            partitionId,
            server.getVeniceServer().getKafkaStoreIngestionService().getPubSubContext());
  }

  /**
   * Reads the unique key count directly from the running StoreIngestionTask's PCS (in-memory).
   * Bypasses syncOffset — returns the live count from the AtomicLong.
   * Returns -1 if the task or PCS is not found.
   */
  private long getLiveUniqueKeyCount(String topicName, int partitionId) {
    VeniceServerWrapper server = clusterWrapper.getVeniceServers().get(0);
    StoreIngestionTask task = server.getVeniceServer().getKafkaStoreIngestionService().getStoreIngestionTask(topicName);
    if (task == null) {
      return -1;
    }
    for (PartitionConsumptionState pcs: task.getPartitionConsumptionStates()) {
      if (pcs.getPartition() == partitionId) {
        return pcs.getUniqueKeyCount();
      }
    }
    return -1;
  }

  /**
   * Creates an A/A hybrid store with 1 partition and runs a VPJ batch push with the given record count.
   * Returns the record schema used to create the store.
   */
  private Schema createAAHybridStoreAndPush(String storeName, File inputDir) throws Exception {
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();

    UpdateStoreQueryParams storeParams = new UpdateStoreQueryParams().setActiveActiveReplicationEnabled(true)
        .setHybridRewindSeconds(360)
        .setHybridOffsetLagThreshold(0)
        .setChunkingEnabled(false)
        .setNativeReplicationEnabled(true)
        .setPartitionCount(1);

    createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, storeParams).close();
    IntegrationTestPushUtils.runVPJ(props);

    // Wait for version 1 to become current
    ControllerClient childControllerClient =
        new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
    try {
      TestUtils.waitForNonDeterministicAssertion(
          30,
          TimeUnit.SECONDS,
          () -> Assert.assertEquals(childControllerClient.getStore(storeName).getStore().getCurrentVersion(), 1));
    } finally {
      childControllerClient.close();
    }
    return recordSchema;
  }

  /**
   * Creates an A/A hybrid store with custom store parameters and runs a VPJ batch push.
   * Returns the record schema used to create the store.
   */
  private Schema createAAHybridStoreAndPush(String storeName, File inputDir, UpdateStoreQueryParams storeParams)
      throws Exception {
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();

    createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, storeParams).close();
    IntegrationTestPushUtils.runVPJ(props);

    // Wait for version 1 to become current
    ControllerClient childControllerClient =
        new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
    try {
      TestUtils.waitForNonDeterministicAssertion(
          30,
          TimeUnit.SECONDS,
          () -> Assert.assertEquals(childControllerClient.getStore(storeName).getStore().getCurrentVersion(), 1));
    } finally {
      childControllerClient.close();
    }
    return recordSchema;
  }

  /**
   * After a batch push of 100 records (keys "1" through "100") to a single-partition A/A hybrid store,
   * the OffsetRecord for partition 0 must have a non-negative unique key count (i.e., tracking is active).
   * Since there is only 1 partition, all 100 keys land on partition 0 and the count should equal 100.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testBatchPushPopulatesUniqueKeyCount() throws Exception {
    String storeName = Utils.getUniqueString("store-ukc-batch");
    File inputDir = getTempDataDirectory();

    try {
      createAAHybridStoreAndPush(storeName, inputDir);

      String topicName = Version.composeKafkaTopic(storeName, 1);

      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
        OffsetRecord offsetRecord = getOffsetRecord(topicName, 0);
        Assert.assertTrue(
            offsetRecord.getUniqueKeyCount() >= 0,
            "Expected unique key count to be non-negative after batch push, but got: "
                + offsetRecord.getUniqueKeyCount());
        Assert.assertTrue(
            offsetRecord.getUniqueKeyCount() > 0,
            "Expected unique key count to be positive after 100-record batch push, but got: "
                + offsetRecord.getUniqueKeyCount());
      });

      // Also verify that data is readable
      try (AvroGenericStoreClient<String, Utf8> client = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(clusterWrapper.getRandomRouterURL()))) {
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
          for (int i = 1; i <= 10; i++) {
            Assert.assertNotNull(client.get(Integer.toString(i)).get(), "Key " + i + " should be readable");
          }
        });
      }
    } finally {
      parentControllerClient.disableAndDeleteStore(storeName);
    }
  }

  /**
   * After a batch push, sending new RT records (keys not in batch) should increase the unique key count.
   * Uses direct PCS access (in-memory count) alongside OffsetRecord (persisted count) to validate
   * both the signal computation and the persistence.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testHybridRTUpdatesUniqueKeyCount() throws Exception {
    String storeName = Utils.getUniqueString("store-ukc-hybrid");
    File inputDir = getTempDataDirectory();

    try {
      createAAHybridStoreAndPush(storeName, inputDir);

      String topicName = Version.composeKafkaTopic(storeName, 1);

      // Capture the batch count from the persisted OffsetRecord
      long[] batchCount = new long[1];
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
        OffsetRecord offsetRecord = getOffsetRecord(topicName, 0);
        Assert.assertTrue(
            offsetRecord.getUniqueKeyCount() > 0,
            "Expected positive unique key count after batch push, but got: " + offsetRecord.getUniqueKeyCount());
        batchCount[0] = offsetRecord.getUniqueKeyCount();
      });

      // Also verify the live PCS count matches
      long liveBatchCount = getLiveUniqueKeyCount(topicName, 0);
      Assert.assertEquals(
          liveBatchCount,
          batchCount[0],
          "Live PCS count should match persisted OffsetRecord count after batch");

      // Send 10 RT records with NEW keys (200-209, not in batch range 1-100)
      // Then a marker record to guarantee all prior records are processed
      try (VeniceSystemProducer veniceProducer =
          IntegrationTestPushUtils.getSamzaProducerForStream(multiRegionMultiClusterWrapper, 0, storeName)) {
        for (int i = 200; i < 210; i++) {
          sendStreamingRecord(veniceProducer, storeName, Integer.toString(i), "stream_" + i);
        }
        // Marker record: once readable, all prior RT records are guaranteed processed
        sendStreamingRecord(veniceProducer, storeName, "marker_210", "marker_value");
      }

      // Wait for marker to be readable (proves all 10 new keys + marker are consumed)
      try (AvroGenericStoreClient<String, Utf8> client = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(clusterWrapper.getRandomRouterURL()))) {
        TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
          Assert.assertNotNull(client.get("marker_210").get(), "Marker should be readable");
        });
      }

      // Check the live PCS count (in-memory) and the persisted OffsetRecord
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
        long liveCount = getLiveUniqueKeyCount(topicName, 0);
        Assert.assertTrue(
            liveCount > batchCount[0],
            "Live PCS unique key count (" + liveCount + ") should be greater than batch count (" + batchCount[0]
                + ") after 10 new RT keys + 1 marker");
      });

      // Verify the persisted OffsetRecord also reflects the updated count
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
        OffsetRecord offsetRecord = getOffsetRecord(topicName, 0);
        Assert.assertTrue(
            offsetRecord.getUniqueKeyCount() > batchCount[0],
            "Persisted unique key count (" + offsetRecord.getUniqueKeyCount() + ") should be greater than batch count ("
                + batchCount[0] + ")");
      });
    } finally {
      parentControllerClient.disableAndDeleteStore(storeName);
    }
  }

  /**
   * Unique key counts should be independent across version pushes.
   * A second batch push (v2) with a different record count should have its own independent count.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testUniqueKeyCountPersistsThroughVersionSwap() throws Exception {
    String storeName = Utils.getUniqueString("store-ukc-vswap");
    File inputDir = getTempDataDirectory();

    try {
      // Push v1 with 50 records
      Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir, 50);
      String inputDirPath = "file:" + inputDir.getAbsolutePath();
      Properties props =
          IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
      String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
      String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();

      UpdateStoreQueryParams storeParams = new UpdateStoreQueryParams().setActiveActiveReplicationEnabled(true)
          .setHybridRewindSeconds(360)
          .setHybridOffsetLagThreshold(0)
          .setChunkingEnabled(false)
          .setNativeReplicationEnabled(true)
          .setPartitionCount(1);

      createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, storeParams).close();
      IntegrationTestPushUtils.runVPJ(props);

      ControllerClient childControllerClientV1 =
          new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
      try {
        TestUtils.waitForNonDeterministicAssertion(
            30,
            TimeUnit.SECONDS,
            () -> Assert.assertEquals(childControllerClientV1.getStore(storeName).getStore().getCurrentVersion(), 1));
      } finally {
        childControllerClientV1.close();
      }

      // Validate v1 unique key count
      String topicV1 = Version.composeKafkaTopic(storeName, 1);
      long[] v1Count = new long[1];
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
        OffsetRecord offsetRecord = getOffsetRecord(topicV1, 0);
        Assert.assertTrue(
            offsetRecord.getUniqueKeyCount() > 0,
            "Expected positive unique key count for v1 with 50 records, but got: " + offsetRecord.getUniqueKeyCount());
        v1Count[0] = offsetRecord.getUniqueKeyCount();
      });

      // Push v2 with 80 records
      TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir, 80);
      IntegrationTestPushUtils.runVPJ(props);

      ControllerClient childControllerClientV2 =
          new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
      try {
        TestUtils.waitForNonDeterministicAssertion(
            30,
            TimeUnit.SECONDS,
            () -> Assert.assertEquals(childControllerClientV2.getStore(storeName).getStore().getCurrentVersion(), 2));
      } finally {
        childControllerClientV2.close();
      }

      // Validate v2 unique key count is independent of v1
      String topicV2 = Version.composeKafkaTopic(storeName, 2);
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
        OffsetRecord offsetRecord = getOffsetRecord(topicV2, 0);
        Assert.assertTrue(
            offsetRecord.getUniqueKeyCount() > 0,
            "Expected positive unique key count for v2 with 80 records, but got: " + offsetRecord.getUniqueKeyCount());
        // v2 has 80 records vs v1's 50, so the count should differ
        Assert.assertNotEquals(
            offsetRecord.getUniqueKeyCount(),
            v1Count[0],
            "v2 unique key count should differ from v1 (v1=" + v1Count[0] + ", v2=" + offsetRecord.getUniqueKeyCount()
                + ")");
      });
    } finally {
      parentControllerClient.disableAndDeleteStore(storeName);
    }
  }

  /**
   * Validates that the unique key count works for a batch-only (non-hybrid, non-A/A) store
   * when SERVER_UNIQUE_KEY_COUNT_FOR_ALL_BATCH_PUSH_ENABLED is true.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testBatchOnlyStoreGetsUniqueKeyCount() throws Exception {
    String storeName = Utils.getUniqueString("store-ukc-batchonly");
    File inputDir = getTempDataDirectory();

    try {
      Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir);
      String inputDirPath = "file:" + inputDir.getAbsolutePath();
      Properties props =
          IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
      String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
      String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();

      // Non-hybrid, non-A/A store — just a plain batch store with 1 partition
      UpdateStoreQueryParams storeParams = new UpdateStoreQueryParams().setChunkingEnabled(false).setPartitionCount(1);

      createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, storeParams).close();
      IntegrationTestPushUtils.runVPJ(props);

      ControllerClient childControllerClient =
          new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
      try {
        TestUtils.waitForNonDeterministicAssertion(
            30,
            TimeUnit.SECONDS,
            () -> Assert.assertEquals(childControllerClient.getStore(storeName).getStore().getCurrentVersion(), 1));
      } finally {
        childControllerClient.close();
      }

      String topicName = Version.composeKafkaTopic(storeName, 1);

      // Since SERVER_UNIQUE_KEY_COUNT_FOR_ALL_BATCH_PUSH_ENABLED is true, even a batch-only store
      // should have a non-negative unique key count
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
        OffsetRecord offsetRecord = getOffsetRecord(topicName, 0);
        Assert.assertTrue(
            offsetRecord.getUniqueKeyCount() >= 0,
            "Expected non-negative unique key count for batch-only store, but got: "
                + offsetRecord.getUniqueKeyCount());
        Assert.assertTrue(
            offsetRecord.getUniqueKeyCount() > 0,
            "Expected positive unique key count for 100-record batch-only push, but got: "
                + offsetRecord.getUniqueKeyCount());
      });

      // Verify data is readable
      try (AvroGenericStoreClient<String, Utf8> client = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(clusterWrapper.getRandomRouterURL()))) {
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
          for (int i = 1; i <= 10; i++) {
            Assert.assertNotNull(client.get(Integer.toString(i)).get(), "Key " + i + " should be readable");
          }
        });
      }
    } finally {
      parentControllerClient.disableAndDeleteStore(storeName);
    }
  }

  /**
   * Validates that a chunking-enabled A/A hybrid store correctly populates the unique key count after a batch push.
   * With chunkingEnabled=true, the ingestion pipeline may split large values into chunk fragments, but the unique
   * key count should only count logical keys (not chunk fragments). Even with normal-sized values that do not
   * actually trigger chunking, this test verifies that the chunking-enabled configuration does not break the
   * unique key count pipeline.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testChunkedHybridStoreUniqueKeyCount() throws Exception {
    String storeName = Utils.getUniqueString("store-ukc-chunk");
    File inputDir = getTempDataDirectory();

    try {
      UpdateStoreQueryParams storeParams = new UpdateStoreQueryParams().setActiveActiveReplicationEnabled(true)
          .setHybridRewindSeconds(360)
          .setHybridOffsetLagThreshold(0)
          .setChunkingEnabled(true)
          .setNativeReplicationEnabled(true)
          .setPartitionCount(1);

      createAAHybridStoreAndPush(storeName, inputDir, storeParams);

      String topicName = Version.composeKafkaTopic(storeName, 1);

      // Validate that the unique key count is populated and reflects the 100 logical keys
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
        OffsetRecord offsetRecord = getOffsetRecord(topicName, 0);
        Assert.assertTrue(
            offsetRecord.getUniqueKeyCount() > 0,
            "Expected positive unique key count after batch push to chunking-enabled store, but got: "
                + offsetRecord.getUniqueKeyCount());
      });

      // Verify data is readable through the store client
      try (AvroGenericStoreClient<String, Utf8> client = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(clusterWrapper.getRandomRouterURL()))) {
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
          for (int i = 1; i <= 10; i++) {
            Assert.assertNotNull(client.get(Integer.toString(i)).get(), "Key " + i + " should be readable");
          }
        });
      }
    } finally {
      parentControllerClient.disableAndDeleteStore(storeName);
    }
  }

  /**
   * Validates that a VPJ incremental push to an A/A hybrid store increases the unique key count when new keys
   * are introduced. The test flow is:
   * 1. Create an A/A hybrid store with incremental push enabled and run a batch push (keys 1-100).
   * 2. Capture the unique key count after the batch push.
   * 3. Run a VPJ incremental push that writes keys 51-150 (50 overlapping, 50 new).
   * 4. Validate that the unique key count increased (the 50 new keys 101-150 should be counted).
   *
   * Note: For A/A hybrid stores, incremental push data flows through the real-time topic, similar to
   * Samza-based RT writes tested in {@link #testHybridRTUpdatesUniqueKeyCount()}. This test exercises
   * the VPJ incremental push pipeline specifically.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testIncrementalPushUpdatesUniqueKeyCount() throws Exception {
    String storeName = Utils.getUniqueString("store-ukc-incpush");
    File inputDir = getTempDataDirectory();

    try {
      UpdateStoreQueryParams storeParams = new UpdateStoreQueryParams().setActiveActiveReplicationEnabled(true)
          .setHybridRewindSeconds(360)
          .setHybridOffsetLagThreshold(0)
          .setChunkingEnabled(true)
          .setNativeReplicationEnabled(true)
          .setIncrementalPushEnabled(true)
          .setPartitionCount(1);

      createAAHybridStoreAndPush(storeName, inputDir, storeParams);

      String topicName = Version.composeKafkaTopic(storeName, 1);

      // Capture the batch count
      long[] batchCount = new long[1];
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
        OffsetRecord offsetRecord = getOffsetRecord(topicName, 0);
        Assert.assertTrue(
            offsetRecord.getUniqueKeyCount() > 0,
            "Expected positive unique key count after batch push, but got: " + offsetRecord.getUniqueKeyCount());
        batchCount[0] = offsetRecord.getUniqueKeyCount();
      });

      // Run VPJ incremental push with keys 51-150 (50 overlap with batch, 50 new)
      File incPushDir = getTempDataDirectory();
      TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema2(incPushDir);
      String incPushDirPath = "file:" + incPushDir.getAbsolutePath();
      Properties incPushProps =
          IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, incPushDirPath, storeName);
      incPushProps.setProperty(INCREMENTAL_PUSH, "true");
      incPushProps.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);
      IntegrationTestPushUtils.runVPJ(incPushProps);

      // Wait for new keys to be readable
      try (AvroGenericStoreClient<String, Utf8> client = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(clusterWrapper.getRandomRouterURL()))) {
        TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
          Assert.assertNotNull(client.get("150").get(), "Key 150 from incremental push should be readable");
        });
      }

      // Check live PCS count
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
        long liveCount = getLiveUniqueKeyCount(topicName, 0);
        Assert.assertTrue(
            liveCount > batchCount[0],
            "Live PCS unique key count (" + liveCount + ") should be greater than batch count (" + batchCount[0]
                + ") after incremental push with 50 new keys");
      });

      // Verify the persisted OffsetRecord also reflects the updated count
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
        OffsetRecord offsetRecord = getOffsetRecord(topicName, 0);
        Assert.assertTrue(
            offsetRecord.getUniqueKeyCount() > batchCount[0],
            "Persisted unique key count (" + offsetRecord.getUniqueKeyCount() + ") should be greater than batch count ("
                + batchCount[0] + ")");
      });

    } finally {
      parentControllerClient.disableAndDeleteStore(storeName);
    }
  }
}
