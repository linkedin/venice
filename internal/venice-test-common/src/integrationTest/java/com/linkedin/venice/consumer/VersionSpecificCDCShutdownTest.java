package com.linkedin.venice.consumer;

import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.CONSUMER_METRIC_ENTITIES;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_SCAN_INTERVAL_IN_SECONDS;
import static com.linkedin.venice.ConfigKeys.PUSH_STATUS_STORE_ENABLED;
import static com.linkedin.venice.integration.utils.VeniceControllerWrapper.D2_SERVICE_NAME;
import static com.linkedin.venice.stats.ClientType.CHANGE_DATA_CAPTURE_CLIENT;
import static com.linkedin.venice.stats.VeniceMetricsRepository.getVeniceMetricsRepository;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecord;
import static com.linkedin.venice.utils.TestWriteUtils.DEFAULT_USER_DATA_RECORD_COUNT;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFile;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_VALUE_FIELD_PROP;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertTrue;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.davinci.consumer.ChangeEvent;
import com.linkedin.davinci.consumer.ChangelogClientConfig;
import com.linkedin.davinci.consumer.VeniceChangeCoordinate;
import com.linkedin.davinci.consumer.VeniceChangelogConsumer;
import com.linkedin.davinci.consumer.VeniceChangelogConsumerClientFactory;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.endToEnd.TestChangelogKey;
import com.linkedin.venice.endToEnd.TestChangelogValue;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.samza.VeniceSystemProducer;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.PushInputSchemaBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.view.TestView;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.samza.system.SystemProducer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Regression test for the CDC consumer shutdown bottleneck that causes the Flink crash loop.
 *
 * Uses two stores sharing a DaVinciBackend singleton: store B stays alive throughout while
 * store A is closed and restarted. Validates that close completes within Flink's 30s timeout,
 * the restarted consumer can seek to checkpoint and receive nearline records, and store B
 * remains unaffected.
 */
@Test(singleThreaded = true)
public class VersionSpecificCDCShutdownTest {
  private static final Logger LOGGER = LogManager.getLogger(VersionSpecificCDCShutdownTest.class);
  private static final int TEST_TIMEOUT = 3 * Time.MS_PER_MINUTE;
  private static final int PARTITION_COUNT = 3;

  private String clusterName;
  private VeniceClusterWrapper clusterWrapper;
  private D2Client d2Client;
  private MetricsRepository metricsRepository;
  private String zkAddress;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    Utils.thisIsLocalhost();

    Properties clusterConfig = new Properties();
    clusterConfig.put(PUSH_STATUS_STORE_ENABLED, true);
    clusterConfig.put(DAVINCI_PUSH_STATUS_SCAN_INTERVAL_IN_SECONDS, 3);
    clusterConfig.put(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, false);

    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
        .numberOfServers(1)
        .numberOfRouters(1)
        .replicationFactor(1)
        .forkServer(false)
        .extraProperties(clusterConfig)
        .build();

    clusterWrapper = ServiceFactory.getVeniceCluster(options);
    clusterName = clusterWrapper.getClusterName();
    zkAddress = clusterWrapper.getZk().getAddress();

    d2Client = new D2ClientBuilder().setZkHosts(zkAddress)
        .setZkSessionTimeout(3, TimeUnit.SECONDS)
        .setZkStartupTimeout(3, TimeUnit.SECONDS)
        .build();
    D2ClientUtils.startClient(d2Client);

    metricsRepository = getVeniceMetricsRepository(CHANGE_DATA_CAPTURE_CLIENT, CONSUMER_METRIC_ENTITIES, true);
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(clusterWrapper);
    TestView.resetCounters();
  }

  /**
   * Simulates the Flink CDC task restart scenario end-to-end:
   * 1. Store B's consumer runs throughout, keeping the DaVinciBackend singleton alive.
   * 2. Store A's consumer subscribes and receives batch + nearline data.
   * 3. Additional nearline records are produced to load the drainer before close.
   * 4. Store A's consumer close() is called with Flink's 30s timeout.
   * 5. Asserts close completes within 30s.
   * 6. A new consumer seeks to checkpoint, receives new nearline records with value verification.
   * 7. Store B receives new nearline records with value verification.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testVersionSpecificCDCConsumerRestartWithinFlinkTimeout() throws Exception {
    String storeA = Utils.getUniqueString("storeA");
    String storeB = Utils.getUniqueString("storeB");
    String inputDir = setUpStore(storeA);
    setUpStore(storeB);

    // Produce nearline records BEFORE consumers subscribe so the version topic contains:
    // batch data (100 records) → EOP → nearline records (10 per store).
    // This ensures checkpoints captured during polling are past EOP.
    try (
        VeniceSystemProducer producerA =
            IntegrationTestPushUtils.getSamzaProducer(clusterWrapper, storeA, Version.PushType.STREAM);
        VeniceSystemProducer producerB =
            IntegrationTestPushUtils.getSamzaProducer(clusterWrapper, storeB, Version.PushType.STREAM)) {
      runSamzaStreamJob(producerA, storeA, 10, 0, 100);
      runSamzaStreamJob(producerB, storeB, 10, 0, 100);
    }

    VeniceChangelogConsumerClientFactory factory = createFactory(inputDir);

    // Store B consumer keeps DaVinciBackend alive across the close/restart of store A
    VeniceChangelogConsumer<GenericRecord, GenericRecord> consumerB =
        factory.getVersionSpecificChangelogConsumer(storeB, 1);
    consumerB.subscribeAll().get();
    // Verify batch + nearline data received
    Map<String, GenericRecord> consumerBEvents = new HashMap<>();
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, false, () -> {
      pollAndCollect(consumerB, consumerBEvents);
      int expectedMinEvents = DEFAULT_USER_DATA_RECORD_COUNT + 9;
      assertTrue(
          consumerBEvents.size() >= expectedMinEvents,
          "Store B expected >= " + expectedMinEvents + " but got " + consumerBEvents.size());
    });
    verifyNearlineValues(consumerBEvents, 100, 110);
    LOGGER.info("Store B consumer verified with {} events.", consumerBEvents.size());

    // Store A consumer subscribes, receives data, and captures checkpoints
    VeniceChangelogConsumer<GenericRecord, GenericRecord> consumerA =
        factory.getVersionSpecificChangelogConsumer(storeA, 1);
    consumerA.subscribeAll().get();
    Map<String, GenericRecord> consumerAEvents = new HashMap<>();
    Set<VeniceChangeCoordinate> checkpoints = new HashSet<>();
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, false, () -> {
      pollAndCollectWithCheckpoints(consumerA, consumerAEvents, checkpoints);
      int expectedMinEventsA = DEFAULT_USER_DATA_RECORD_COUNT + 9;
      assertTrue(
          consumerAEvents.size() >= expectedMinEventsA,
          "Store A expected >= " + expectedMinEventsA + " but got " + consumerAEvents.size());
    });
    verifyNearlineValues(consumerAEvents, 100, 110);
    // Verify checkpoints cover all partitions to ensure seekToCheckpoint subscribes to all of them
    Set<Integer> checkpointPartitions = new HashSet<>();
    for (VeniceChangeCoordinate checkpoint: checkpoints) {
      checkpointPartitions.add(checkpoint.getPartition());
    }
    assertTrue(
        checkpointPartitions.size() >= PARTITION_COUNT,
        "Expected checkpoints from all " + PARTITION_COUNT + " partitions but got " + checkpointPartitions.size());
    LOGGER.info(
        "Store A consumer verified with {} events, captured checkpoints from {} partitions.",
        consumerAEvents.size(),
        checkpointPartitions.size());

    // Produce more nearline records to load the drainer before close
    try (
        VeniceSystemProducer producerA =
            IntegrationTestPushUtils.getSamzaProducer(clusterWrapper, storeA, Version.PushType.STREAM);
        VeniceSystemProducer producerB =
            IntegrationTestPushUtils.getSamzaProducer(clusterWrapper, storeB, Version.PushType.STREAM)) {
      runSamzaStreamJob(producerA, storeA, 10, 0, 110);
      runSamzaStreamJob(producerB, storeB, 10, 0, 110);
    }

    // Simulate Flink: close in background with 30s timeout
    ExecutorService executor = Executors.newSingleThreadExecutor();
    long closeStartMs = System.currentTimeMillis();
    Future<?> closeFuture = executor.submit(() -> consumerA.close());

    boolean closedInTime = false;
    try {
      closeFuture.get(30, TimeUnit.SECONDS);
      closedInTime = true;
    } catch (TimeoutException e) {
      // Expected without the fix
    } finally {
      executor.shutdown();
    }

    long closeElapsedMs = System.currentTimeMillis() - closeStartMs;
    LOGGER.info("Consumer A close() completed in {}ms", closeElapsedMs);
    assertTrue(closedInTime, "CDC consumer close() took " + closeElapsedMs + "ms, exceeding 30s threshold.");

    // Restart consumer A with seekToCheckpoint (simulating Flink checkpoint restore).
    // The factory must return a fresh consumer — if deregisterClient() didn't run during close,
    // the factory returns the stale cached consumer which would throw "already subscribed".
    VeniceChangelogConsumer<GenericRecord, GenericRecord> newConsumerA =
        factory.getVersionSpecificChangelogConsumer(storeA, 1);
    assertNotNull(newConsumerA);
    assertNotSame(newConsumerA, consumerA, "Factory should return a fresh consumer, not the closed stale instance");
    newConsumerA.seekToCheckpoint(checkpoints).get();
    LOGGER.info("Restarted consumer A seeked to {} checkpoints.", checkpoints.size());

    // Produce new nearline records after restart
    try (
        VeniceSystemProducer producerA =
            IntegrationTestPushUtils.getSamzaProducer(clusterWrapper, storeA, Version.PushType.STREAM);
        VeniceSystemProducer producerB =
            IntegrationTestPushUtils.getSamzaProducer(clusterWrapper, storeB, Version.PushType.STREAM)) {
      runSamzaStreamJob(producerA, storeA, 10, 0, 120);
      runSamzaStreamJob(producerB, storeB, 10, 0, 120);
    }

    // Verify restarted consumer A receives the new nearline records (keys 120-129)
    Map<String, GenericRecord> restartedAEvents = new HashMap<>();
    pollAndVerifyNearlineRecords(newConsumerA, restartedAEvents, 120, 130);
    LOGGER.info("Restarted consumer A received {} events after checkpoint seek.", restartedAEvents.size());

    // Verify store B receives the new nearline records (keys 120-129)
    Map<String, GenericRecord> newConsumerBEvents = new HashMap<>();
    pollAndVerifyNearlineRecords(consumerB, newConsumerBEvents, 120, 130);
    LOGGER.info("Store B received {} nearline records after store A restart.", newConsumerBEvents.size());

    // Cleanup
    closeInBackground(newConsumerA);
    closeInBackground(consumerB);
    cleanUpStore(storeA);
    cleanUpStore(storeB);
  }

  private void pollAndVerifyNearlineRecords(
      VeniceChangelogConsumer<GenericRecord, GenericRecord> consumer,
      Map<String, GenericRecord> eventsMap,
      int startIdx,
      int endIdx) {
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, false, () -> {
      pollAndCollect(consumer, eventsMap);
      for (int i = startIdx; i < endIdx; i++) {
        String key = String.valueOf(i);
        GenericRecord value = eventsMap.get(key);
        assertNotNull(value, "Missing event for key " + key);
        assertTrue(
            value.get("firstName").toString().equals("first_name_stream_" + i),
            "Value mismatch for key " + key + ": " + value.get("firstName"));
      }
    });
  }

  private void pollAndCollect(
      VeniceChangelogConsumer<GenericRecord, GenericRecord> consumer,
      Map<String, GenericRecord> eventsMap) {
    Collection<PubSubMessage<GenericRecord, ChangeEvent<GenericRecord>, VeniceChangeCoordinate>> messages =
        consumer.poll(1000);
    for (PubSubMessage<GenericRecord, ChangeEvent<GenericRecord>, VeniceChangeCoordinate> msg: messages) {
      if (msg.getKey() != null) {
        eventsMap.put(String.valueOf(msg.getKey().get("id")), msg.getValue().getCurrentValue());
      }
    }
  }

  private void pollAndCollectWithCheckpoints(
      VeniceChangelogConsumer<GenericRecord, GenericRecord> consumer,
      Map<String, GenericRecord> eventsMap,
      Set<VeniceChangeCoordinate> checkpoints) {
    Collection<PubSubMessage<GenericRecord, ChangeEvent<GenericRecord>, VeniceChangeCoordinate>> messages =
        consumer.poll(1000);
    for (PubSubMessage<GenericRecord, ChangeEvent<GenericRecord>, VeniceChangeCoordinate> msg: messages) {
      if (msg.getKey() != null) {
        eventsMap.put(String.valueOf(msg.getKey().get("id")), msg.getValue().getCurrentValue());
      }
      checkpoints.add(msg.getPosition());
    }
  }

  private void verifyNearlineValues(Map<String, GenericRecord> events, int startIdx, int endIdx) {
    for (int i = startIdx; i < endIdx; i++) {
      String key = String.valueOf(i);
      GenericRecord value = events.get(key);
      assertNotNull(value, "Missing event for key " + key);
      assertTrue(
          value.get("firstName").toString().equals("first_name_stream_" + i),
          "Value mismatch for key " + key + ": " + value.get("firstName"));
    }
  }

  private void runSamzaStreamJob(SystemProducer producer, String storeName, int numPuts, int numDels, int startIdx) {
    for (int i = startIdx; i < startIdx + numPuts; i++) {
      TestChangelogKey key = new TestChangelogKey();
      key.id = i;
      TestChangelogValue value = new TestChangelogValue();
      value.firstName = "first_name_stream_" + i;
      value.lastName = "last_name_stream_" + i;
      sendStreamingRecord(producer, storeName, key, value, null);
    }
  }

  private String setUpStore(String storeName) throws Exception {
    File inputDir = getTempDataDirectory();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties props = defaultVPJProps(clusterWrapper, inputDirPath, storeName);

    Schema recordSchema = new PushInputSchemaBuilder().setKeySchema(TestChangelogKey.SCHEMA$)
        .setValueSchema(TestChangelogValue.SCHEMA$)
        .build();

    writeSimpleAvroFile(inputDir, recordSchema, i -> {
      GenericRecord keyValueRecord = new GenericData.Record(recordSchema);
      TestChangelogKey key = new TestChangelogKey();
      key.id = i;
      keyValueRecord.put(DEFAULT_KEY_FIELD_PROP, key);
      TestChangelogValue value = new TestChangelogValue();
      value.firstName = "first_name" + i;
      value.lastName = "last_name" + i;
      keyValueRecord.put(DEFAULT_VALUE_FIELD_PROP, value);
      return keyValueRecord;
    }, DEFAULT_USER_DATA_RECORD_COUNT);

    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();

    UpdateStoreQueryParams storeParams = new UpdateStoreQueryParams().setHybridRewindSeconds(500)
        .setHybridOffsetLagThreshold(8)
        .setChunkingEnabled(true)
        .setNativeReplicationEnabled(true)
        .setPartitionCount(PARTITION_COUNT)
        .setBlobTransferEnabled(true);

    try (ControllerClient controllerClient =
        createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, storeParams)) {
      clusterWrapper.createMetaSystemStore(storeName);
      clusterWrapper.createPushStatusSystemStore(storeName);
      TestUtils.assertCommand(controllerClient.updateStore(storeName, storeParams));
      TestUtils.assertCommand(controllerClient.addValueSchema(storeName, valueSchemaStr));
      IntegrationTestPushUtils.runVPJ(props, 1, controllerClient);
    }

    return inputDirPath;
  }

  private VeniceChangelogConsumerClientFactory createFactory(String inputDirRef) {
    Properties consumerProps = ChangelogConsumerTestUtils.buildConsumerProperties(clusterWrapper, inputDirRef);
    ChangelogClientConfig globalConfig = new ChangelogClientConfig().setConsumerProperties(consumerProps)
        .setControllerD2ServiceName(D2_SERVICE_NAME)
        .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
        .setLocalD2ZkHosts(zkAddress)
        .setControllerRequestRetryCount(3)
        .setD2Client(d2Client)
        .setMaxBufferSize(10);
    return new VeniceChangelogConsumerClientFactory(globalConfig, metricsRepository);
  }

  private void closeInBackground(VeniceChangelogConsumer<?, ?> consumer) {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(() -> {
      try {
        consumer.close();
      } catch (Exception e) {
        LOGGER.warn("Background close failed", e);
      }
    });
    executor.shutdown();
  }

  private void cleanUpStore(String storeName) {
    clusterWrapper.useControllerClient(controllerClient -> {
      controllerClient.disableAndDeleteStore(storeName);
    });
  }
}
