package com.linkedin.venice.consumer;

import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.CONSUMER_METRIC_ENTITIES;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_SCAN_INTERVAL_IN_SECONDS;
import static com.linkedin.venice.ConfigKeys.PUSH_STATUS_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED;
import static com.linkedin.venice.integration.utils.VeniceControllerWrapper.D2_SERVICE_NAME;
import static com.linkedin.venice.stats.ClientType.CHANGE_DATA_CAPTURE_CLIENT;
import static com.linkedin.venice.stats.VeniceMetricsRepository.getVeniceMetricsRepository;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecord;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFile;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_VALUE_FIELD_PROP;
import static org.testng.Assert.assertNotNull;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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
 * Regression test for the Flink CDC shutdown race condition.
 *
 * In a Flink CDC pipeline, when a task restarts, Flink gives the old task 30 seconds to close.
 * If close() does not complete in time, Flink deploys a new task while the old one is still
 * shutting down. The root cause is that
 * {@link com.linkedin.davinci.consumer.VeniceChangelogConsumerDaVinciRecordTransformerImpl#stop()}
 * calls {@code daVinciClient.close()} which blocks in
 * {@link com.linkedin.davinci.kafka.consumer.StoreIngestionTask#shutdownPartitionConsumptionStates()}
 * due to {@link com.linkedin.davinci.kafka.consumer.SharedKafkaConsumer#waitAfterUnsubscribe}
 * taking 10 seconds per partition per shared consumer. With many partitions this easily exceeds 30
 * seconds, preventing {@code deregisterClient()} and {@code clearPartitionState()} from running.
 * The factory then returns the stale cached consumer on restart, causing:
 * "Cannot subscribe to partitions ... as they are already subscribed"
 *
 * The test keeps one CDC consumer running on store B throughout to ensure the DaVinciBackend
 * singleton stays alive across the restart, matching the Flink production topology where multiple
 * stores share the same backend process.
 *
 * WITHOUT the fix: close() takes ~30s (shutdownAndWait timeout), exceeding the 20s threshold →
 * test fails.
 * WITH the fix (skip waitAfterUnsubscribe during SIT shutdown): close() takes <10s → test passes
 * and verifies the restarted consumer receives data.
 */
@Test(singleThreaded = true)
public class VersionSpecificCDCShutdownTest {
  private static final Logger LOGGER = LogManager.getLogger(VersionSpecificCDCShutdownTest.class);
  private static final int TEST_TIMEOUT = 5 * Time.MS_PER_MINUTE;
  private static final int PARTITION_COUNT = 3;

  /**
   * Flink's SplitFetcherManager close timeout. The CDC consumer must complete close() within
   * this window.
   *
   * Without the fix: {@code StoreIngestionTask.shutdownAndWait(30)} blocks for the full 30s because
   * the graceful-shutdown latch is only counted down on the success path (after
   * {@code shutdownPartitionConsumptionStates()} succeeds). Any exception during partition shutdown
   * leaves the latch at 1 forever, so {@code shutdownAndWait} always blocks for its full 30s wait
   * time, making {@code daVinciClient.close()} exceed this timeout.
   *
   * With the fix (latch counted down in the {@code finally} block): close() completes in ~20s
   * (dominated by {@code waitAfterUnsubscribe} + drainer sync), well within this 30s limit.
   */
  private static final int CLOSE_THRESHOLD_SECONDS = 30;

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
   * Simulates the Flink task restart scenario end-to-end:
   * <ol>
   *   <li>Store B's consumer runs throughout, keeping the DaVinciBackend singleton alive.</li>
   *   <li>Store A's consumer subscribes and receives data.</li>
   *   <li>Store A's consumer close() is called in a background thread with Flink's 30s timeout.</li>
   *   <li>After close completes, a new consumer is created from the same factory and must
   *       be able to subscribe and receive data without errors.</li>
   * </ol>
   *
   * Fails without the fix because {@code StoreIngestionTask.shutdownAndWait(30)} blocks for its
   * full 30s: the graceful-shutdown latch is only counted down on the success path of
   * {@code shutdownPartitionConsumptionStates()}, so any exception (e.g. the 60s parallel-shutdown
   * {@code TimeoutException}) leaves the latch permanently at 1, causing close() to always exceed
   * 30s. With the fix ({@code shutdownLatch.countDown()} in {@code finally}), close() completes
   * in ~20s and the restarted consumer works correctly.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testVersionSpecificCDCConsumerRestartWithinFlinkTimeout() throws Exception {
    String storeA = Utils.getUniqueString("storeA");
    String storeB = Utils.getUniqueString("storeB");
    setUpStore(storeA);
    setUpStore(storeB);

    VeniceChangelogConsumerClientFactory factory = createFactory(storeA);

    // Store B consumer keeps DaVinciBackend alive across the close/restart of store A,
    // matching the Flink topology where multiple stores share one backend process.
    VeniceChangelogConsumer<GenericRecord, GenericRecord> consumerB =
        factory.getVersionSpecificChangelogConsumer(storeB, 1);
    try (VeniceSystemProducer producerB =
        IntegrationTestPushUtils.getSamzaProducer(clusterWrapper, storeB, Version.PushType.STREAM)) {
      produceData(producerB, storeB, 10, 100);
    }
    consumerB.subscribeAll().get();
    waitForEvents(consumerB, 10);

    try {
      // Subscribe store A and confirm it receives data
      VeniceChangelogConsumer<GenericRecord, GenericRecord> consumerA =
          factory.getVersionSpecificChangelogConsumer(storeA, 1);
      try (VeniceSystemProducer producerA =
          IntegrationTestPushUtils.getSamzaProducer(clusterWrapper, storeA, Version.PushType.STREAM)) {
        produceData(producerA, storeA, 10, 100);
      }
      consumerA.subscribeAll().get();
      waitForEvents(consumerA, 10);

      // Simulate Flink: close in background, wait up to CLOSE_THRESHOLD_SECONDS (30s).
      // Without the fix, shutdownAndWait(30) blocks for the full 30s and close() exceeds this limit.
      // With the fix (latch in finally), close() completes in ~20s.
      long closeStartMs = System.currentTimeMillis();
      ExecutorService executor = Executors.newSingleThreadExecutor();
      Future<?> closeFuture = executor.submit(() -> consumerA.close());

      boolean closedInTime = false;
      try {
        closeFuture.get(CLOSE_THRESHOLD_SECONDS, TimeUnit.SECONDS);
        closedInTime = true;
      } catch (TimeoutException e) {
        // Expected without the fix
      } finally {
        executor.shutdown();
      }

      long closeElapsedMs = System.currentTimeMillis() - closeStartMs;
      LOGGER.info(
          "Consumer A close() {}in {}ms (threshold: {}s)",
          closedInTime ? "completed " : "did NOT complete ",
          closeElapsedMs,
          CLOSE_THRESHOLD_SECONDS);

      assertTrue(
          closedInTime,
          "CDC consumer close() took " + closeElapsedMs + "ms, exceeding the " + CLOSE_THRESHOLD_SECONDS
              + "s threshold. Without the fix, StoreIngestionTask.shutdownAndWait(30) blocks for the "
              + "full 30s because SharedKafkaConsumer.waitAfterUnsubscribe() times out at 10s per "
              + "partition per shared consumer. This prevents deregisterClient() and "
              + "clearPartitionState() from running, so the factory returns the stale cached consumer "
              + "on restart, causing 'Cannot subscribe to partitions ... as they are already subscribed'.");

      // Verify the restarted consumer works end-to-end: new consumer from the factory must
      // subscribe successfully and receive data (would throw "already subscribed" without the fix).
      VeniceChangelogConsumer<GenericRecord, GenericRecord> newConsumerA =
          factory.getVersionSpecificChangelogConsumer(storeA, 1);
      assertNotNull(newConsumerA, "Factory must return a fresh consumer after clean close");

      try (VeniceSystemProducer producerA2 =
          IntegrationTestPushUtils.getSamzaProducer(clusterWrapper, storeA, Version.PushType.STREAM)) {
        produceData(producerA2, storeA, 10, 200);
      }
      newConsumerA.subscribeAll().get();
      waitForEvents(newConsumerA, 10);
      LOGGER.info("Restarted store A consumer received data successfully.");
      closeInBackground(newConsumerA);

      // Confirm store B consumer remained unaffected throughout
      try (VeniceSystemProducer producerB2 =
          IntegrationTestPushUtils.getSamzaProducer(clusterWrapper, storeB, Version.PushType.STREAM)) {
        produceData(producerB2, storeB, 5, 200);
      }
      waitForEvents(consumerB, 5);
      LOGGER.info("Store B consumer still functional after store A restart.");
    } finally {
      closeInBackground(consumerB);
      cleanUpStore(storeA);
      cleanUpStore(storeB);
    }
  }

  private VeniceChangelogConsumerClientFactory createFactory(String inputDirRef) {
    Properties consumerProps = ChangelogConsumerTestUtils.buildConsumerProperties(clusterWrapper, inputDirRef);
    consumerProps.put(SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED, true);

    ChangelogClientConfig globalConfig = new ChangelogClientConfig().setConsumerProperties(consumerProps)
        .setControllerD2ServiceName(D2_SERVICE_NAME)
        .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
        .setLocalD2ZkHosts(zkAddress)
        .setControllerRequestRetryCount(3)
        .setD2Client(d2Client)
        .setMaxBufferSize(10);

    return new VeniceChangelogConsumerClientFactory(globalConfig, metricsRepository);
  }

  private void waitForEvents(VeniceChangelogConsumer<GenericRecord, GenericRecord> consumer, int minEvents) {
    Map<String, PubSubMessage<GenericRecord, ChangeEvent<GenericRecord>, VeniceChangeCoordinate>> eventsMap =
        new HashMap<>();
    List<PubSubMessage<GenericRecord, ChangeEvent<GenericRecord>, VeniceChangeCoordinate>> eventsList =
        new ArrayList<>();
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, false, () -> {
      Collection<PubSubMessage<GenericRecord, ChangeEvent<GenericRecord>, VeniceChangeCoordinate>> messages =
          consumer.poll(1000);
      for (PubSubMessage<GenericRecord, ChangeEvent<GenericRecord>, VeniceChangeCoordinate> msg: messages) {
        eventsMap.put(msg.getKey() == null ? null : String.valueOf(msg.getKey().get("id")), msg);
      }
      eventsList.addAll(messages);
      assertTrue(
          eventsList.size() >= minEvents,
          "Expected at least " + minEvents + " events but got " + eventsList.size());
    });
  }

  /**
   * Closes a consumer in a background thread without blocking the caller.
   * Used for cleanup where we don't want slow close() calls to hang the test runner.
   */
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
    }, 100);

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

  private void produceData(SystemProducer producer, String storeName, int numPuts, int startIdx) {
    for (int i = startIdx; i < startIdx + numPuts; i++) {
      TestChangelogKey key = new TestChangelogKey();
      key.id = i;
      TestChangelogValue value = new TestChangelogValue();
      value.firstName = "first_name_stream_" + i;
      value.lastName = "last_name_stream_" + i;
      sendStreamingRecord(producer, storeName, key, value, null);
    }
  }

  private void cleanUpStore(String storeName) {
    clusterWrapper.useControllerClient(controllerClient -> {
      controllerClient.disableAndDeleteStore(storeName);
    });
  }
}
