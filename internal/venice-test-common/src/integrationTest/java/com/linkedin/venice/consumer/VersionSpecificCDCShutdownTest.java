package com.linkedin.venice.consumer;

import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.CONSUMER_METRIC_ENTITIES;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.ADMIN_CONSUMPTION_MAX_WORKER_THREAD_POOL_SIZE;
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_SCAN_INTERVAL_IN_SECONDS;
import static com.linkedin.venice.ConfigKeys.DEFAULT_OFFLINE_PUSH_STRATEGY;
import static com.linkedin.venice.ConfigKeys.SERVER_DEDICATED_DRAINER_FOR_SORTED_INPUT_ENABLED;
import static com.linkedin.venice.ConfigKeys.SORTED_INPUT_DRAINER_SIZE;
import static com.linkedin.venice.ConfigKeys.UNSORTED_INPUT_DRAINER_SIZE;
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
import static org.testng.Assert.assertEquals;
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
import com.linkedin.venice.meta.OfflinePushStrategy;
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
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
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
 * Uses two stores sharing one changelog consumer factory and backend: store B stays alive
 * throughout while store A is closed and restarted. Validates that close completes within
 * Flink's 30s timeout, store B keeps progressing, and the restarted store A consumer can
 * replay from external checkpoints.
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
    clusterConfig.put(DAVINCI_PUSH_STATUS_SCAN_INTERVAL_IN_SECONDS, 3);
    clusterConfig.put(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, false);
    clusterConfig.put(DEFAULT_OFFLINE_PUSH_STRATEGY, OfflinePushStrategy.WAIT_ALL_REPLICAS.name());
    clusterConfig.put(ADMIN_CONSUMPTION_MAX_WORKER_THREAD_POOL_SIZE, 1);

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
    if (d2Client != null) {
      D2ClientUtils.shutdownClient(d2Client);
    }
    Utils.closeQuietlyWithErrorLogged(clusterWrapper);
    TestView.resetCounters();
  }

  /**
   * Simulates the Flink CDC task restart scenario end-to-end:
   * 1. Store B's consumer runs throughout, keeping the shared backend alive.
   * 2. Store A's consumer subscribes and receives batch + nearline data.
   * 3. Store A's bounded output queue fills and blocks the sole shared hybrid drainer.
   * 4. A store B record is produced while store A's output remains full.
   * 5. Store A's consumer close() completes within Flink's 30s timeout and releases store B.
   * 6. A fresh store A consumer seeks to the latest external checkpoints and replays every blocked record.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testVersionSpecificCDCConsumerRestartWithinFlinkTimeout() throws Exception {
    int outputBufferSize = 100;
    int closeTimeoutSeconds = 30;
    int blockingRecordStart = 110;
    int storeARecordCount = outputBufferSize + 1;
    int storeBRecordCount = 1;
    String storeA = Utils.getUniqueString("storeA");
    String storeB = Utils.getUniqueString("storeB");
    VeniceChangelogConsumer<GenericRecord, GenericRecord> consumerA = null;
    VeniceChangelogConsumer<GenericRecord, GenericRecord> consumerB = null;
    VeniceChangelogConsumer<GenericRecord, GenericRecord> restartedConsumerA = null;
    BlockingQueue<?> consumerAOutputQueueForCleanup = null;
    ExecutorService closeExecutor = null;
    Future<?> closeFuture = null;

    try {
      String inputDir = setUpStore(storeA);
      setUpStore(storeB);

      // Produce nearline records before subscribing so the external checkpoints are past EOP.
      try (
          VeniceSystemProducer producerA =
              IntegrationTestPushUtils.getSamzaProducer(clusterWrapper, storeA, Version.PushType.STREAM);
          VeniceSystemProducer producerB =
              IntegrationTestPushUtils.getSamzaProducer(clusterWrapper, storeB, Version.PushType.STREAM)) {
        runSamzaStreamJob(producerA, storeA, 10, 100);
        runSamzaStreamJob(producerB, storeB, 10, 100);
      }

      VeniceChangelogConsumerClientFactory factory = createFactory(inputDir, outputBufferSize);

      VeniceChangelogConsumer<GenericRecord, GenericRecord> activeConsumerB =
          factory.getVersionSpecificChangelogConsumer(storeB, 1);
      consumerB = activeConsumerB;
      activeConsumerB.subscribeAll().get();
      Map<String, GenericRecord> consumerBEvents = new HashMap<>();
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, () -> {
        pollAndCollect(activeConsumerB, consumerBEvents);
        int expectedMinEvents = DEFAULT_USER_DATA_RECORD_COUNT + 9;
        assertTrue(
            consumerBEvents.size() >= expectedMinEvents,
            "Store B expected >= " + expectedMinEvents + " but got " + consumerBEvents.size());
      });
      verifyNearlineValues(consumerBEvents, 100, 110);

      VeniceChangelogConsumer<GenericRecord, GenericRecord> activeConsumerA =
          factory.getVersionSpecificChangelogConsumer(storeA, 1);
      consumerA = activeConsumerA;
      activeConsumerA.subscribeAll().get();
      Map<String, GenericRecord> consumerAEvents = new HashMap<>();
      Map<Integer, VeniceChangeCoordinate> latestCheckpointsByPartition = new HashMap<>();
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, () -> {
        pollAndCollectWithCheckpoints(activeConsumerA, consumerAEvents, latestCheckpointsByPartition);
        int expectedMinEvents = DEFAULT_USER_DATA_RECORD_COUNT + 9;
        assertTrue(
            consumerAEvents.size() >= expectedMinEvents,
            "Store A expected >= " + expectedMinEvents + " but got " + consumerAEvents.size());
      });
      verifyNearlineValues(consumerAEvents, 100, 110);

      assertEquals(
          latestCheckpointsByPartition.size(),
          PARTITION_COUNT,
          "External checkpoints must cover every partition before the restart");
      Set<VeniceChangeCoordinate> latestCheckpoints = new HashSet<>(latestCheckpointsByPartition.values());

      BlockingQueue<?> consumerAOutputQueue = getOutputQueue(activeConsumerA);
      consumerAOutputQueueForCleanup = consumerAOutputQueue;

      // Stop polling A, fill its bounded output queue, and block the sole shared hybrid drainer in put().
      try (VeniceSystemProducer producerA =
          IntegrationTestPushUtils.getSamzaProducer(clusterWrapper, storeA, Version.PushType.STREAM)) {
        runSamzaStreamJob(producerA, storeA, storeARecordCount, blockingRecordStart);
      }

      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, () -> {
        assertEquals(
            consumerAOutputQueue.size(),
            outputBufferSize,
            "Store A output queue should be full before shutdown");
      });

      // Produce B while A's output is full. B must make progress once A shuts down.
      try (VeniceSystemProducer producerB =
          IntegrationTestPushUtils.getSamzaProducer(clusterWrapper, storeB, Version.PushType.STREAM)) {
        runSamzaStreamJob(producerB, storeB, storeBRecordCount, blockingRecordStart);
      }

      VeniceChangelogConsumer<GenericRecord, GenericRecord> consumerAToClose = consumerA;
      closeExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "cdc-consumer-close");
        t.setDaemon(true);
        return t;
      });
      long closeStartMs = System.currentTimeMillis();
      closeFuture = closeExecutor.submit(consumerAToClose::close);
      try {
        closeFuture.get(closeTimeoutSeconds, TimeUnit.SECONDS);
      } catch (TimeoutException e) {
        throw new AssertionError(
            "CDC consumer A close exceeded Flink's " + closeTimeoutSeconds
                + "s timeout after its output queue reached capacity",
            e);
      }

      long closeElapsedMs = System.currentTimeMillis() - closeStartMs;
      LOGGER.info("CDC consumer A close completed in {} ms after its output queue reached capacity", closeElapsedMs);

      Map<String, GenericRecord> resumedBEvents = new HashMap<>();
      pollAndVerifyNearlineRecords(
          activeConsumerB,
          resumedBEvents,
          blockingRecordStart,
          blockingRecordStart + storeBRecordCount);
      assertEquals(resumedBEvents.size(), storeBRecordCount, "Store B should receive exactly the produced record");

      restartedConsumerA = factory.getVersionSpecificChangelogConsumer(storeA, 1);
      assertNotNull(restartedConsumerA);
      assertNotSame(
          restartedConsumerA,
          consumerA,
          "Factory should return a fresh consumer after the old consumer is closed");
      restartedConsumerA.seekToCheckpoint(latestCheckpoints).get();

      Map<String, GenericRecord> replayedAEvents = new HashMap<>();
      pollAndVerifyNearlineRecords(
          restartedConsumerA,
          replayedAEvents,
          blockingRecordStart,
          blockingRecordStart + storeARecordCount);
      LOGGER.info(
          "Restarted consumer A replayed all {} expected records from external checkpoints; "
              + "store B resumed with {} records.",
          storeARecordCount,
          resumedBEvents.size());
    } finally {
      if (consumerAOutputQueueForCleanup != null) {
        consumerAOutputQueueForCleanup.clear();
      }
      if (closeFuture != null) {
        try {
          closeFuture.get(closeTimeoutSeconds, TimeUnit.SECONDS);
        } catch (Exception e) {
          LOGGER.warn("Consumer A close did not finish during test cleanup", e);
        }
      } else if (closeFuture == null && consumerA != null) {
        closeInBackground(consumerA);
      }
      if (closeExecutor != null) {
        closeExecutor.shutdown();
      }
      closeInBackground(restartedConsumerA);
      closeInBackground(consumerB);
      cleanUpStore(storeA);
      cleanUpStore(storeB);
    }
  }

  private BlockingQueue<?> getOutputQueue(VeniceChangelogConsumer<?, ?> consumer) throws ReflectiveOperationException {
    return (BlockingQueue<?>) readField(consumer, "pubSubMessages");
  }

  private Object readField(Object target, String fieldName) throws ReflectiveOperationException {
    Class<?> currentClass = target.getClass();
    while (currentClass != null) {
      try {
        Field field = currentClass.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
      } catch (NoSuchFieldException e) {
        currentClass = currentClass.getSuperclass();
      }
    }
    throw new NoSuchFieldException(target.getClass().getName() + "." + fieldName);
  }

  private void pollAndVerifyNearlineRecords(
      VeniceChangelogConsumer<GenericRecord, GenericRecord> consumer,
      Map<String, GenericRecord> eventsMap,
      int startIdx,
      int endIdx) {
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, () -> {
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
      Map<Integer, VeniceChangeCoordinate> checkpointsByPartition) {
    Collection<PubSubMessage<GenericRecord, ChangeEvent<GenericRecord>, VeniceChangeCoordinate>> messages =
        consumer.poll(1000);
    for (PubSubMessage<GenericRecord, ChangeEvent<GenericRecord>, VeniceChangeCoordinate> msg: messages) {
      if (msg.getKey() != null) {
        eventsMap.put(String.valueOf(msg.getKey().get("id")), msg.getValue().getCurrentValue());
      }
      checkpointsByPartition.put(msg.getPartition(), msg.getPosition());
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

  private void runSamzaStreamJob(SystemProducer producer, String storeName, int numPuts, int startIdx) {
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

  private VeniceChangelogConsumerClientFactory createFactory(String inputDirRef, int outputBufferSize) {
    Properties consumerProps = ChangelogConsumerTestUtils.buildConsumerProperties(clusterWrapper, inputDirRef);
    consumerProps.put(SERVER_DEDICATED_DRAINER_FOR_SORTED_INPUT_ENABLED, true);
    consumerProps.put(SORTED_INPUT_DRAINER_SIZE, 1);
    consumerProps.put(UNSORTED_INPUT_DRAINER_SIZE, 1);
    ChangelogClientConfig globalConfig = new ChangelogClientConfig().setConsumerProperties(consumerProps)
        .setControllerD2ServiceName(D2_SERVICE_NAME)
        .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
        .setLocalD2ZkHosts(zkAddress)
        .setControllerRequestRetryCount(3)
        .setD2Client(d2Client)
        .setMaxBufferSize(outputBufferSize);
    return new VeniceChangelogConsumerClientFactory(globalConfig, metricsRepository);
  }

  private void closeInBackground(VeniceChangelogConsumer<?, ?> consumer) {
    if (consumer == null) {
      return;
    }
    ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
      Thread t = new Thread(r, "cdc-consumer-close-bg");
      t.setDaemon(true);
      return t;
    });
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
