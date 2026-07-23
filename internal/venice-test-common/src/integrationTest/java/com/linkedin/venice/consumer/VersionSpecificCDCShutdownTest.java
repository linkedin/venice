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
import com.linkedin.davinci.DaVinciBackend;
import com.linkedin.davinci.client.AvroGenericDaVinciClient;
import com.linkedin.davinci.consumer.ChangeEvent;
import com.linkedin.davinci.consumer.ChangelogClientConfig;
import com.linkedin.davinci.consumer.VeniceChangeCoordinate;
import com.linkedin.davinci.consumer.VeniceChangelogConsumer;
import com.linkedin.davinci.consumer.VeniceChangelogConsumerClientFactory;
import com.linkedin.davinci.consumer.VeniceChangelogConsumerDaVinciRecordTransformerImpl;
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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
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
  private static final int CDC_OUTPUT_BUFFER_SIZE = 100;
  private static final int FLINK_CLOSE_TIMEOUT_SECONDS = 30;
  private static final int BLOCKING_RECORD_START = 110;
  private static final int BLOCKING_RECORD_COUNT = 120;
  private static final int STORE_B_BLOCKED_RECORD_COUNT = 10;
  private static final String HYBRID_DRAINER_THREAD_PREFIX = "Store-writer-hybrid";

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
    VeniceChangelogConsumer<GenericRecord, GenericRecord> consumerA = null;
    VeniceChangelogConsumer<GenericRecord, GenericRecord> consumerB = null;
    VeniceChangelogConsumer<GenericRecord, GenericRecord> restartedConsumerA = null;
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

      VeniceChangelogConsumerClientFactory factory = createFactory(inputDir);

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
      Map<Integer, VeniceChangeCoordinate> checkpointsByPartition = new HashMap<>();
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, () -> {
        pollAndCollectWithCheckpoints(activeConsumerA, consumerAEvents, checkpointsByPartition);
        int expectedMinEvents = DEFAULT_USER_DATA_RECORD_COUNT + 9;
        assertTrue(
            consumerAEvents.size() >= expectedMinEvents,
            "Store A expected >= " + expectedMinEvents + " but got " + consumerAEvents.size());
      });
      verifyNearlineValues(consumerAEvents, 100, 110);

      assertEquals(
          checkpointsByPartition.size(),
          PARTITION_COUNT,
          "External checkpoints must cover every partition before the restart");
      Set<VeniceChangeCoordinate> checkpoints = new HashSet<>(checkpointsByPartition.values());

      BlockingQueue<?> consumerAOutputQueue = getOutputQueue(activeConsumerA);
      BlockingQueue<?> consumerBOutputQueue = getOutputQueue(activeConsumerB);
      assertTrue(consumerAOutputQueue.isEmpty(), "Store A output queue must start empty");
      assertTrue(consumerBOutputQueue.isEmpty(), "Store B output queue must start empty");

      // Stop polling A, fill its tiny output queue, and block the sole shared hybrid drainer in put().
      try (VeniceSystemProducer producerA =
          IntegrationTestPushUtils.getSamzaProducer(clusterWrapper, storeA, Version.PushType.STREAM)) {
        runSamzaStreamJob(producerA, storeA, BLOCKING_RECORD_COUNT, BLOCKING_RECORD_START);
      }

      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, () -> {
        assertEquals(
            consumerAOutputQueue.size(),
            CDC_OUTPUT_BUFFER_SIZE,
            "Store A output queue should be full before shutdown");
        assertNotNull(
            findBlockedSharedHybridDrainer(),
            "The sole shared hybrid drainer should be blocked in consumer A's ArrayBlockingQueue.put");
      });

      // Queue store B records behind the blocked A record and prove B cannot make progress before A shuts down.
      try (VeniceSystemProducer producerB =
          IntegrationTestPushUtils.getSamzaProducer(clusterWrapper, storeB, Version.PushType.STREAM)) {
        runSamzaStreamJob(producerB, storeB, STORE_B_BLOCKED_RECORD_COUNT, BLOCKING_RECORD_START);
      }

      BlockingQueue<?> sharedHybridDrainerQueue = getSharedHybridDrainerQueue();
      String storeBVersionTopic = Version.composeKafkaTopic(storeB, 1);
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, () -> {
        assertTrue(
            drainerQueueContainsTopic(sharedHybridDrainerQueue, storeBVersionTopic),
            "Store B records should be queued behind the blocked store A record");
      });
      assertTrue(consumerBOutputQueue.isEmpty(), "Store B must not progress while the shared drainer is blocked");

      Thread blockedDrainer = findBlockedSharedHybridDrainer();
      assertNotNull(blockedDrainer, "Shared hybrid drainer unexpectedly unblocked before consumer A shutdown");
      LOGGER.info(
          "CDC shutdown precondition reproduced: consumerAQueueSize={}, consumerBQueueSize={}, "
              + "sharedDrainerQueueSize={}, blockedDrainer={} ({})",
          consumerAOutputQueue.size(),
          consumerBOutputQueue.size(),
          sharedHybridDrainerQueue.size(),
          blockedDrainer.getName(),
          blockedDrainer.getState());

      VeniceChangelogConsumer<GenericRecord, GenericRecord> consumerAToClose = consumerA;
      closeExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "cdc-consumer-close");
        t.setDaemon(true);
        return t;
      });
      long closeStartMs = System.currentTimeMillis();
      closeFuture = closeExecutor.submit(consumerAToClose::close);

      boolean closedInTime = false;
      try {
        closeFuture.get(FLINK_CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        closedInTime = true;
      } catch (TimeoutException e) {
        Thread stillBlockedDrainer = findBlockedSharedHybridDrainer();
        LOGGER.error(
            "CDC shutdown red reproduction: close exceeded Flink's {}s timeout; "
                + "consumerAQueueSize={}, consumerBQueueSize={}, blockedDrainer={}",
            FLINK_CLOSE_TIMEOUT_SECONDS,
            consumerAOutputQueue.size(),
            consumerBOutputQueue.size(),
            describeThread(stillBlockedDrainer));
      }

      long closeElapsedMs = System.currentTimeMillis() - closeStartMs;
      assertTrue(
          closedInTime,
          "CDC consumer A close exceeded Flink's 30s timeout while the shared StoreBuffer drainer was blocked "
              + "in its full output queue; elapsedMs=" + closeElapsedMs + ", drainer="
              + describeThread(findBlockedSharedHybridDrainer()));

      Map<String, GenericRecord> resumedBEvents = new HashMap<>();
      try {
        pollAndVerifyNearlineRecords(
            activeConsumerB,
            resumedBEvents,
            BLOCKING_RECORD_START,
            BLOCKING_RECORD_START + STORE_B_BLOCKED_RECORD_COUNT);
      } catch (AssertionError e) {
        throw new AssertionError(
            "Store B did not resume after consumer A shutdown; shared drainer="
                + describeThread(findBlockedSharedHybridDrainer()),
            e);
      }

      restartedConsumerA = factory.getVersionSpecificChangelogConsumer(storeA, 1);
      assertNotNull(restartedConsumerA);
      assertNotSame(
          restartedConsumerA,
          consumerA,
          "Factory should return a fresh consumer after the old consumer is closed");
      restartedConsumerA.seekToCheckpoint(checkpoints).get();

      Map<String, GenericRecord> replayedAEvents = new HashMap<>();
      pollAndVerifyNearlineRecords(
          restartedConsumerA,
          replayedAEvents,
          BLOCKING_RECORD_START,
          BLOCKING_RECORD_START + BLOCKING_RECORD_COUNT);
      LOGGER.info(
          "Restarted consumer A replayed all {} expected records from external checkpoints; "
              + "store B resumed with {} records.",
          BLOCKING_RECORD_COUNT,
          resumedBEvents.size());
    } finally {
      releaseBlockedSharedDrainerForCleanup(consumerA);
      if (closeFuture != null && !closeFuture.isDone()) {
        try {
          closeFuture.get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
          LOGGER.warn("Consumer A close did not finish during test cleanup", e);
        }
      } else if (closeFuture == null && consumerA != null) {
        closeInBackground(consumerA);
      }
      if (closeExecutor != null) {
        closeExecutor.shutdownNow();
      }
      closeInBackground(restartedConsumerA);
      closeInBackground(consumerB);
      cleanUpStore(storeA);
      cleanUpStore(storeB);
    }
  }

  private BlockingQueue<?> getOutputQueue(VeniceChangelogConsumer<?, ?> consumer) throws ReflectiveOperationException {
    Field outputQueueField =
        VeniceChangelogConsumerDaVinciRecordTransformerImpl.class.getDeclaredField("pubSubMessages");
    outputQueueField.setAccessible(true);
    return (BlockingQueue<?>) outputQueueField.get(consumer);
  }

  private BlockingQueue<?> getSharedHybridDrainerQueue() throws ReflectiveOperationException {
    DaVinciBackend backend = AvroGenericDaVinciClient.getBackend();
    Object ingestionService = readField(backend, "ingestionService");
    Object storeBufferService = readField(ingestionService, "storeBufferService");
    if (storeBufferService.getClass().getSimpleName().equals("SeparatedStoreBufferService")) {
      storeBufferService = readField(storeBufferService, "unsortedStoreBufferServiceDelegate");
    }

    List<?> drainerQueues = (List<?>) readField(storeBufferService, "blockingQueueArr");
    assertEquals(drainerQueues.size(), 1, "The integration test must use exactly one shared hybrid drainer");
    return (BlockingQueue<?>) drainerQueues.get(0);
  }

  private boolean drainerQueueContainsTopic(BlockingQueue<?> drainerQueue, String topicName) {
    try {
      Lock queueLock = (Lock) readField(drainerQueue, "memoryLock");
      queueLock.lock();
      try {
        for (Object queueNode: drainerQueue.toArray()) {
          PubSubMessage consumerRecord = (PubSubMessage) readField(queueNode, "consumerRecord");
          if (topicName.equals(consumerRecord.getTopicPartition().getPubSubTopic().getName())) {
            return true;
          }
        }
        return false;
      } finally {
        queueLock.unlock();
      }
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException("Unable to inspect the shared drainer queue", e);
    }
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

  private Thread findBlockedSharedHybridDrainer() {
    for (Map.Entry<Thread, StackTraceElement[]> entry: Thread.getAllStackTraces().entrySet()) {
      Thread thread = entry.getKey();
      if (!thread.getName().startsWith(HYBRID_DRAINER_THREAD_PREFIX)) {
        continue;
      }

      boolean blockedInOutputQueuePut = false;
      boolean insideConsumerBufferAdd = false;
      for (StackTraceElement stackFrame: entry.getValue()) {
        if (stackFrame.getClassName().equals("java.util.concurrent.ArrayBlockingQueue")
            && stackFrame.getMethodName().equals("put")) {
          blockedInOutputQueuePut = true;
        }
        if (stackFrame.getClassName().startsWith(VeniceChangelogConsumerDaVinciRecordTransformerImpl.class.getName())
            && stackFrame.getMethodName().equals("internalAddMessageToBuffer")) {
          insideConsumerBufferAdd = true;
        }
      }

      if (blockedInOutputQueuePut && insideConsumerBufferAdd) {
        return thread;
      }
    }
    return null;
  }

  private String describeThread(Thread thread) {
    if (thread == null) {
      return "none";
    }

    StringBuilder description = new StringBuilder(thread.getName()).append('(').append(thread.getState()).append(')');
    StackTraceElement[] stackTrace = thread.getStackTrace();
    for (int i = 0; i < Math.min(stackTrace.length, 8); i++) {
      description.append(System.lineSeparator()).append("  at ").append(stackTrace[i]);
    }
    return description.toString();
  }

  private void releaseBlockedSharedDrainerForCleanup(VeniceChangelogConsumer<?, ?> consumer) {
    if (consumer == null) {
      return;
    }

    long deadlineMs = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30);
    Thread blockedDrainer;
    while ((blockedDrainer = findBlockedSharedHybridDrainer()) != null && System.currentTimeMillis() < deadlineMs) {
      try {
        consumer.poll(0);
        Thread.sleep(10);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      } catch (Exception e) {
        LOGGER.warn("Failed to drain consumer A during test cleanup", e);
        break;
      }
    }

    if (blockedDrainer != null) {
      LOGGER.warn("Shared hybrid drainer remained blocked after test cleanup: {}", describeThread(blockedDrainer));
    }
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

  private VeniceChangelogConsumerClientFactory createFactory(String inputDirRef) {
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
        .setMaxBufferSize(CDC_OUTPUT_BUFFER_SIZE);
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
