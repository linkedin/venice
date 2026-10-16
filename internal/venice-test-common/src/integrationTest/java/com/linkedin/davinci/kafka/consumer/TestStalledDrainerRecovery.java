package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.venice.ConfigKeys.ERROR_PARTITION_AUTO_RESET_LIMIT;
import static com.linkedin.venice.ConfigKeys.ERROR_PARTITION_PROCESSING_CYCLE_DELAY;
import static com.linkedin.venice.ConfigKeys.SERVER_BLOCKED_DRAINER_THRESHOLD_MS;
import static com.linkedin.venice.ConfigKeys.SERVER_DEDICATED_DRAINER_FOR_SORTED_INPUT_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_RESET_ERROR_REPLICA_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_STALLED_DRAINER_QUEUE_FAILURE_ENABLED;
import static com.linkedin.venice.ConfigKeys.SORTED_INPUT_DRAINER_SIZE;
import static com.linkedin.venice.ConfigKeys.STORE_WRITER_BUFFER_MEMORY_CAPACITY;
import static com.linkedin.venice.ConfigKeys.STORE_WRITER_BUFFER_NOTIFY_DELTA;
import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Total;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.Test;


public class TestStalledDrainerRecovery {
  private static final Logger LOGGER = LogManager.getLogger(TestStalledDrainerRecovery.class);
  private static final String LATENCY_SENSOR = ".StoreBufferServiceSorted--internal_processing_latency";

  /**
   * Uses a completed, non-hybrid current version so the real ingestion exception handler and controller auto-reset
   * are eligible. An incomplete batch push does not take that recovery path. Enqueue failure reaches the real
   * data receiver without bypassing the partition exception handler.
   * The injected owner holds the actual Tehuti sensor lock; it models contention, not a corrupted AQS lock.
   */
  @Test(timeOut = 240_000)
  public void testBlockedEnqueueFailureRebootstrapsCurrentVersionAfterDrainerRecovers() throws Exception {
    Properties properties = new Properties();
    properties.setProperty(ERROR_PARTITION_AUTO_RESET_LIMIT, "30");
    properties.setProperty(ERROR_PARTITION_PROCESSING_CYCLE_DELAY, "2000");
    properties.setProperty(SERVER_DEDICATED_DRAINER_FOR_SORTED_INPUT_ENABLED, "true");
    properties.setProperty(SORTED_INPUT_DRAINER_SIZE, "1");
    properties.setProperty(STORE_WRITER_BUFFER_MEMORY_CAPACITY, "32768");
    properties.setProperty(STORE_WRITER_BUFFER_NOTIFY_DELTA, "1024");
    properties.setProperty(SERVER_BLOCKED_DRAINER_THRESHOLD_MS, "5000");
    properties.setProperty(SERVER_STALLED_DRAINER_QUEUE_FAILURE_ENABLED, "true");
    properties.setProperty(SERVER_RESET_ERROR_REPLICA_ENABLED, "true");

    CountDownLatch lockHeld = new CountDownLatch(1);
    CountDownLatch releaseLock = new CountDownLatch(1);
    AtomicReference<Thread> owner = new AtomicReference<>();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<?> lockHolder = null;
    try (VeniceClusterWrapper cluster = ServiceFactory.getVeniceCluster(
        new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
            .numberOfServers(1)
            .numberOfRouters(1)
            .replicationFactor(1)
            .numberOfPartitions(1)
            .extraProperties(properties)
            .build())) {
      try {
        String storeName = Utils.getUniqueString("stalled-drainer-store");
        cluster.useControllerClient(controller -> {
          assertCommand(controller.createNewStore(storeName, "synthetic-owner", "\"string\"", "\"string\""));
          assertCommand(controller.updateStore(storeName, new UpdateStoreQueryParams().setPartitionCount(1)));
        });
        cluster.createVersion(
            storeName,
            "\"string\"",
            "\"string\"",
            Stream.<Map.Entry>of(new AbstractMap.SimpleEntry<>("initial-key", "initial-value")));
        String topic = Version.composeKafkaTopic(storeName, 1);
        VeniceServerWrapper server = cluster.getVeniceServers().get(0);
        assertTrue(server.getVeniceServer().getServerConfig().isStalledDrainerQueueFailureEnabled());
        assertTrue(server.getVeniceServer().getServerConfig().isResetErrorReplicaEnabled());
        assertEquals(server.getVeniceServer().getServerConfig().getBlockedDrainerThresholdMs(), 5000L);
        KafkaStoreIngestionService ingestion = server.getVeniceServer().getKafkaStoreIngestionService();
        StoreIngestionTask originalTask = ingestion.getStoreIngestionTask(topic);
        assertFalse(originalTask.isHybridMode(), "Exercise the sorted delegate");
        PartitionConsumptionState originalPartition = originalTask.getPartitionConsumptionState(0);
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
          assertTrue(originalPartition.isCompletionReported());
          assertEquals(originalPartition.getLeaderFollowerState(), LeaderFollowerStateType.LEADER);
        });
        assertEquals(
            cluster.getLeaderVeniceController()
                .getVeniceHelixAdmin()
                .getHelixVeniceClusterResources(cluster.getClusterName())
                .getConfig()
                .getErrorPartitionProcessingCycleDelay(),
            2000L);
        try (VeniceWriter<String, String, byte[]> writer = cluster.getVeniceWriter(topic);
            AvroGenericStoreClient<String, Object> reader = ClientFactory.getAndStartGenericAvroClient(
                ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(cluster.getRandomRouterURL()))) {
          Sensor sensor = server.getMetricsRepository().getSensor(LATENCY_SENSOR);
          assertNotNull(sensor);
          sensor.add(LATENCY_SENSOR + ".test_lock_holder", new Total() {
            @Override
            public void record(double value, long now) {
              if (Thread.currentThread() == owner.get()) {
                lockHeld.countDown();
                try {
                  assertTrue(releaseLock.await(180, TimeUnit.SECONDS), "Metric lock injection exceeded its bound");
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  throw new AssertionError("Metric lock owner interrupted", e);
                }
              }
              super.record(value, now);
            }
          });
          lockHolder = executor.submit(() -> {
            owner.set(Thread.currentThread());
            sensor.record();
          });
          assertTrue(lockHeld.await(10, TimeUnit.SECONDS));
          // Completed batch-only replicas accept END_OF_SEGMENT, but reject additional user puts.
          // Incremental-push stores are automatically made hybrid and would exercise the unsorted delegate.
          writer.broadcastEndOfPush(Collections.emptyMap());
          writer.flush();
          TestUtils.waitForNonDeterministicAssertion(15, TimeUnit.SECONDS, () -> {
            assertTrue(
                hasFrame("com.linkedin.davinci.stats.StoreBufferServiceStats", "recordInternalProcessingLatency"));
            assertNotNull(originalPartition.getLastQueuedRecordPersistedFuture());
            assertTrue(originalPartition.getLastQueuedRecordPersistedFuture().isDone());
            assertFalse(originalPartition.getLastQueuedRecordPersistedFuture().isCompletedExceptionally());
          });
          for (int i = 0; i < 128; i++) {
            writer.broadcastEndOfPush(Collections.emptyMap());
          }
          writer.flush();

          AtomicReference<Thread> blockedConsumer = new AtomicReference<>();
          TestUtils.waitForNonDeterministicAssertion(15, TimeUnit.SECONDS, () -> {
            assertTrue(
                hasFrame("com.linkedin.davinci.stats.StoreBufferServiceStats", "recordInternalProcessingLatency"));
            Thread consumer = findThread("com.linkedin.venice.utils.collections.MemoryBoundBlockingQueue", "put");
            assertNotNull(consumer, "The real consumer must reach bounded-queue backpressure");
            blockedConsumer.set(consumer);
          });
          CompletableFuture<Void> queuedFuture = originalPartition.getLastQueuedRecordPersistedFuture();
          assertNotNull(queuedFuture);
          assertFalse(queuedFuture.isDone(), "Capture a queued record, not the already-persisted held record");
          LOGGER.info("STALL_PHASE: sorted drainer blocked in real latency sensor; consumer blocked in queue.put");

          // The production monitor has a fixed 30-second scan interval; allow two scans, without changing it.
          TestUtils.waitForNonDeterministicAssertion(65, TimeUnit.SECONDS, () -> {
            assertTrue(queuedFuture.isCompletedExceptionally(), "Discarded record must fail");
            assertFalse(
                containsFrame(
                    blockedConsumer.get().getStackTrace(),
                    "com.linkedin.venice.utils.collections.MemoryBoundBlockingQueue",
                    "put"),
                "Queue failure must release the blocked consumer");
          });
          MetricsRepository controllerMetrics = cluster.getLeaderVeniceController().getMetricRepository();
          String resetMetric = "." + cluster.getClusterName() + "--current_version_error_partition_reset_attempt.Total";
          TestUtils.waitForNonDeterministicAssertion(20, TimeUnit.SECONDS, () -> {
            assertTrue(originalTask.getFailedPartitions().contains(0), "Partition failure must be registered");
            assertNull(originalTask.getLastConsumerException(), "Queue rejection must remain partition-scoped");
            assertTrue(
                cluster.getLeaderVeniceController()
                    .getVeniceHelixAdmin()
                    .getHelixVeniceClusterResources(cluster.getClusterName())
                    .getRoutingDataRepository()
                    .getPartitionAssignments(topic)
                    .getPartition(0)
                    .getErrorInstances()
                    .size() == 1,
                "Affected completed current-version replica must reach actual Helix ERROR");
          });
          TestUtils.waitForNonDeterministicAssertion(
              20,
              TimeUnit.SECONDS,
              () -> assertTrue(controllerMetrics.getMetric(resetMetric).value() >= 1, "Controller must issue reset"));
          assertFalse(lockHolder.isDone());
          assertTrue(hasFrame("com.linkedin.davinci.stats.StoreBufferServiceStats", "recordInternalProcessingLatency"));
          PubSubPosition blockedPosition = originalPartition.getLatestProcessedVtPosition();
          LOGGER.info(
              "ERROR_PHASE: queue future failed, consumer released, partition failure registered; "
                  + "actual Helix ERROR and controller reset observed while metric lock remains held; attempts={}",
              controllerMetrics.getMetric(resetMetric).value());

          releaseLock.countDown();
          lockHolder.get(10, TimeUnit.SECONDS);
          TestUtils.waitForNonDeterministicAssertion(
              10,
              TimeUnit.SECONDS,
              () -> assertEquals(
                  server.getMetricsRepository()
                      .getMetric(".StoreBufferServiceSorted--max_blocked_time_per_writer.Gauge")
                      .value(),
                  0.0));
          TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
            StoreIngestionTask recoveredTask = ingestion.getStoreIngestionTask(topic);
            assertNotNull(recoveredTask);
            assertTrue(recoveredTask.isRunning());
            PartitionConsumptionState recoveredPartition = recoveredTask.getPartitionConsumptionState(0);
            assertNotNull(recoveredPartition);
            assertTrue(recoveredPartition != originalPartition, "Helix reset must create a new subscription");
            assertTrue(recoveredPartition.isCompletionReported(), "Replacement subscription must finish bootstrap");
            assertEquals(recoveredPartition.getLeaderFollowerState(), LeaderFollowerStateType.LEADER);
            assertTrue(
                cluster.getLeaderVeniceController()
                    .getVeniceHelixAdmin()
                    .getHelixVeniceClusterResources(cluster.getClusterName())
                    .getRoutingDataRepository()
                    .getPartitionAssignments(topic)
                    .getPartition(0)
                    .getErrorInstances()
                    .isEmpty());
          });
          PartitionConsumptionState recoveredPartition =
              ingestion.getStoreIngestionTask(topic).getPartitionConsumptionState(0);
          PubSubPosition recoveredPosition = recoveredPartition.getLatestProcessedVtPosition();
          TopicManager topicManager =
              ingestion.getStoreIngestionTask(topic).getTopicManager(cluster.getPubSubBrokerWrapper().getAddress());
          PubSubPosition beforeFreshWrite =
              topicManager.getLatestPositionWithRetries(recoveredPartition.getReplicaTopicPartition(), 3);
          writer.broadcastEndOfPush(Collections.emptyMap());
          writer.flush();
          TestUtils.waitForNonDeterministicAssertion(20, TimeUnit.SECONDS, () -> {
            assertFalse(recoveredPartition.getLatestProcessedVtPosition().equals(recoveredPosition));
            assertFalse(recoveredPartition.getLatestProcessedVtPosition().equals(blockedPosition));
            assertTrue(
                topicManager.comparePosition(
                    recoveredPartition.getReplicaTopicPartition(),
                    recoveredPartition.getLatestProcessedVtPosition(),
                    beforeFreshWrite) > 0,
                "Must consume beyond the log end captured before the fresh write, not just drain old backlog");
          });
          assertEquals(reader.get("initial-key").get(5, TimeUnit.SECONDS).toString(), "initial-value");
          LOGGER.info(
              "RELEASE_PHASE: drainer unblocked; new partition subscription completed bootstrap and became LEADER; "
                  + "fresh END_OF_SEGMENT consumed beyond pre-stall and post-bootstrap positions; batch data readable");
        }
      } finally {
        releaseLock.countDown();
        if (lockHolder != null) {
          lockHolder.get(10, TimeUnit.SECONDS);
        }
      }
    } finally {
      releaseLock.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private static boolean hasFrame(String className, String methodName) {
    return findThread(className, methodName) != null;
  }

  private static Thread findThread(String className, String methodName) {
    return Thread.getAllStackTraces()
        .entrySet()
        .stream()
        .filter(entry -> containsFrame(entry.getValue(), className, methodName))
        .map(Map.Entry::getKey)
        .findFirst()
        .orElse(null);
  }

  private static boolean containsFrame(StackTraceElement[] stack, String className, String methodName) {
    return Arrays.stream(stack)
        .anyMatch(frame -> frame.getClassName().equals(className) && frame.getMethodName().equals(methodName));
  }
}
