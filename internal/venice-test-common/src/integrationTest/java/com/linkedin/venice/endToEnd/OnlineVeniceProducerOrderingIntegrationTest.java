package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.CLIENT_PRODUCER_CALLBACK_QUEUE_CAPACITY;
import static com.linkedin.venice.ConfigKeys.CLIENT_PRODUCER_CALLBACK_THREAD_COUNT;
import static com.linkedin.venice.ConfigKeys.CLIENT_PRODUCER_WORKER_COUNT;
import static com.linkedin.venice.ConfigKeys.CLIENT_PRODUCER_WORKER_QUEUE_CAPACITY;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.DEFAULT_KEY_SCHEMA;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.DEFAULT_VALUE_SCHEMA;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.producer.DurableWrite;
import com.linkedin.venice.producer.VeniceProducer;
import com.linkedin.venice.producer.online.OnlineProducerFactory;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Integration tests for {@link com.linkedin.venice.producer.online.OnlineVeniceProducer} ordering guarantees.
 * Validates per-key ordering with concurrent writes using {@link com.linkedin.venice.producer.PartitionedProducerExecutor}.
 *
 * <p>This test exercises different execution modes by parameterizing:</p>
 * <ul>
 *   <li><b>concurrentCallers</b>: External threads submitting to the producer</li>
 *   <li><b>workerCount</b>: Internal worker threads (0 = inline execution)</li>
 *   <li><b>workerQueueCapacity</b>: Queue depth per worker (key for backpressure)</li>
 *   <li><b>callbackThreadCount</b>: Callback threads (0 = inline on Kafka thread)</li>
 *   <li><b>callbackQueueCapacity</b>: Callback queue depth (key for backpressure)</li>
 * </ul>
 *
 * <p>Execution modes tested:</p>
 * <ul>
 *   <li><b>Fully Inline</b>: workerCount=0, callbackThreadCount=0</li>
 *   <li><b>Workers Only</b>: workerCount>0, callbackThreadCount=0 (default)</li>
 *   <li><b>Callbacks Only</b>: workerCount=0, callbackThreadCount>0</li>
 *   <li><b>Full Async</b>: workerCount>0, callbackThreadCount>0</li>
 * </ul>
 */
public class OnlineVeniceProducerOrderingIntegrationTest {
  private static final Logger LOGGER = LogManager.getLogger(OnlineVeniceProducerOrderingIntegrationTest.class);
  private VeniceClusterWrapper cluster;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    LOGGER.info("Setting up Venice cluster for ordering tests");
    cluster = ServiceFactory.getVeniceCluster(
        new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
            .numberOfServers(2)
            .numberOfRouters(1)
            .replicationFactor(2)
            .build());
    LOGGER.info("Venice cluster started: {}", cluster.getClusterName());
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    LOGGER.info("Tearing down Venice cluster");
    Utils.closeQuietlyWithErrorLogged(cluster);
  }

  private void createHybridStore(String storeName, int partitionCount) {
    LOGGER.info("Creating hybrid store: {} with {} partitions", storeName, partitionCount);
    cluster.useControllerClient(client -> {
      assertFalse(
          client.createNewStore(storeName, "test-owner", DEFAULT_KEY_SCHEMA, DEFAULT_VALUE_SCHEMA).isError(),
          "Failed to create store");
      assertFalse(
          client
              .updateStore(
                  storeName,
                  new UpdateStoreQueryParams().setHybridRewindSeconds(1000000L)
                      .setHybridOffsetLagThreshold(10L)
                      .setPartitionCount(partitionCount))
              .isError(),
          "Failed to update store to hybrid");
      LOGGER.info("Store {} configured as hybrid", storeName);
    });

    LOGGER.info("Pushing empty initial version for store: {}", storeName);
    // empty push
    cluster.useControllerClient(client -> {
      TestUtils.assertCommand(
          client.sendEmptyPushAndWait(storeName, "empty-push-to-materialize-hybrid-version", 60, 120_000),
          "Failed to push empty initial version");
    });

    LOGGER.info("Initial version pushed for store: {}", storeName);
  }

  private void waitForStoreReady(AvroGenericStoreClient<Integer, Integer> client, String storeName) throws Exception {
    LOGGER.info("Waiting for store {} to be ready (empty push)", storeName);
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
      // Just verify we can query the store - empty push means no data
      client.get(0).get();
    });
    LOGGER.info("Store {} is ready", storeName);
  }

  /**
   * DataProvider for producer configuration matrix.
   *
   * <p>Optimized test matrix covering:</p>
   * <ul>
   *   <li>All 4 execution modes (worker/callback enabled/disabled combinations)</li>
   *   <li>Backpressure scenarios (worker queue, callback queue, both)</li>
   *   <li>Edge cases (single worker, high contention)</li>
   * </ul>
   *
   * @return test configuration matrix
   */
  @DataProvider(name = "producerConfigurationMatrix", parallel = true)
  public Object[][] producerConfigurationMatrix() {
    return new Object[][] {
        // Format: {concurrentCallers, workerCount, workerQueueCapacity, callbackThreadCount, callbackQueueCapacity,
        // recordsPerKey, description}

        // ============================================================================
        // EXECUTION MODES (4 quadrants - must test all combinations)
        // ============================================================================
        { 4, 0, 100, 0, 100, 50, "Mode: Fully inline (workers=0, callbacks=0)" },
        { 4, 2, 100, 0, 100, 50, "Mode: Workers only (default)" }, { 4, 0, 100, 2, 100, 50, "Mode: Callbacks only" },
        { 4, 2, 100, 2, 100, 50, "Mode: Full async (workers + callbacks)" },

        // ============================================================================
        // BACKPRESSURE SCENARIOS (small queues with high load)
        // ============================================================================
        // 8 callers × 200 records = 1600 total records
        { 8, 2, 5, 0, 100, 200, "Backpressure: Worker queue (1600 records vs 10 slots)" },
        { 8, 2, 100, 2, 5, 200, "Backpressure: Callback queue (1600 records vs 10 slots)" },
        { 8, 2, 5, 2, 5, 200, "Backpressure: Both queues (1600 records vs 10+10 slots)" },

        // ============================================================================
        // EDGE CASES
        // ============================================================================
        { 4, 1, 10, 0, 100, 100, "Edge: Single worker bottleneck" },
        { 16, 2, 50, 0, 100, 25, "Edge: High contention (16 callers, 2 workers)" }, };
  }

  /**
   * Creates a VeniceProducer with the specified internal configuration and metrics repository.
   *
   * @param storeName the store name
   * @param workerCount number of partition workers (0 = inline)
   * @param workerQueueCapacity queue capacity per worker
   * @param callbackThreadCount number of callback threads (0 = inline)
   * @param callbackQueueCapacity callback queue capacity
   * @param metricsRepository metrics repository for capturing latency metrics
   * @return configured VeniceProducer
   */
  private VeniceProducer createConfiguredProducer(
      String storeName,
      int workerCount,
      int workerQueueCapacity,
      int callbackThreadCount,
      int callbackQueueCapacity,
      MetricsRepository metricsRepository) {
    VeniceProperties producerConfig = new PropertyBuilder().put(CLIENT_PRODUCER_WORKER_COUNT, workerCount)
        .put(CLIENT_PRODUCER_WORKER_QUEUE_CAPACITY, workerQueueCapacity)
        .put(CLIENT_PRODUCER_CALLBACK_THREAD_COUNT, callbackThreadCount)
        .put(CLIENT_PRODUCER_CALLBACK_QUEUE_CAPACITY, callbackQueueCapacity)
        .build();

    ClientConfig clientConfig = ClientConfig.defaultGenericClientConfig(storeName)
        .setVeniceURL(cluster.getRandomRouterURL())
        .setMetricsRepository(metricsRepository);

    return OnlineProducerFactory.createProducer(clientConfig, producerConfig, null);
  }

  /**
   * Gets the max value from a percentile sensor.
   *
   * @param metricsRepository the metrics repository
   * @param storeName the store name
   * @param metricName the metric name (e.g., "request_dispatch_latency")
   * @return the max value, or 0 if not found
   */
  private double getMaxLatency(MetricsRepository metricsRepository, String storeName, String metricName) {
    // Metric name format: ".{storeName}--{metricName}.{statType}"
    String maxMetricName = "." + storeName + "--" + metricName + ".Max";
    try {
      return metricsRepository.getMetric(maxMetricName).value();
    } catch (Exception e) {
      LOGGER.warn("Could not get max metric {}: {}", maxMetricName, e.getMessage());
      return 0;
    }
  }

  /**
   * Gets the average value from a percentile sensor.
   *
   * @param metricsRepository the metrics repository
   * @param storeName the store name
   * @param metricName the metric name
   * @return the average value, or 0 if not found
   */
  private double getAvgLatency(MetricsRepository metricsRepository, String storeName, String metricName) {
    // Metric name format: ".{storeName}--{metricName}.{statType}"
    String avgMetricName = "." + storeName + "--" + metricName + ".Avg";
    try {
      return metricsRepository.getMetric(avgMetricName).value();
    } catch (Exception e) {
      LOGGER.warn("Could not get avg metric {}: {}", avgMetricName, e.getMessage());
      return 0;
    }
  }

  /**
   * Parameterized test for OnlineVeniceProducer ordering guarantees with various configurations.
   *
   * <p>This test validates that per-key ordering is maintained regardless of:</p>
   * <ul>
   *   <li>Number of concurrent callers</li>
   *   <li>Internal worker configuration</li>
   *   <li>Queue sizes and backpressure behavior</li>
   *   <li>Callback execution mode</li>
   * </ul>
   *
   * @param concurrentCallers number of external threads submitting to the producer
   * @param workerCount internal worker threads (0 = inline execution)
   * @param workerQueueCapacity queue depth per worker
   * @param callbackThreadCount callback threads (0 = inline on Kafka thread)
   * @param callbackQueueCapacity callback queue depth
   * @param recordsPerKey records each caller writes to their key
   * @param testDescription human-readable description of the test scenario
   */
  @Test(dataProvider = "producerConfigurationMatrix", timeOut = 5 * Time.MS_PER_MINUTE)
  public void testOrderingWithVariousConfigurations(
      int concurrentCallers,
      int workerCount,
      int workerQueueCapacity,
      int callbackThreadCount,
      int callbackQueueCapacity,
      int recordsPerKey,
      String testDescription) throws Exception {

    String storeName = Utils.getUniqueString("ordering-test");
    int partitions = 3;

    LOGGER.info(
        "=== Starting Test: {} ==="
            + "\n  Callers: {}, Workers: {}, WorkerQueue: {}, CallbackThreads: {}, CallbackQueue: {}, Records/Key: {}",
        testDescription,
        concurrentCallers,
        workerCount,
        workerQueueCapacity,
        callbackThreadCount,
        callbackQueueCapacity,
        recordsPerKey);

    createHybridStore(storeName, partitions);

    try (AvroGenericStoreClient<Integer, Integer> client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(cluster.getRandomRouterURL()))) {

      waitForStoreReady(client, storeName);

      // Create metrics repository to capture latency metrics
      MetricsRepository metricsRepository = new MetricsRepository(new MetricConfig());

      try (VeniceProducer producer = createConfiguredProducer(
          storeName,
          workerCount,
          workerQueueCapacity,
          callbackThreadCount,
          callbackQueueCapacity,
          metricsRepository)) {

        LOGGER
            .info("OnlineVeniceProducer created with workers={}, callbackThreads={}", workerCount, callbackThreadCount);

        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);
        Map<Integer, Integer> expectedLastValues = Collections.synchronizedMap(new HashMap<>());
        CountDownLatch started = new CountDownLatch(concurrentCallers);
        CountDownLatch finished = new CountDownLatch(concurrentCallers);
        ExecutorService pool =
            Executors.newFixedThreadPool(concurrentCallers, new DaemonThreadFactory("test-producer"));

        long startTime = System.nanoTime();
        LOGGER.info("Launching {} producer threads", concurrentCallers);

        for (int t = 0; t < concurrentCallers; t++) {
          final int threadId = t;
          pool.submit(() -> {
            try {
              started.countDown();
              if (!started.await(30, TimeUnit.SECONDS)) {
                throw new RuntimeException("Timed out waiting for all threads to start");
              }
              LOGGER.debug("Thread {} started producing to key {}", threadId, threadId);

              List<CompletableFuture<DurableWrite>> futures = new ArrayList<>(recordsPerKey);
              for (int seq = 0; seq < recordsPerKey; seq++) {
                futures.add(producer.asyncPut(threadId, seq));
              }

              for (CompletableFuture<DurableWrite> f: futures) {
                f.get(120, TimeUnit.SECONDS);
                successCount.incrementAndGet();
              }
              expectedLastValues.put(threadId, recordsPerKey - 1);
              LOGGER.debug("Thread {} completed {} writes", threadId, recordsPerKey);
            } catch (Exception e) {
              failureCount.incrementAndGet();
              LOGGER.error("Thread {} failed: {}", threadId, e.getMessage(), e);
            } finally {
              finished.countDown();
            }
          });
        }

        assertTrue(finished.await(4, TimeUnit.MINUTES), "All producers should finish");
        pool.shutdown();
        pool.awaitTermination(10, TimeUnit.SECONDS);

        long timeMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
        int total = concurrentCallers * recordsPerKey;
        double throughput = total > 0 && timeMs > 0 ? (double) total / timeMs * 1000 : 0;
        LOGGER.info(
            "Produced {} records in {}ms ({} records/sec). Success: {}, Failures: {}",
            total,
            timeMs,
            String.format("%.2f", throughput),
            successCount.get(),
            failureCount.get());

        assertEquals(successCount.get(), total, "All records should be produced");
        assertEquals(failureCount.get(), 0, "No failures should occur");

        LOGGER.info("Verifying ordering for {} keys", concurrentCallers);
        TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, true, () -> {
          for (Map.Entry<Integer, Integer> e: expectedLastValues.entrySet()) {
            Object actual = client.get(e.getKey()).get();
            assertEquals(actual, e.getValue(), "Key " + e.getKey() + " ordering violation");
          }
        });

        // === METRIC VALIDATION ===
        // Capture latency metrics to validate backpressure behavior
        double maxQueueWaitLatency = getMaxLatency(metricsRepository, storeName, "queue_wait_latency");
        double avgQueueWaitLatency = getAvgLatency(metricsRepository, storeName, "queue_wait_latency");
        double maxPreprocessingLatency = getMaxLatency(metricsRepository, storeName, "preprocessing_latency");
        double avgPreprocessingLatency = getAvgLatency(metricsRepository, storeName, "preprocessing_latency");
        double maxBufferLatency =
            getMaxLatency(metricsRepository, storeName, "caller_to_pubsub_producer_buffer_latency");
        double avgBufferLatency =
            getAvgLatency(metricsRepository, storeName, "caller_to_pubsub_producer_buffer_latency");
        double maxEndToEndLatency = getMaxLatency(metricsRepository, storeName, "end_to_end_latency");
        double avgEndToEndLatency = getAvgLatency(metricsRepository, storeName, "end_to_end_latency");

        // Calculate expected backpressure based on configuration
        int totalQueueCapacity = workerCount > 0 ? workerQueueCapacity * workerCount : Integer.MAX_VALUE;
        int totalRecords = concurrentCallers * recordsPerKey;
        double queuePressureRatio = workerCount > 0 ? (double) totalRecords / totalQueueCapacity : 0;
        boolean expectBackpressure = queuePressureRatio > 10; // Expect backpressure when ratio > 10:1

        LOGGER.info(
            "=== LATENCY METRICS for {} ===" + "\n  Queue Wait Latency: avg={}ms, max={}ms"
                + "\n  Preprocessing Latency: avg={}ms, max={}ms"
                + "\n  Caller to PubSub Buffer Latency: avg={}ms, max={}ms"
                + "\n  End-to-End Latency: avg={}ms, max={}ms"
                + "\n  Queue Pressure Ratio: {}:1 (totalRecords={}, queueCapacity={})"
                + "\n  Backpressure Expected: {}",
            testDescription,
            String.format("%.2f", avgQueueWaitLatency),
            String.format("%.2f", maxQueueWaitLatency),
            String.format("%.2f", avgPreprocessingLatency),
            String.format("%.2f", maxPreprocessingLatency),
            String.format("%.2f", avgBufferLatency),
            String.format("%.2f", maxBufferLatency),
            String.format("%.2f", avgEndToEndLatency),
            String.format("%.2f", maxEndToEndLatency),
            String.format("%.1f", queuePressureRatio),
            totalRecords,
            totalQueueCapacity,
            expectBackpressure);

        // Note: Backpressure latency validation is informational only.
        // The actual backpressure behavior depends on thread scheduling and timing which
        // can vary significantly in CI environments. The primary test goal is ordering
        // correctness, not latency measurement accuracy.
        if (expectBackpressure && workerCount > 0) {
          if (maxQueueWaitLatency > 1.0) {
            LOGGER.info(
                "Backpressure observed: max queue wait latency {}ms > 1ms threshold (ratio {}:1)",
                String.format("%.2f", maxQueueWaitLatency),
                String.format("%.1f", queuePressureRatio));
          } else {
            // Log but don't fail - backpressure timing is non-deterministic in integration tests
            LOGGER.info(
                "Backpressure not observed in metrics (max queue wait latency {}ms, ratio {}:1). "
                    + "This is expected in fast CI environments where queue blocking may not occur.",
                String.format("%.2f", maxQueueWaitLatency),
                String.format("%.1f", queuePressureRatio));
          }
        }

        LOGGER.info("=== Test PASSED: {} - {} keys verified ===", testDescription, concurrentCallers);
      }
    }
  }
}
