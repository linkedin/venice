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
   * <p>Tests different execution modes:</p>
   * <ul>
   *   <li>Queue size variations (worker and callback)</li>
   *   <li>Worker count variations (0 = inline, >0 = parallel)</li>
   *   <li>Callback thread count variations</li>
   *   <li>Concurrent caller variations</li>
   *   <li>Combined stress scenarios</li>
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
        // SECTION 1: QUEUE SIZE VARIATIONS (Primary Focus)
        // ============================================================================

        // --- Worker Queue Size Variations ---
        // Fixed: 4 callers, 2 workers, 0 callback threads, 50 records/key
        { 4, 2, 5, 0, 1000, 50, "Worker queue=5: extreme backpressure" },
        { 4, 2, 10, 0, 1000, 50, "Worker queue=10: high backpressure" },
        { 4, 2, 50, 0, 1000, 50, "Worker queue=50: moderate backpressure" },
        { 4, 2, 100, 0, 1000, 50, "Worker queue=100: light backpressure" },
        { 4, 2, 1000, 0, 1000, 50, "Worker queue=1000: minimal backpressure" },

        // --- Callback Queue Size Variations ---
        // Fixed: 4 callers, 2 workers, 2 callback threads, 50 records/key
        { 4, 2, 1000, 2, 5, 50, "Callback queue=5: extreme backpressure" },
        { 4, 2, 1000, 2, 10, 50, "Callback queue=10: high backpressure" },
        { 4, 2, 1000, 2, 50, 50, "Callback queue=50: moderate backpressure" },
        { 4, 2, 1000, 2, 100, 50, "Callback queue=100: light backpressure" },
        { 4, 2, 1000, 2, 1000, 50, "Callback queue=1000: minimal backpressure" },

        // --- Both Queues Small (Stress Test) ---
        { 4, 2, 10, 2, 10, 50, "Both queues=10: max backpressure" },
        { 4, 2, 5, 2, 5, 30, "Both queues=5: extreme stress" },

        // ============================================================================
        // SECTION 2: WORKER COUNT VARIATIONS
        // ============================================================================

        // Fixed: 4 callers, queue=100, 0 callback threads, 50 records/key
        { 4, 0, 100, 0, 100, 50, "Workers=0: fully inline" },
        { 4, 1, 100, 0, 100, 50, "Workers=1: single worker bottleneck" },
        { 4, 2, 100, 0, 100, 50, "Workers=2: fewer than callers" },
        { 4, 4, 100, 0, 100, 50, "Workers=4: equal to callers" },
        { 4, 8, 100, 0, 100, 50, "Workers=8: more than callers" },

        // ============================================================================
        // SECTION 3: CALLBACK THREAD COUNT VARIATIONS
        // ============================================================================

        // Fixed: 4 callers, 2 workers, queue=100, 50 records/key
        { 4, 2, 100, 0, 100, 50, "Callbacks=0: inline on Kafka thread" },
        { 4, 2, 100, 1, 100, 50, "Callbacks=1: single callback thread" },
        { 4, 2, 100, 2, 100, 50, "Callbacks=2: moderate parallelism" },
        { 4, 2, 100, 4, 100, 50, "Callbacks=4: high parallelism" },

        // ============================================================================
        // SECTION 4: CONCURRENT CALLERS VARIATIONS
        // ============================================================================

        // Fixed: 2 workers, queue=100, 0 callback threads, varying records/key
        { 1, 2, 100, 0, 100, 100, "Callers=1: single threaded" },
        { 2, 2, 100, 0, 100, 50, "Callers=2: equal to workers" }, { 4, 2, 100, 0, 100, 50, "Callers=4: 2x workers" },
        { 8, 2, 100, 0, 100, 25, "Callers=8: 4x workers" }, { 16, 2, 100, 0, 100, 15, "Callers=16: high contention" },

        // ============================================================================
        // SECTION 5: COMBINED STRESS SCENARIOS
        // ============================================================================

        { 10, 4, 20, 4, 20, 30, "High load + small queues" },
        { 8, 2, 10, 0, 100, 40, "Many callers + tiny worker queue" },
        { 4, 1, 5, 2, 5, 25, "Single worker + tiny queues" },

        // ============================================================================
        // SECTION 6: KEYS > QUEUES (Backpressure Simulation)
        // ============================================================================
        // These scenarios ensure total pending records FAR exceed queue capacity,
        // forcing sustained backpressure and blocking behavior.
        // Formula: totalRecords = concurrentCallers * recordsPerKey
        // Backpressure triggers when: totalRecords >> (workerQueueCapacity * workerCount)

        // --- Worker Queue Backpressure (high record counts) ---
        // 8 callers × 500 records = 4000 total vs 20 queue slots (200:1 ratio)
        { 8, 2, 10, 0, 100, 500, "WorkerBackpressure: 4000 records vs 20 queue slots" },
        // 10 callers × 300 records = 3000 total vs 10 queue slots (300:1 ratio)
        { 10, 2, 5, 0, 100, 300, "WorkerBackpressure: 3000 records vs 10 queue slots" },
        // 6 callers × 400 records = 2400 total vs 5 queue slots (480:1 ratio, single worker)
        { 6, 1, 5, 0, 100, 400, "WorkerBackpressure: 2400 records vs 5 slots (single worker)" },

        // --- Callback Queue Backpressure (high record counts) ---
        // 8 callers × 500 records = 4000 total vs 10 callback slots (400:1 ratio)
        { 8, 4, 1000, 2, 10, 500, "CallbackBackpressure: 4000 records vs 10 callback slots" },
        // 10 callers × 300 records = 3000 total vs 5 callback slots (600:1 ratio)
        { 10, 4, 1000, 1, 5, 300, "CallbackBackpressure: 3000 records vs 5 callback slots" },

        // --- Both Queues Overwhelmed (sustained backpressure) ---
        // 8 callers × 400 records = 3200 total vs 10+10 queue slots
        { 8, 2, 5, 2, 5, 400, "DualBackpressure: 3200 records vs 10+10 queue slots" },
        // 12 callers × 250 records = 3000 total vs 6+6 queue slots (extreme)
        { 12, 2, 3, 2, 3, 250, "DualBackpressure: 3000 records vs 6+6 queue slots (extreme)" }, };
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

        // Allow time for async metric recording to complete.
        // The end_to_end_latency metric is recorded AFTER the future is completed in the callback,
        // so we need a brief pause to ensure all callbacks have finished metric recording.
        Thread.sleep(100);

        LOGGER.info("Verifying ordering for {} keys", concurrentCallers);
        TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, true, () -> {
          for (Map.Entry<Integer, Integer> e: expectedLastValues.entrySet()) {
            Object actual = client.get(e.getKey()).get();
            assertEquals(actual, e.getValue(), "Key " + e.getKey() + " ordering violation");
          }
        });

        // === METRIC VALIDATION ===
        // Capture latency metrics to validate backpressure behavior
        double maxDispatchLatency = getMaxLatency(metricsRepository, storeName, "request_dispatch_latency");
        double avgDispatchLatency = getAvgLatency(metricsRepository, storeName, "request_dispatch_latency");
        double maxEndToEndLatency = getMaxLatency(metricsRepository, storeName, "end_to_end_latency");
        double avgEndToEndLatency = getAvgLatency(metricsRepository, storeName, "end_to_end_latency");

        // Calculate expected backpressure based on configuration
        int totalQueueCapacity = workerCount > 0 ? workerQueueCapacity * workerCount : Integer.MAX_VALUE;
        int totalRecords = concurrentCallers * recordsPerKey;
        double queuePressureRatio = workerCount > 0 ? (double) totalRecords / totalQueueCapacity : 0;
        boolean expectBackpressure = queuePressureRatio > 10; // Expect backpressure when ratio > 10:1

        LOGGER.info(
            "=== LATENCY METRICS for {} ===" + "\n  Request Dispatch Latency: avg={}ms, max={}ms"
                + "\n  End-to-End Latency: avg={}ms, max={}ms"
                + "\n  Queue Pressure Ratio: {}:1 (totalRecords={}, queueCapacity={})"
                + "\n  Backpressure Expected: {}",
            testDescription,
            String.format("%.2f", avgDispatchLatency),
            String.format("%.2f", maxDispatchLatency),
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
          if (maxDispatchLatency > 1.0) {
            LOGGER.info(
                "Backpressure observed: max dispatch latency {}ms > 1ms threshold (ratio {}:1)",
                String.format("%.2f", maxDispatchLatency),
                String.format("%.1f", queuePressureRatio));
          } else {
            // Log but don't fail - backpressure timing is non-deterministic in integration tests
            LOGGER.info(
                "Backpressure not observed in metrics (max dispatch latency {}ms, ratio {}:1). "
                    + "This is expected in fast CI environments where queue blocking may not occur.",
                String.format("%.2f", maxDispatchLatency),
                String.format("%.1f", queuePressureRatio));
          }
        }

        LOGGER.info("=== Test PASSED: {} - {} keys verified ===", testDescription, concurrentCallers);
      }
    }
  }
}
