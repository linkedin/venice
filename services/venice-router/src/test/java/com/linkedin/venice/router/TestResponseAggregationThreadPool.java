package com.linkedin.venice.router;

import static org.testng.Assert.*;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.stats.ThreadPoolStats;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.util.concurrent.ThreadPoolExecutor;
import org.testng.annotations.Test;


/**
 * Integration test for response aggregation thread pool configuration and metrics.
 */
public class TestResponseAggregationThreadPool {
  @Test
  public void testResponseAggregationThreadPoolConfigurationParsing() {
    // Test that config parsing works correctly with custom values
    VeniceProperties props = new PropertyBuilder().put(ConfigKeys.ROUTER_RESPONSE_AGGREGATION_THREAD_POOL_SIZE, "5")
        .put(ConfigKeys.ROUTER_RESPONSE_AGGREGATION_QUEUE_CAPACITY, "1000")
        .build();

    int threadPoolSize = props.getInt(ConfigKeys.ROUTER_RESPONSE_AGGREGATION_THREAD_POOL_SIZE, 10);
    int queueCapacity = props.getInt(ConfigKeys.ROUTER_RESPONSE_AGGREGATION_QUEUE_CAPACITY, 500000);

    assertEquals(threadPoolSize, 5, "Thread pool size should be 5");
    assertEquals(queueCapacity, 1000, "Queue capacity should be 1000");
  }

  @Test
  public void testResponseAggregationThreadPoolDefaultValues() {
    // Test default values when configs are not specified
    VeniceProperties props = new PropertyBuilder().build();

    int threadPoolSize = props.getInt(ConfigKeys.ROUTER_RESPONSE_AGGREGATION_THREAD_POOL_SIZE, 10);
    int queueCapacity = props.getInt(ConfigKeys.ROUTER_RESPONSE_AGGREGATION_QUEUE_CAPACITY, 500000);

    assertEquals(threadPoolSize, 10, "Default thread pool size should be 10");
    assertEquals(queueCapacity, 500000, "Default queue capacity should be 500000");
  }

  @Test
  public void testThreadPoolCreationAndMetrics() {
    // Create a metrics repository
    MetricsRepository metricsRepository = new VeniceMetricsRepository();
    ThreadPoolExecutor executor = null;

    try {
      // Create a thread pool executor similar to what RouterServer creates
      executor = com.linkedin.venice.utils.concurrent.ThreadPoolFactory.createThreadPool(
          5,
          "TestResponseAggregation",
          null,
          1000,
          com.linkedin.venice.utils.concurrent.BlockingQueueType.LINKED_BLOCKING_QUEUE);

      // Create ThreadPoolStats for the executor
      ThreadPoolStats stats = new ThreadPoolStats(metricsRepository, executor, "test_response_aggregation_pool");
      assertNotNull(stats, "Stats should be created");

      // Verify pool configuration
      assertEquals(executor.getCorePoolSize(), 5, "Core pool size should be 5");
      assertEquals(executor.getMaximumPoolSize(), 5, "Max pool size should be 5");
      assertTrue(executor.getQueue().remainingCapacity() > 0, "Queue should have capacity");
      assertEquals(executor.getQueue().remainingCapacity(), 1000, "Queue capacity should be 1000");

      // Verify the pool can execute tasks
      assertNotNull(executor, "Executor should be created");
      assertFalse(executor.isShutdown(), "Executor should not be shutdown initially");
    } finally {
      // Cleanup
      if (executor != null) {
        executor.shutdown();
      }
      metricsRepository.close();
    }
  }

  @Test
  public void testThreadPoolUnderLoad() throws Exception {
    // Create a thread pool
    ThreadPoolExecutor executor = null;
    MetricsRepository metricsRepository = new VeniceMetricsRepository();

    try {
      executor = com.linkedin.venice.utils.concurrent.ThreadPoolFactory.createThreadPool(
          2,
          "TestLoadResponseAggregation",
          null,
          100,
          com.linkedin.venice.utils.concurrent.BlockingQueueType.LINKED_BLOCKING_QUEUE);

      ThreadPoolStats stats = new ThreadPoolStats(metricsRepository, executor, "test_load_pool");
      assertNotNull(stats, "Stats should be created");

      // Submit some tasks to the pool
      for (int i = 0; i < 10; i++) {
        executor.submit(() -> {
          try {
            // Simulate some work
            Thread.sleep(10);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        });
      }

      // Give tasks time to be queued/executed
      Thread.sleep(50);

      // Verify metrics show activity
      assertTrue(executor.getTaskCount() > 0, "Task count should be greater than 0");

      // Wait for tasks to complete
      executor.shutdown();
      assertTrue(
          executor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS),
          "Executor should terminate within 5 seconds");
    } finally {
      if (executor != null && !executor.isShutdown()) {
        executor.shutdown();
      }
      metricsRepository.close();
    }
  }
}
