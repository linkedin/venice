package com.linkedin.venice.stats;

import com.linkedin.venice.tehuti.MockTehutiReporter;
import com.linkedin.venice.utils.metrics.MetricsRepositoryUtils;
import io.tehuti.metrics.MetricsRepository;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ThreadPoolStatsTest {
  @Test
  public void testThreadPoolStatsReporterCanReport() {
    // createSingleThreadedMetricsRepository builds a dedicated AsyncGaugeExecutor — close in finally
    // to release it. Tehuti MetricsRepository is not AutoCloseable.
    MetricsRepository metricsRepository = MetricsRepositoryUtils.createSingleThreadedMetricsRepository();
    try {
      MockTehutiReporter reporter = new MockTehutiReporter();
      metricsRepository.addReporter(reporter);

      ThreadPoolExecutor threadPool = Mockito.mock(ThreadPoolExecutor.class);
      String name = "test_pool";
      new ThreadPoolStats(metricsRepository, threadPool, name);

      int activeThreadNumber = 1;
      int maxThreadNumber = 2;
      BlockingQueue<Runnable> queue = Mockito.mock(BlockingQueue.class);
      int queuedTaskNumber = 100;
      Mockito.doReturn(activeThreadNumber).when(threadPool).getActiveCount();
      Mockito.doReturn(maxThreadNumber).when(threadPool).getMaximumPoolSize();
      Mockito.doReturn(queue).when(threadPool).getQueue();
      Mockito.doReturn(queuedTaskNumber).when(queue).size();

      Assert.assertEquals(
          (int) reporter.query("." + name + "--active_thread_number.LambdaStat").value(),
          activeThreadNumber);
      Assert.assertEquals((int) reporter.query("." + name + "--max_thread_number.LambdaStat").value(), maxThreadNumber);
      Assert.assertEquals(
          (int) reporter.query("." + name + "--queued_task_count_gauge.LambdaStat").value(),
          queuedTaskNumber);
    } finally {
      metricsRepository.close();
    }
  }

  @Test
  public void testRecordActiveThreadCountReportsAvgAndMax() {
    MetricsRepository metricsRepository = MetricsRepositoryUtils.createSingleThreadedMetricsRepository();
    try {
      MockTehutiReporter reporter = new MockTehutiReporter();
      metricsRepository.addReporter(reporter);

      ThreadPoolExecutor threadPool = Mockito.mock(ThreadPoolExecutor.class);
      BlockingQueue<Runnable> queue = Mockito.mock(BlockingQueue.class);
      Mockito.doReturn(queue).when(threadPool).getQueue();
      Mockito.doReturn(0).when(queue).size();
      String name = "test_pool_active_thread_count";
      ThreadPoolStats stats = new ThreadPoolStats(metricsRepository, threadPool, name);

      // Simulate two request submissions observing different active thread counts.
      Mockito.doReturn(2).when(threadPool).getActiveCount();
      stats.recordActiveThreadCount();
      Mockito.doReturn(8).when(threadPool).getActiveCount();
      stats.recordActiveThreadCount();

      Assert.assertEquals(reporter.query("." + name + "--active_thread_count.Avg").value(), 5.0);
      Assert.assertEquals(reporter.query("." + name + "--active_thread_count.Max").value(), 8.0);
    } finally {
      metricsRepository.close();
    }
  }

  /**
   * Edge case: a thread pool that has never had any active threads (e.g. immediately after construction, before any
   * request has been submitted) should still be able to record a zero active thread count without error, and the
   * gauge-based "active_thread_number" (collection-time) metric must remain independent of the request-triggered
   * "active_thread_count" (Avg/Max) metric.
   */
  @Test
  public void testRecordActiveThreadCountWithZeroActiveThreadsDoesNotThrow() {
    MetricsRepository metricsRepository = MetricsRepositoryUtils.createSingleThreadedMetricsRepository();
    try {
      MockTehutiReporter reporter = new MockTehutiReporter();
      metricsRepository.addReporter(reporter);

      ThreadPoolExecutor threadPool = Mockito.mock(ThreadPoolExecutor.class);
      BlockingQueue<Runnable> queue = Mockito.mock(BlockingQueue.class);
      Mockito.doReturn(queue).when(threadPool).getQueue();
      Mockito.doReturn(0).when(queue).size();
      Mockito.doReturn(0).when(threadPool).getActiveCount();
      String name = "test_pool_idle";
      ThreadPoolStats stats = new ThreadPoolStats(metricsRepository, threadPool, name);

      stats.recordActiveThreadCount();

      Assert.assertEquals(reporter.query("." + name + "--active_thread_count.Avg").value(), 0.0);
      Assert.assertEquals(reporter.query("." + name + "--active_thread_count.Max").value(), 0.0);
      // The collection-time gauge is unaffected by the request-triggered recording.
      Assert.assertEquals((int) reporter.query("." + name + "--active_thread_number.LambdaStat").value(), 0);
    } finally {
      metricsRepository.close();
    }
  }
}
