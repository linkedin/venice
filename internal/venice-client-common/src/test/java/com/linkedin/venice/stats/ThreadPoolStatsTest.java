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
      ThreadPoolStats stats = new ThreadPoolStats(metricsRepository, threadPool, name);

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

      stats.recordActiveThreadsCount();
      Mockito.doReturn(5).when(threadPool).getActiveCount();
      stats.recordActiveThreadsCount();
      Mockito.doReturn(0).when(threadPool).getActiveCount();
      stats.recordActiveThreadsCount();
      Assert.assertNull(metricsRepository.getMetric("." + name + "--active_thread_count.Avg"));
      Assert.assertNull(metricsRepository.getMetric("." + name + "--active_thread_count.Max"));
      Assert.assertEquals(reporter.query("." + name + "--active_thread_number.LambdaStat").value(), 0.0);
    } finally {
      metricsRepository.close();
    }
  }
}
