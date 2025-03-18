package com.linkedin.venice.stats;

import com.linkedin.venice.tehuti.MockTehutiReporter;
import io.tehuti.metrics.MetricsRepository;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ThreadPoolStatsTest {
  @Test
  public void testThreadPoolStatsReporterCanReport() {
    MetricsRepository metricsRepository = new MetricsRepository();
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
  }
}
