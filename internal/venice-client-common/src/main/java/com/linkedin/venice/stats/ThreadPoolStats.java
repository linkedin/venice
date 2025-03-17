package com.linkedin.venice.stats;

import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import java.util.concurrent.ThreadPoolExecutor;


/**
 * Stats used to collect the usage of a thread pool including: 1. active thread number, 2. max thread number and 3.
 * queued task number.
 */
public class ThreadPoolStats extends AbstractVeniceStats {
  private ThreadPoolExecutor threadPoolExecutor;

  private Sensor activeThreadNumberSensor;

  private Sensor maxThreadNumberSensor;

  private Sensor queuedTasksCountSensor;

  public ThreadPoolStats(MetricsRepository metricsRepository, ThreadPoolExecutor threadPoolExecutor, String name) {
    super(metricsRepository, name);
    this.threadPoolExecutor = threadPoolExecutor;

    activeThreadNumberSensor = registerSensor(
        new LambdaStat((ignored, ignored2) -> this.threadPoolExecutor.getActiveCount(), "active_thread_number"));
    maxThreadNumberSensor = registerSensor(
        new LambdaStat((ignored, ignored2) -> this.threadPoolExecutor.getMaximumPoolSize(), "max_thread_number"));
    registerSensor(
        new LambdaStat((ignored, ignored2) -> this.threadPoolExecutor.getQueue().size(), "queued_task_count_gauge"));
    /**
     * If only registered as Gauge, the metric would show the queue size at the time of the metric collection, which is not
     * very useful. It can provide a better view of the queue size if we record the average and max queue size within
     * the metric reporting time window which is usually 1 minute.
     * As a result, we need the users of the thread pool to explicitly call the record function to record the queue size
     * during each new task submission.
     */
    queuedTasksCountSensor = registerSensor("queued_task_count", avgAndMax());
  }

  public void recordQueuedTasksCount(int queuedTasksNumber) {
    // TODO: remove the argument in the function; you don't need it
    queuedTasksCountSensor.record(this.threadPoolExecutor.getQueue().size());
  }
}
