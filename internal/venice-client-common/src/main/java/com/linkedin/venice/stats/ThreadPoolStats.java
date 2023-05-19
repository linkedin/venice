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

  private Sensor queuedTasksNumberSensor;

  public ThreadPoolStats(MetricsRepository metricsRepository, ThreadPoolExecutor threadPoolExecutor, String name) {
    super(metricsRepository, name);
    this.threadPoolExecutor = threadPoolExecutor;

    activeThreadNumberSensor =
        registerSensor("active_thread_number", new LambdaStat(() -> this.threadPoolExecutor.getActiveCount()));
    maxThreadNumberSensor =
        registerSensor("max_thread_number", new LambdaStat(() -> this.threadPoolExecutor.getMaximumPoolSize()));
    queuedTasksNumberSensor =
        registerSensor("queued_task_number", new LambdaStat(() -> this.threadPoolExecutor.getQueue().size()));
  }
}
