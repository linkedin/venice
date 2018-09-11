package com.linkedin.venice.controller.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Count;


public class TopicMonitorStats extends AbstractVeniceStats {
  /**
   * Count number of times {@link com.linkedin.venice.controller.kafka.TopicMonitor} thread skipped a cluster because of exceptions.
   */
  final private Sensor clusterSkippedByExceptionSensor;
  /**
   * Count number of time {@link com.linkedin.venice.controller.kafka.TopicMonitor} thread aborted from an execution cycle
   * because of exceptions.
   */
  final private Sensor abortByExceptionSensor;

  public TopicMonitorStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);
    clusterSkippedByExceptionSensor = registerSensor("cluster_skipped_by_exception_count", new Count());
    abortByExceptionSensor = registerSensor("abort_by_exception_count", new Count());
  }

  public void recordClusterSkippedByException() { clusterSkippedByExceptionSensor.record(); }

  public void recordAbortByException() { abortByExceptionSensor.record(); }
}
