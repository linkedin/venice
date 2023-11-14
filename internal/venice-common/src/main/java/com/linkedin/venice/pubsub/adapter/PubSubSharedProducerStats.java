package com.linkedin.venice.pubsub.adapter;

import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;


/**
 * A stats class that registers and tracks shared producer related stats.
 */
public class PubSubSharedProducerStats extends AbstractVeniceStats {
  /**
   * Metric to keep track of number of currently active ingestion tasks that is using a shared producer instance.
   */
  private final Sensor sharedProducerActiveTasksCountSensor;

  /**
   * Metric to keep track of number of open shared producer instance.
   */
  private final Sensor sharedProducerActiveCountSensor;

  public PubSubSharedProducerStats(
      MetricsRepository metricsRepository,
      PubSubSharedProducerFactory sharedProducerFactory) {
    super(metricsRepository, "PubSubSharedProducerStats");
    sharedProducerActiveTasksCountSensor = registerSensor(
        new AsyncGauge(
            (c, t) -> sharedProducerFactory.getActiveSharedProducerTasksCount(),
            "shared_producer_active_task_count"));
    sharedProducerActiveCountSensor = registerSensor(
        new AsyncGauge((c, t) -> sharedProducerFactory.getActiveSharedProducerCount(), "shared_producer_active_count"));
  }
}
