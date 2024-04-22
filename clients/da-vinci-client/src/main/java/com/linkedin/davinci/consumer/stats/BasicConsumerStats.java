package com.linkedin.davinci.consumer.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Max;


public class BasicConsumerStats extends AbstractVeniceStats {
  public BasicConsumerStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);
    maxLagSensor = registerSensor("max_partition_lag", new Max());
  }

  private final Sensor maxLagSensor;

  public void recordLag(Long lag) {
    maxLagSensor.record(lag);
  }
}
