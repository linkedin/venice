package com.linkedin.venice.stats;

import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Total;


public class LeakedResourceCleanerStats extends AbstractVeniceStats {
  private final Sensor leakedVersionTotalSensor;

  public LeakedResourceCleanerStats(MetricsRepository metricsRepository) {
    super(metricsRepository, "LeakedResourceCleaner");

    this.leakedVersionTotalSensor = registerSensor("leaked_version", new Total());
  }

  public void recordLeakedVersion() {
    leakedVersionTotalSensor.record();
  }
}
