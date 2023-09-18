package com.linkedin.venice.stats;

import com.linkedin.venice.cleaner.LeakedResourceCleaner;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Total;


/**
 * {@code LeakedResourceCleanerStats} records the occurrences of store resources get removed by {@link LeakedResourceCleaner}.
 */
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
