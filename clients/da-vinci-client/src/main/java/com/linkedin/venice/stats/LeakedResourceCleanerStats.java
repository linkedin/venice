package com.linkedin.venice.stats;

import com.linkedin.venice.cleaner.LeakedResourceCleaner;
import com.linkedin.venice.utils.lazy.Lazy;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Total;


/**
 * {@code LeakedResourceCleanerStats} records the occurrences of store resources get removed by {@link LeakedResourceCleaner}.
 */
public class LeakedResourceCleanerStats extends AbstractVeniceStats {
  private final Lazy<Sensor> leakedVersionTotalSensor;

  public LeakedResourceCleanerStats(MetricsRepository metricsRepository) {
    super(metricsRepository, "LeakedResourceCleaner");

    this.leakedVersionTotalSensor = registerLazySensor("leaked_version", new Total());
  }

  public void recordLeakedVersion() {
    leakedVersionTotalSensor.get().record();
  }
}
