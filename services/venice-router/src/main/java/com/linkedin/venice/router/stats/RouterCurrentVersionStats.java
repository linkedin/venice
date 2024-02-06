package com.linkedin.venice.router.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Gauge;


public class RouterCurrentVersionStats extends AbstractVeniceStats {
  private final Sensor currentVersionNumberSensor;

  public RouterCurrentVersionStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);
    this.currentVersionNumberSensor = registerSensor("current_version", new Gauge(-1));
  }

  public void updateCurrentVersion(int currentVersion) {
    this.currentVersionNumberSensor.record(currentVersion);
  }
}
