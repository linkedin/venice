package com.linkedin.venice.router.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.Gauge;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;


public class RouterCurrentVersionStats extends AbstractVeniceStats {
  private final Sensor currentVersionNumberSensor;
  private int currentVersion = -1;

  public RouterCurrentVersionStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);
    this.currentVersionNumberSensor = registerSensor("current_version", new Gauge(() -> this.currentVersion));
  }

  public void updateCurrentVersion(int currentVersion) {
    this.currentVersion = currentVersion;
  }
}
