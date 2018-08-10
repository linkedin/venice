package com.linkedin.venice.router.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Max;


public class StaleVersionStats extends AbstractVeniceStats {
  private final Sensor staleVersionStat;

  public StaleVersionStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);
    staleVersionStat = registerSensor("stale_version_delta", new Max());
  }

  public void recordNotStale(){
    staleVersionStat.record(0);
  }

  public void recordStale(int metadataCurrentVersion, int servingVersion){
    staleVersionStat.record((long)(metadataCurrentVersion - servingVersion));
  }
}
