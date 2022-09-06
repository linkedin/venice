package com.linkedin.venice.router.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.OccurrenceRate;


public class StaleVersionStats extends AbstractVeniceStats {
  private final Sensor staleVersionStat;
  private final VeniceConcurrentHashMap<StaleVersionReason, Sensor> staleVersionReasonStats =
      new VeniceConcurrentHashMap<>();

  public StaleVersionStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);
    staleVersionStat = registerSensor("stale_version_delta", new Max());
    for (StaleVersionReason reason: StaleVersionReason.values()) {
      staleVersionReasonStats.computeIfAbsent(
          reason,
          staleVersionReason -> registerSensor(
              "stale_version_reason_" + staleVersionReason.name().toLowerCase(),
              new OccurrenceRate()));
    }
  }

  public void recordNotStale() {
    staleVersionStat.record(0);
  }

  public void recordStale(int metadataCurrentVersion, int servingVersion) {
    staleVersionStat.record((long) (metadataCurrentVersion - servingVersion));
  }

  public void recordStalenessReason(StaleVersionReason reason) {
    Sensor sensor = staleVersionReasonStats.get(reason);
    if (sensor != null) {
      sensor.record();
    }
  }
}
