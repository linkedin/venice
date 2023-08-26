package com.linkedin.venice.controller.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Count;


public class SystemStoreCheckStats extends AbstractVeniceStats {
  private final Sensor badSystemStoreCount;

  public SystemStoreCheckStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);

    badSystemStoreCount = registerSensorIfAbsent("bad_system_store_count", new Count());
  }

  public void recordBadSystemStoreCount(int value) {
    badSystemStoreCount.record(value);
  }
}
