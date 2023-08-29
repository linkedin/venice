package com.linkedin.venice.controller.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.Gauge;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;


public class SystemStoreCheckStats extends AbstractVeniceStats {
  private final Sensor badMetaSystemStoreCount;
  private final Sensor badPushStatusSystemStoreCount;

  public SystemStoreCheckStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);
    badMetaSystemStoreCount = registerSensorIfAbsent("bad_meta_system_store_count", new Gauge());
    badPushStatusSystemStoreCount = registerSensorIfAbsent("bad_push_status_system_store_count", new Gauge());
  }

  public void recordBadMetaSystemStoreCount(long value) {
    badMetaSystemStoreCount.record(value);
  }

  public void recordBadPushStatusSystemStoreCount(long value) {
    badPushStatusSystemStoreCount.record(value);
  }
}
