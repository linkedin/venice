package com.linkedin.venice.controller.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;
import java.util.concurrent.atomic.AtomicLong;


/**
 * This class is the metric class for {@link com.linkedin.venice.controller.systemstore.SystemStoreRepairService}
 */
public class SystemStoreHealthCheckStats extends AbstractVeniceStats {
  private final Sensor badMetaSystemStoreCountSensor;
  private final Sensor badPushStatusSystemStoreCountSensor;
  private final Sensor notRepairableSystemStoreCountSensor;
  private final AtomicLong badMetaSystemStoreCounter = new AtomicLong(0);
  private final AtomicLong badPushStatusSystemStoreCounter = new AtomicLong(0);
  private final AtomicLong notRepairableSystemStoreCounter = new AtomicLong(0);

  public SystemStoreHealthCheckStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);
    badMetaSystemStoreCountSensor = registerSensorIfAbsent(
        new AsyncGauge((ignored, ignored2) -> badMetaSystemStoreCounter.get(), "bad_meta_system_store_count"));
    badPushStatusSystemStoreCountSensor = registerSensorIfAbsent(
        new AsyncGauge(
            (ignored, ignored2) -> badPushStatusSystemStoreCounter.get(),
            "bad_push_status_system_store_count"));
    notRepairableSystemStoreCountSensor = registerSensorIfAbsent(
        new AsyncGauge(
            (ignored, ignored2) -> notRepairableSystemStoreCounter.get(),
            "not_repairable_system_store_count"));
  }

  public AtomicLong getBadMetaSystemStoreCounter() {
    return badMetaSystemStoreCounter;
  }

  public AtomicLong getBadPushStatusSystemStoreCounter() {
    return badPushStatusSystemStoreCounter;
  }

  public AtomicLong getNotRepairableSystemStoreCounter() {
    return notRepairableSystemStoreCounter;
  }
}
