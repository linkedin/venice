package com.linkedin.venice.controller.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.Gauge;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import java.util.concurrent.atomic.AtomicLong;


/**
 * This class is the metric class for {@link com.linkedin.venice.controller.systemstore.SystemStoreRepairService}
 */
public class SystemStoreHealthCheckStats extends AbstractVeniceStats {
  private final Sensor badMetaSystemStoreCountSensor;
  private final Sensor badPushStatusSystemStoreCountSensor;
  private final Sensor unreachableSystemStoreCountSensor;
  private final Sensor notRepairableSystemStoreCountSensor;
  private final AtomicLong badMetaSystemStoreCounter = new AtomicLong(0);
  private final AtomicLong badPushStatusSystemStoreCounter = new AtomicLong(0);
  private final AtomicLong unreachableSystemStoreCounter = new AtomicLong(0);
  private final AtomicLong notRepairableSystemStoreCounter = new AtomicLong(0);

  public SystemStoreHealthCheckStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);
    badMetaSystemStoreCountSensor =
        registerSensorIfAbsent("bad_meta_system_store_count", new Gauge(badMetaSystemStoreCounter::get));
    badPushStatusSystemStoreCountSensor =
        registerSensorIfAbsent("bad_push_status_system_store_count", new Gauge(badPushStatusSystemStoreCounter::get));
    unreachableSystemStoreCountSensor =
        registerSensorIfAbsent("unreachable_system_store_count", new Gauge(unreachableSystemStoreCounter::get));
    notRepairableSystemStoreCountSensor =
        registerSensorIfAbsent("not_repairable_system_store_count", new Gauge(notRepairableSystemStoreCounter::get));
  }

  public AtomicLong getBadMetaSystemStoreCounter() {
    return badMetaSystemStoreCounter;
  }

  public AtomicLong getBadPushStatusSystemStoreCounter() {
    return badPushStatusSystemStoreCounter;
  }

  public AtomicLong getUnreachableSystemStoreCounter() {
    return unreachableSystemStoreCounter;
  }

  public AtomicLong getNotRepairableSystemStoreCounter() {
    return notRepairableSystemStoreCounter;
  }
}
