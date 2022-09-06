package com.linkedin.venice.stats;

import io.tehuti.metrics.MeasurableStat;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.Max;
import java.util.function.Supplier;


public class VeniceLockStats extends AbstractVeniceStats {
  public final Sensor lockAcquisitionTimeMs;
  public final Sensor lockRetentionTimeMs;
  public final Sensor successfulLockAcquisition;
  public final Sensor failedLockAcquisition;

  public VeniceLockStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);
    Supplier<MeasurableStat[]> countStats = () -> new MeasurableStat[] { new Count() };
    lockAcquisitionTimeMs = registerSensorIfAbsent("lock_acquisition_time_ms", new Avg(), new Max());
    lockRetentionTimeMs = registerSensorIfAbsent("lock_retention_time_ms", new Avg(), new Max());
    successfulLockAcquisition = registerSensorWithAggregate("successful_lock_acquisition", countStats);
    failedLockAcquisition = registerSensorWithAggregate("failed_lock_acquisition", countStats);
  }
}
