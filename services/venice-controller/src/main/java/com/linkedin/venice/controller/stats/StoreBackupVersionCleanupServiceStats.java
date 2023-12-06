package com.linkedin.venice.controller.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.OccurrenceRate;


public class StoreBackupVersionCleanupServiceStats extends AbstractVeniceStats {
  private final Sensor backupVersionMismatchSensor;

  public StoreBackupVersionCleanupServiceStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);

    this.backupVersionMismatchSensor = registerSensor("backup_version_cleanup_version_mismatch", new OccurrenceRate());
  }

  public void recordBackupVersionMismatch() {
    this.backupVersionMismatchSensor.record();
  }
}
