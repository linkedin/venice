package com.linkedin.venice.stats;

import com.linkedin.venice.cleaner.BackupVersionOptimizationService;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.OccurrenceRate;


/**
 * {@code BackupVersionOptimizationServiceStats} record the statistics for the database optimization done by the
 * {@link BackupVersionOptimizationService} including both successes and failures.
 */
public class BackupVersionOptimizationServiceStats extends AbstractVeniceStats {
  private final Sensor optimizationSensor;
  private final Sensor optimizationErrorSensor;

  public BackupVersionOptimizationServiceStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);

    this.optimizationSensor = registerSensor("backup_version_database_optimization", new OccurrenceRate());
    this.optimizationErrorSensor = registerSensor("backup_version_data_optimization_error", new OccurrenceRate());
  }

  public void recordBackupVersionDatabaseOptimization() {
    this.optimizationSensor.record();
  }

  public void recordBackupVersionDatabaseOptimizationError() {
    this.optimizationErrorSensor.record();
  }
}
