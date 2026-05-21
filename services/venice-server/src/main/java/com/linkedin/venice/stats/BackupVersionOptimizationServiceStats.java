package com.linkedin.venice.stats;

import static com.linkedin.davinci.stats.BackupVersionOptimizationOtelMetricEntity.REOPEN_COUNT;

import com.linkedin.venice.cleaner.BackupVersionOptimizationService;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.dimensions.VeniceOperationOutcome;
import com.linkedin.venice.stats.metrics.AbstractStatsCloseable;
import com.linkedin.venice.stats.metrics.AsyncMetricEntityState.TehutiSensorRegistrationFunction;
import com.linkedin.venice.stats.metrics.MetricEntityStateOneEnum;
import com.linkedin.venice.stats.metrics.MetricEntityStateUtils;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.OccurrenceRate;
import java.util.Collections;
import java.util.Map;


/**
 * {@code BackupVersionOptimizationServiceStats} records the statistics for the database optimization done by the
 * {@link BackupVersionOptimizationService} including both successes and failures.
 *
 * <p>OTel uses a single COUNTER with STORE_NAME + OPERATION_OUTCOME dimensions. Tehuti uses
 * 2 separate OccurrenceRate sensors (backward compatible, no per-store granularity in Tehuti).
 */
public class BackupVersionOptimizationServiceStats extends AbstractVeniceStats {
  /** Tehuti metric names for BackupVersionOptimizationServiceStats sensors. */
  enum TehutiMetricName implements TehutiMetricNameEnum {
    BACKUP_VERSION_DATABASE_OPTIMIZATION, BACKUP_VERSION_DATA_OPTIMIZATION_ERROR
  }

  private final VeniceOpenTelemetryMetricsRepository otelRepository;
  private final Map<VeniceMetricsDimensions, String> baseDimensionsMap;

  /** Per-store success/error wrappers; one OTel instrument, two Tehuti sensors. Bounded by host store count. */
  private final Map<String, PerStoreEntry> perStoreEntryMap = new VeniceConcurrentHashMap<>();

  private static final class PerStoreEntry extends AbstractStatsCloseable {
    final MetricEntityStateOneEnum<VeniceOperationOutcome> success;
    final MetricEntityStateOneEnum<VeniceOperationOutcome> error;

    PerStoreEntry(
        VeniceOpenTelemetryMetricsRepository otelRepository,
        Map<VeniceMetricsDimensions, String> dims,
        TehutiSensorRegistrationFunction registerTehutiSensorFn) {
      this.success = MetricEntityStateOneEnum.create(
          REOPEN_COUNT.getMetricEntity(),
          otelRepository,
          registerTehutiSensorFn,
          TehutiMetricName.BACKUP_VERSION_DATABASE_OPTIMIZATION,
          Collections.singletonList(new OccurrenceRate()),
          dims,
          VeniceOperationOutcome.class,
          statsCloseables);
      this.error = MetricEntityStateOneEnum.create(
          REOPEN_COUNT.getMetricEntity(),
          otelRepository,
          registerTehutiSensorFn,
          TehutiMetricName.BACKUP_VERSION_DATA_OPTIMIZATION_ERROR,
          Collections.singletonList(new OccurrenceRate()),
          dims,
          VeniceOperationOutcome.class,
          statsCloseables);
    }
  }

  public BackupVersionOptimizationServiceStats(MetricsRepository metricsRepository, String name, String clusterName) {
    super(metricsRepository, name);

    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository).setClusterName(clusterName).build();
    this.otelRepository = otelData.getOtelRepository();
    this.baseDimensionsMap = otelData.getBaseDimensionsMap();
  }

  public void recordBackupVersionDatabaseOptimization(String storeName) {
    getOrCreateEntry(storeName).success.record(1, VeniceOperationOutcome.SUCCESS);
  }

  public void recordBackupVersionDatabaseOptimizationError(String storeName) {
    getOrCreateEntry(storeName).error.record(1, VeniceOperationOutcome.FAIL);
  }

  private PerStoreEntry getOrCreateEntry(String storeName) {
    return perStoreEntryMap.computeIfAbsent(
        storeName,
        k -> new PerStoreEntry(
            otelRepository,
            OpenTelemetryMetricsSetup.buildStoreDimensionsMap(baseDimensionsMap, k),
            this::registerSensorIfAbsent));
  }

  @Override
  public void close() {
    MetricEntityStateUtils.closeAndClear(perStoreEntryMap);
    super.close();
  }
}
