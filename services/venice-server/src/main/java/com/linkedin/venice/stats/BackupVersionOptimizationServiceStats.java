package com.linkedin.venice.stats;

import static com.linkedin.davinci.stats.BackupVersionOptimizationOtelMetricEntity.REOPEN_COUNT;

import com.linkedin.venice.cleaner.BackupVersionOptimizationService;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.dimensions.VeniceOperationOutcome;
import com.linkedin.venice.stats.metrics.MetricEntityStateOneEnum;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.OccurrenceRate;
import java.util.Collections;
import java.util.HashMap;
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

  /**
   * Per-store joint Tehuti+OTel metric state maps. Two maps because they bind to different Tehuti sensors
   * (success vs error) while sharing the same OTel instrument ({@code REOPEN_COUNT}) differentiated
   * by {@link VeniceOperationOutcome}. Tehuti sensor is registered once (first store) and shared by all
   * subsequent stores via {@code registerSensorIfAbsent}. Bounded by the number of stores on this host.
   * When OTel is disabled, {@code otelRepository} is null and OTel recording is a no-op.
   */
  private final Map<String, MetricEntityStateOneEnum<VeniceOperationOutcome>> successPerStore =
      new VeniceConcurrentHashMap<>();
  private final Map<String, MetricEntityStateOneEnum<VeniceOperationOutcome>> errorPerStore =
      new VeniceConcurrentHashMap<>();

  public BackupVersionOptimizationServiceStats(MetricsRepository metricsRepository, String name, String clusterName) {
    super(metricsRepository, name);

    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository).setClusterName(clusterName).build();
    this.otelRepository = otelData.getOtelRepository();
    this.baseDimensionsMap = otelData.getBaseDimensionsMap();
  }

  public void recordBackupVersionDatabaseOptimization(String storeName) {
    getOrCreateMetric(successPerStore, storeName, TehutiMetricName.BACKUP_VERSION_DATABASE_OPTIMIZATION)
        .record(1, VeniceOperationOutcome.SUCCESS);
  }

  public void recordBackupVersionDatabaseOptimizationError(String storeName) {
    getOrCreateMetric(errorPerStore, storeName, TehutiMetricName.BACKUP_VERSION_DATA_OPTIMIZATION_ERROR)
        .record(1, VeniceOperationOutcome.FAIL);
  }

  private MetricEntityStateOneEnum<VeniceOperationOutcome> getOrCreateMetric(
      Map<String, MetricEntityStateOneEnum<VeniceOperationOutcome>> perStoreMap,
      String storeName,
      TehutiMetricName tehutiName) {
    return perStoreMap.computeIfAbsent(storeName, k -> createPerStoreMetric(k, tehutiName));
  }

  private MetricEntityStateOneEnum<VeniceOperationOutcome> createPerStoreMetric(
      String storeName,
      TehutiMetricName tehutiName) {
    Map<VeniceMetricsDimensions, String> storeDims = new HashMap<>(baseDimensionsMap);
    storeDims.put(VeniceMetricsDimensions.VENICE_STORE_NAME, OpenTelemetryMetricsSetup.sanitizeStoreName(storeName));
    return MetricEntityStateOneEnum.create(
        REOPEN_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        tehutiName,
        Collections.singletonList(new OccurrenceRate()),
        storeDims,
        VeniceOperationOutcome.class);
  }
}
