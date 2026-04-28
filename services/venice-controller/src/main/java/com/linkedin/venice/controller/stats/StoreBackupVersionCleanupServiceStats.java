package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricEntityStateBase;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import io.opentelemetry.api.common.Attributes;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.OccurrenceRate;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;


public class StoreBackupVersionCleanupServiceStats extends AbstractVeniceStats {
  private final MetricEntityStateBase versionMismatchMetric;
  private final MetricEntityStateBase rolledBackVersionDeletedMetric;
  private final MetricEntityStateBase rolledBackVersionDeleteErrorMetric;

  public StoreBackupVersionCleanupServiceStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);

    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository).setClusterName(name).build();
    VeniceOpenTelemetryMetricsRepository otelRepository = otelData.getOtelRepository();
    Map<VeniceMetricsDimensions, String> baseDimensionsMap = otelData.getBaseDimensionsMap();
    Attributes baseAttributes = otelData.getBaseAttributes();

    versionMismatchMetric = MetricEntityStateBase.create(
        BackupVersionCleanupOtelMetricEntity.BACKUP_VERSION_CLEANUP_MISMATCH_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        BackupVersionCleanupTehutiMetricNameEnum.BACKUP_VERSION_CLEANUP_VERSION_MISMATCH,
        Arrays.asList(new OccurrenceRate()),
        baseDimensionsMap,
        baseAttributes);

    rolledBackVersionDeletedMetric = MetricEntityStateBase.create(
        BackupVersionCleanupOtelMetricEntity.ROLLED_BACK_VERSION_DELETED_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        BackupVersionCleanupTehutiMetricNameEnum.ROLLED_BACK_VERSION_DELETED,
        Arrays.asList(new OccurrenceRate()),
        baseDimensionsMap,
        baseAttributes);

    rolledBackVersionDeleteErrorMetric = MetricEntityStateBase.create(
        BackupVersionCleanupOtelMetricEntity.ROLLED_BACK_VERSION_DELETE_ERROR_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        BackupVersionCleanupTehutiMetricNameEnum.ROLLED_BACK_VERSION_DELETE_ERROR,
        Arrays.asList(new OccurrenceRate()),
        baseDimensionsMap,
        baseAttributes);
  }

  public void recordBackupVersionMismatch() {
    versionMismatchMetric.record(1);
  }

  public void recordRolledBackVersionDeleted() {
    rolledBackVersionDeletedMetric.record(1);
  }

  public void recordRolledBackVersionDeleteError() {
    rolledBackVersionDeleteErrorMetric.record(1);
  }

  enum BackupVersionCleanupTehutiMetricNameEnum implements TehutiMetricNameEnum {
    BACKUP_VERSION_CLEANUP_VERSION_MISMATCH, ROLLED_BACK_VERSION_DELETED, ROLLED_BACK_VERSION_DELETE_ERROR
  }

  public enum BackupVersionCleanupOtelMetricEntity implements ModuleMetricEntityInterface {
    /** Count of backup version cleanup version mismatches */
    BACKUP_VERSION_CLEANUP_MISMATCH_COUNT(
        "backup_version_cleanup_service.version_mismatch_count", MetricType.COUNTER, MetricUnit.NUMBER,
        "Count of backup version cleanup version mismatches", setOf(VENICE_CLUSTER_NAME)
    ),
    ROLLED_BACK_VERSION_DELETED_COUNT(
        "backup_version_cleanup_service.rolled_back_version_deleted_count", MetricType.COUNTER, MetricUnit.NUMBER,
        "Count of rolled-back versions deleted after retention expiry", setOf(VENICE_CLUSTER_NAME)
    ),
    ROLLED_BACK_VERSION_DELETE_ERROR_COUNT(
        "backup_version_cleanup_service.rolled_back_version_delete_error_count", MetricType.COUNTER, MetricUnit.NUMBER,
        "Count of errors when deleting rolled-back versions", setOf(VENICE_CLUSTER_NAME)
    );

    private final MetricEntity metricEntity;

    BackupVersionCleanupOtelMetricEntity(
        String metricName,
        MetricType metricType,
        MetricUnit unit,
        String description,
        Set<VeniceMetricsDimensions> dimensionsList) {
      this.metricEntity = new MetricEntity(metricName, metricType, unit, description, dimensionsList);
    }

    @Override
    public MetricEntity getMetricEntity() {
      return metricEntity;
    }
  }
}
