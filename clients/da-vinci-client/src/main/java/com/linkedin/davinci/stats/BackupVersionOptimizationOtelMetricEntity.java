package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_OPERATION_OUTCOME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import java.util.Set;


/**
 * OTel metric entity definitions for
 * {@link com.linkedin.venice.stats.BackupVersionOptimizationServiceStats}.
 */
public enum BackupVersionOptimizationOtelMetricEntity implements ModuleMetricEntityInterface {
  REOPEN_COUNT(
      "version.backup.optimization.reopen_count", MetricType.COUNTER, MetricUnit.NUMBER,
      "Count of backup version storage partition reopens by outcome (success or fail)",
      setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME, VENICE_OPERATION_OUTCOME)
  );

  private final MetricEntity metricEntity;

  BackupVersionOptimizationOtelMetricEntity(
      String metricName,
      MetricType metricType,
      MetricUnit unit,
      String description,
      Set<VeniceMetricsDimensions> dimensions) {
    this.metricEntity = new MetricEntity(metricName, metricType, unit, description, dimensions);
  }

  @Override
  public MetricEntity getMetricEntity() {
    return metricEntity;
  }
}
