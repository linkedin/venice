package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import java.util.Set;


/**
 * OTel metric entity definitions for {@link com.linkedin.venice.stats.DiskHealthStats}.
 */
public enum DiskHealthOtelMetricEntity implements ModuleMetricEntityInterface {
  DISK_HEALTH_STATUS(
      "disk.health.status", MetricType.ASYNC_GAUGE, MetricUnit.NUMBER,
      "Disk health status: 1 if healthy, 0 if unhealthy", setOf(VENICE_CLUSTER_NAME)
  );

  private final MetricEntity metricEntity;

  DiskHealthOtelMetricEntity(
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
