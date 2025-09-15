package com.linkedin.venice.fastclient.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_INSTANCE_ERROR_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.CollectionUtils.setOf;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import java.util.Set;


public enum ClusterMetricEntity implements ModuleMetricEntityInterface {
  /**
   * Count of version update failures.
   */
  VERSION_UPDATE_FAILURE_COUNT(
      "version.update_failure_count", MetricType.COUNTER, MetricUnit.NUMBER,
      "Count of version update failures for the store", setOf(VENICE_STORE_NAME)
  ),

  /**
   * Count of instance errors (blocked, unhealthy, overloaded instances).
   */
  INSTANCE_ERROR_COUNT(
      "instance.error_count", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.NUMBER,
      "Count of instance errors for the store", setOf(VENICE_STORE_NAME, VENICE_INSTANCE_ERROR_TYPE)
  ),

  /**
   * Current version number that the client is at.
   */
  VERSION_CURRENT_NUMBER(
      "version.current_number", MetricType.ASYNC_GAUGE, MetricUnit.NUMBER,
      "Current version number that the client is at", setOf(VENICE_STORE_NAME)
  );

  private final MetricEntity entity;

  ClusterMetricEntity(
      String name,
      MetricType metricType,
      MetricUnit unit,
      String description,
      Set<VeniceMetricsDimensions> dimensions) {
    this.entity = new MetricEntity(name, metricType, unit, description, dimensions);
  }

  @Override
  public MetricEntity getMetricEntity() {
    return entity;
  }
}
