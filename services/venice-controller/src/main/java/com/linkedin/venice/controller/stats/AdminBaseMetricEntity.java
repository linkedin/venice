package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_PARENT_ADMIN_METHOD;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_PARENT_ADMIN_METHOD_STEP;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.annotation.VisibleForTesting;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import java.util.Set;


/**
 * This is a base metric entity for venice admin related metrics.
 */
public enum AdminBaseMetricEntity implements ModuleMetricEntityInterface {
  PARENT_ADMIN_CALL_TIME(
      "call.time", MetricType.HISTOGRAM, MetricUnit.MILLISECOND, "latency histogram of the calls to parent admin",
      setOf(VENICE_CLUSTER_NAME, VENICE_PARENT_ADMIN_METHOD, VENICE_PARENT_ADMIN_METHOD_STEP)
  );

  private final MetricEntity metricEntity;
  private final String metricName;

  AdminBaseMetricEntity(
      String metricName,
      MetricType metricType,
      MetricUnit unit,
      String description,
      Set<VeniceMetricsDimensions> dimensionsList) {
    this.metricName = metricName;
    this.metricEntity = new MetricEntity(metricName, metricType, unit, description, dimensionsList);
  }

  @VisibleForTesting
  public String getMetricName() {
    return metricName;
  }

  @Override
  public MetricEntity getMetricEntity() {
    return metricEntity;
  }
}
