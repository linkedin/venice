package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_SERVER_LOAD_REQUEST_OUTCOME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import java.util.Set;


/**
 * OTel metric entity definitions for {@link com.linkedin.venice.stats.ServerLoadStats}.
 * Tracks server load control: request outcomes and rejection ratio.
 */
public enum ServerLoadOtelMetricEntity implements ModuleMetricEntityInterface {
  REQUEST_COUNT(
      "load_controller.request.count", MetricType.COUNTER, MetricUnit.NUMBER,
      "Count of requests by load control outcome (accepted or rejected)",
      setOf(VENICE_CLUSTER_NAME, VENICE_SERVER_LOAD_REQUEST_OUTCOME)
  ),

  REJECTION_RATIO(
      "load_controller.request.rejection_ratio", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.RATIO,
      "Server load rejection ratio (fraction of requests being rejected)", setOf(VENICE_CLUSTER_NAME)
  );

  private final MetricEntity metricEntity;

  ServerLoadOtelMetricEntity(
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
