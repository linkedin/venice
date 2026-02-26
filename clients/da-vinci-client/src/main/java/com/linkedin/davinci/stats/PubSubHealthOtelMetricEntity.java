package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_PUBSUB_HEALTH_CATEGORY;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import java.util.Set;


public enum PubSubHealthOtelMetricEntity implements ModuleMetricEntityInterface {
  PUBSUB_HEALTH_UNHEALTHY_COUNT(
      "pubsub.health.unhealthy_count", MetricType.ASYNC_GAUGE, MetricUnit.NUMBER,
      "Count of unhealthy PubSub targets by category", setOf(VENICE_CLUSTER_NAME, VENICE_PUBSUB_HEALTH_CATEGORY)
  ),

  PUBSUB_HEALTH_PROBE_SUCCESS_COUNT(
      "pubsub.health.probe.success_count", MetricType.COUNTER, MetricUnit.NUMBER,
      "Count of successful recovery probe attempts", setOf(VENICE_CLUSTER_NAME, VENICE_PUBSUB_HEALTH_CATEGORY)
  ),

  PUBSUB_HEALTH_PROBE_FAILURE_COUNT(
      "pubsub.health.probe.failure_count", MetricType.COUNTER, MetricUnit.NUMBER,
      "Count of failed recovery probe attempts", setOf(VENICE_CLUSTER_NAME, VENICE_PUBSUB_HEALTH_CATEGORY)
  ),

  PUBSUB_HEALTH_STATE_TRANSITION_COUNT(
      "pubsub.health.state_transition_count", MetricType.COUNTER, MetricUnit.NUMBER,
      "Count of health state transitions", setOf(VENICE_CLUSTER_NAME, VENICE_PUBSUB_HEALTH_CATEGORY)
  );

  private final MetricEntity metricEntity;

  PubSubHealthOtelMetricEntity(
      String name,
      MetricType metricType,
      MetricUnit unit,
      String description,
      Set<VeniceMetricsDimensions> dimensionsList) {
    this.metricEntity = new MetricEntity(name, metricType, unit, description, dimensionsList);
  }

  @Override
  public MetricEntity getMetricEntity() {
    return metricEntity;
  }
}
