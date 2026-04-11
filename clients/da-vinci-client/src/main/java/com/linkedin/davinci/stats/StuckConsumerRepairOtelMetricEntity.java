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
 * OTel metric entity definitions for {@link StuckConsumerRepairStats}.
 * Metrics track the stuck PubSub consumer detection and repair lifecycle.
 */
public enum StuckConsumerRepairOtelMetricEntity implements ModuleMetricEntityInterface {
  STUCK_CONSUMER_DETECTED_COUNT(
      "ingestion.pubsub.consumer.stuck.detected_count", MetricType.COUNTER, MetricUnit.NUMBER,
      "Count of scans that detected a stuck PubSub consumer", setOf(VENICE_CLUSTER_NAME)
  ),

  STUCK_CONSUMER_TASK_REPAIRED_COUNT(
      "ingestion.pubsub.consumer.stuck.task_repaired_count", MetricType.COUNTER, MetricUnit.NUMBER,
      "Count of ingestion tasks killed to unblock a stuck PubSub consumer", setOf(VENICE_CLUSTER_NAME)
  ),

  STUCK_CONSUMER_UNRESOLVED_COUNT(
      "ingestion.pubsub.consumer.stuck.unresolved_count", MetricType.COUNTER, MetricUnit.NUMBER,
      "Count of scans where a stuck PubSub consumer was found but no fixable task identified",
      setOf(VENICE_CLUSTER_NAME)
  );

  private final MetricEntity metricEntity;

  StuckConsumerRepairOtelMetricEntity(
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
