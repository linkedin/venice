package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.REPUSH_STORE_TRIGGER_SOURCE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import java.util.Set;


public enum ControllerMetricEntity {
  REPUSH_STORE_ENDPOINT_CALL_COUNT(
      MetricType.COUNTER, MetricUnit.NUMBER, "Count of all calls to a controller endpoint",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, REPUSH_STORE_TRIGGER_SOURCE)
  ),
  STORE_NOMINATION_TO_COMPACTION_COMPLETE_DURATION(
      MetricType.GAUGE, MetricUnit.NUMBER,
      "This metric indicates the duration from when a store is first nominated for compaction until a repush DAG is triggered on the same store (regardless of repush DAG run success/failure)."
          + " When a store is nominated for scheduled compaction and remains uncompacted, this metric will be at 1."
          + " When the store is compacted, this metric will return to 0.",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME)
  ),
  STORE_NOMINATED_FOR_COMPACTION_COUNT(
      MetricType.COUNTER, MetricUnit.NUMBER, "Count of stores nominated for scheduled compaction",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME)
  ),
  STORE_COMPACTION_TRIGGERED_COUNT(
      MetricType.COUNTER, MetricUnit.NUMBER,
      "Count of stores for which a repush DAG is triggered for scheduled compaction",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME)
  ),;

  private final MetricEntity metricEntity;
  private final String metricName;

  ControllerMetricEntity(
      MetricType metricType,
      MetricUnit unit,
      String description,
      Set<VeniceMetricsDimensions> dimensionsList) {
    this.metricName = this.name().toLowerCase();
    this.metricEntity = new MetricEntity(metricName, metricType, unit, description, dimensionsList);
  }

  public String getMetricName() {
    return metricName;
  }

  public MetricEntity getMetricEntity() {
    return metricEntity;
  }
}
