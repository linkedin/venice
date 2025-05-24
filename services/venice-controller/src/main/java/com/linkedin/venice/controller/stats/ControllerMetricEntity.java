package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.REPUSH_STORE_TRIGGER_SOURCE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_VERSION;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import java.util.Set;


public enum ControllerMetricEntity {
  REPUSH_STORE_ENDPOINT_CALL_COUNT(
      MetricType.COUNTER, MetricUnit.NUMBER, "Count of all calls to a controller endpoint",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, REPUSH_STORE_TRIGGER_SOURCE, VENICE_RESPONSE_STATUS_CODE_CATEGORY)
  ),
  STORE_NOMINATED_FOR_SCHEDULED_COMPACTION(
      MetricType.GAUGE, MetricUnit.NUMBER,
      "When a store is nominated for scheduled compaction and remains uncompacted, this metric will be at 1."
          + " When the store is compacted, this metric will return to 0.",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_STORE_VERSION)
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
