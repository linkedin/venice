package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.STORE_REPUSH_TRIGGER_SOURCE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.annotation.VisibleForTesting;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import java.util.Set;


public enum ControllerMetricEntity implements ModuleMetricEntityInterface {
  STORE_REPUSH_CALL_COUNT(
      "store.repush.call_count", MetricType.COUNTER, MetricUnit.NUMBER, "Count of all requests to repush a store",
      setOf(VENICE_STORE_NAME, VENICE_RESPONSE_STATUS_CODE_CATEGORY, VENICE_CLUSTER_NAME, STORE_REPUSH_TRIGGER_SOURCE)
  ),

  /** log compaction related metrics */
  STORE_COMPACTION_NOMINATED_COUNT(
      "store.compaction.nominated_count", MetricType.COUNTER, MetricUnit.NUMBER,
      "Count of stores nominated for scheduled compaction", setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME)
  ),
  STORE_COMPACTION_ELIGIBLE_STATE(
      "store.compaction.eligible_state", MetricType.GAUGE, MetricUnit.NUMBER,
      "State of the compaction eligibility: shows the duration from compaction nomination to triggered to completion. stays 1 after nomination and becomes 0 when the compaction is compacted",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME)
  ),
  STORE_COMPACTION_TRIGGERED_COUNT(
      "store.compaction.triggered_count", MetricType.COUNTER, MetricUnit.NUMBER,
      "Count of log compaction repush triggered for a store after it becomes eligible",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME)
  );

  private final MetricEntity metricEntity;
  private final String metricName;

  ControllerMetricEntity(
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

  public MetricEntity getMetricEntity() {
    return metricEntity;
  }
}
