package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_ADAPTIVE_THROTTLER_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import java.util.Set;


/** OTel metric entities for adaptive throttling service tracking. */
public enum AdaptiveThrottlingOtelMetricEntity implements ModuleMetricEntityInterface {
  RECORD_COUNT(
      "adaptive_throttler.record_count", MetricType.COUNTER, MetricUnit.NUMBER,
      "Count of records observed by each adaptive record-count ingestion throttler",
      setOf(VENICE_CLUSTER_NAME, VENICE_ADAPTIVE_THROTTLER_TYPE)
  ),
  BYTE_COUNT(
      "adaptive_throttler.byte_count", MetricType.COUNTER, MetricUnit.BYTES,
      "Count of bytes observed by the adaptive bandwidth ingestion throttler",
      setOf(VENICE_CLUSTER_NAME, VENICE_ADAPTIVE_THROTTLER_TYPE)
  );

  private final MetricEntity metricEntity;

  AdaptiveThrottlingOtelMetricEntity(
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
