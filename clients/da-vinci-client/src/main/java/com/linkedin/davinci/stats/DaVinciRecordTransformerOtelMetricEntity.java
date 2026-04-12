package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RECORD_TRANSFORMER_OPERATION;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import java.util.Set;


/**
 * OTel metric entity definitions for {@link DaVinciRecordTransformerStats}.
 * Tracks DaVinci record transformer latency and error counts by operation (put/delete).
 */
public enum DaVinciRecordTransformerOtelMetricEntity implements ModuleMetricEntityInterface {
  RECORD_TRANSFORMER_LATENCY(
      "record_transformer.latency", MetricType.HISTOGRAM, MetricUnit.MILLISECOND,
      "DaVinci record transformer operation latency",
      setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME, VENICE_RECORD_TRANSFORMER_OPERATION)
  ),

  RECORD_TRANSFORMER_ERROR_COUNT(
      "record_transformer.error_count", MetricType.COUNTER, MetricUnit.NUMBER,
      "DaVinci record transformer operation error count",
      setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME, VENICE_RECORD_TRANSFORMER_OPERATION)
  );

  private final MetricEntity metricEntity;

  DaVinciRecordTransformerOtelMetricEntity(
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
