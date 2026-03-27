package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_BUFFER_SERVICE_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import java.util.Set;


/** OTel metric entities for store buffer service (drainer queue) tracking. */
public enum StoreBufferServiceOtelMetricEntity implements ModuleMetricEntityInterface {
  MEMORY_USED(
      "drainer.memory.used", MetricType.ASYNC_GAUGE, MetricUnit.BYTES, "Total memory used across all drainer queues",
      setOf(VENICE_CLUSTER_NAME, VENICE_STORE_BUFFER_SERVICE_TYPE)
  ),
  MEMORY_REMAINING(
      "drainer.memory.remaining", MetricType.ASYNC_GAUGE, MetricUnit.BYTES,
      "Total remaining memory capacity across all drainer queues",
      setOf(VENICE_CLUSTER_NAME, VENICE_STORE_BUFFER_SERVICE_TYPE)
  ),
  MEMORY_USED_PER_WRITER_MAX(
      "drainer.writer.memory.max_used", MetricType.ASYNC_GAUGE, MetricUnit.BYTES,
      "Maximum memory used by any single drainer writer", setOf(VENICE_CLUSTER_NAME, VENICE_STORE_BUFFER_SERVICE_TYPE)
  ),
  MEMORY_USED_PER_WRITER_MIN(
      "drainer.writer.memory.min_used", MetricType.ASYNC_GAUGE, MetricUnit.BYTES,
      "Minimum memory used by any single drainer writer", setOf(VENICE_CLUSTER_NAME, VENICE_STORE_BUFFER_SERVICE_TYPE)
  ),
  PROCESSING_TIME(
      "drainer.record.processing.time", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.MILLISECOND,
      "Time spent processing each record in the drainer",
      setOf(VENICE_CLUSTER_NAME, VENICE_STORE_BUFFER_SERVICE_TYPE, VENICE_STORE_NAME)
  ),
  PROCESSING_ERROR_COUNT(
      "drainer.record.processing.error_count", MetricType.COUNTER, MetricUnit.NUMBER,
      "Count of errors encountered while processing records in the drainer",
      setOf(VENICE_CLUSTER_NAME, VENICE_STORE_BUFFER_SERVICE_TYPE, VENICE_STORE_NAME)
  );

  private final MetricEntity metricEntity;

  StoreBufferServiceOtelMetricEntity(
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
