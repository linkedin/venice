package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CHUNKING_STATUS;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_COMPUTE_OPERATION_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_METHOD;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import java.util.Set;


/**
 * OTel metric entity definitions for server-side read request metrics.
 * These 16 metrics consolidate ~30 Tehuti sensors using dimensions
 * (e.g., success/error latency becomes a single metric with HTTP status dimensions).
 */
public enum ServerReadOtelMetricEntity implements ModuleMetricEntityInterface {
  READ_CALL_TIME(
      "read.call_time", MetricType.HISTOGRAM, MetricUnit.MILLISECOND, "Server-side read request latency",
      setOf(
          VENICE_STORE_NAME,
          VENICE_CLUSTER_NAME,
          VENICE_REQUEST_METHOD,
          HTTP_RESPONSE_STATUS_CODE,
          HTTP_RESPONSE_STATUS_CODE_CATEGORY,
          VENICE_RESPONSE_STATUS_CODE_CATEGORY)
  ),

  READ_CALL_COUNT(
      "read.call_count", MetricType.COUNTER, MetricUnit.NUMBER, "Server-side read request count",
      setOf(
          VENICE_STORE_NAME,
          VENICE_CLUSTER_NAME,
          VENICE_REQUEST_METHOD,
          HTTP_RESPONSE_STATUS_CODE,
          HTTP_RESPONSE_STATUS_CODE_CATEGORY,
          VENICE_RESPONSE_STATUS_CODE_CATEGORY)
  ),

  READ_REQUEST_KEY_COUNT(
      "read.request.key_count", MetricType.HISTOGRAM, MetricUnit.NUMBER, "Number of keys in a read request",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_REQUEST_METHOD)
  ),

  READ_REQUEST_SIZE(
      "read.request.size", MetricType.HISTOGRAM, MetricUnit.BYTES, "Size of the read request payload",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_REQUEST_METHOD)
  ),

  READ_RESPONSE_SIZE(
      "read.response.size", MetricType.HISTOGRAM, MetricUnit.BYTES, "Size of the read response payload",
      setOf(
          VENICE_STORE_NAME,
          VENICE_CLUSTER_NAME,
          VENICE_REQUEST_METHOD,
          HTTP_RESPONSE_STATUS_CODE,
          HTTP_RESPONSE_STATUS_CODE_CATEGORY,
          VENICE_RESPONSE_STATUS_CODE_CATEGORY)
  ),

  READ_REQUEST_KEY_SIZE(
      "read.request.key_size", MetricType.HISTOGRAM, MetricUnit.BYTES, "Size of keys in a read request",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_REQUEST_METHOD)
  ),

  READ_RESPONSE_VALUE_SIZE(
      "read.response.value_size", MetricType.HISTOGRAM, MetricUnit.BYTES, "Size of values in a read response",
      setOf(
          VENICE_STORE_NAME,
          VENICE_CLUSTER_NAME,
          VENICE_REQUEST_METHOD,
          HTTP_RESPONSE_STATUS_CODE,
          HTTP_RESPONSE_STATUS_CODE_CATEGORY,
          VENICE_RESPONSE_STATUS_CODE_CATEGORY)
  ),

  STORAGE_ENGINE_QUERY_TIME(
      "storage_engine.query.time", MetricType.HISTOGRAM, MetricUnit.MILLISECOND, "Storage engine query latency",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_REQUEST_METHOD, VENICE_CHUNKING_STATUS)
  ),

  STORAGE_ENGINE_QUERY_CHUNKED_VALUE_COUNT(
      "storage_engine.query.chunked_value_count", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.NUMBER,
      "Per-request count of values requiring multi-chunk large value reassembly in the storage engine",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_REQUEST_METHOD)
  ),

  STORAGE_ENGINE_QUEUE_WAIT_TIME(
      "storage_engine.queue.wait_time", MetricType.HISTOGRAM, MetricUnit.MILLISECOND,
      "Time spent waiting in the storage execution handler queue",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_REQUEST_METHOD)
  ),

  STORAGE_ENGINE_QUEUE_SIZE(
      "storage_engine.queue.size", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.NUMBER,
      "Number of pending tasks in the storage execution handler queue",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_REQUEST_METHOD)
  ),

  STORAGE_ENGINE_QUERY_DESERIALIZATION_TIME(
      "storage_engine.query.deserialization_time", MetricType.HISTOGRAM, MetricUnit.MILLISECOND,
      "Time spent deserializing values for read-compute operations",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_REQUEST_METHOD)
  ),

  STORAGE_ENGINE_QUERY_SERIALIZATION_TIME(
      "storage_engine.query.serialization_time", MetricType.HISTOGRAM, MetricUnit.MILLISECOND,
      "Time spent serializing results for read-compute operations",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_REQUEST_METHOD)
  ),

  STORAGE_ENGINE_COMPUTE_OPERATION_COUNT(
      "storage_engine.read.compute.operation_count", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.NUMBER,
      "Count of read-compute operations by operation type",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_COMPUTE_OPERATION_TYPE)
  ),

  READ_RESPONSE_FLUSH_TIME(
      "read.response.flush_time", MetricType.HISTOGRAM, MetricUnit.MILLISECOND,
      "Time spent flushing the read response to the network",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_REQUEST_METHOD)
  ),

  READ_RESPONSE_KEY_NOT_FOUND_COUNT(
      "read.response.key.not_found_count", MetricType.COUNTER, MetricUnit.NUMBER,
      "Count of keys not found during read operations",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_REQUEST_METHOD)
  );

  private final MetricEntity metricEntity;

  ServerReadOtelMetricEntity(
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
