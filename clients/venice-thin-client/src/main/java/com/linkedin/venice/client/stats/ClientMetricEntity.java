package com.linkedin.venice.client.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_DELIVERY_PROGRESS;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_MESSAGE_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_METHOD;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_RETRY_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.CollectionUtils.setOf;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import java.util.Set;


public enum ClientMetricEntity implements ModuleMetricEntityInterface {
  /**
   * Client retry count.
   */
  RETRY_COUNT(
      MetricType.COUNTER, MetricUnit.NUMBER, "Count of all retry requests for client",
      setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD, VENICE_REQUEST_RETRY_TYPE)
  ),

  /**
   * Client retry key count.
   */
  RETRY_KEY_COUNT(
      MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.NUMBER, "Key count of retry requests for client",
      setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD, VENICE_MESSAGE_TYPE)
  ),

  /**
   * Request serialization time in milliseconds.
   */
  REQUEST_SERIALIZATION_TIME(
      "request.serialization_time", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.MILLISECOND,
      "Time to serialize the request payload in milliseconds", setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD)
  ),

  /**
   * Time between client submitting a request and beginning to handle the response.
   */
  CALL_SUBMISSION_TO_HANDLING_TIME(
      "call_submission_to_handling_time", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.MILLISECOND,
      "Time between submitting the request and starting to handle the response, in milliseconds",
      setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD)
  ),

  /**
   * Response decompression time in milliseconds.
   */
  RESPONSE_DECOMPRESSION_TIME(
      "response.decompression_time", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.MILLISECOND,
      "Time to decompress the response payload in milliseconds", setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD)
  ),

  /**
   * Response deserialization time in milliseconds.
   */
  RESPONSE_DESERIALIZATION_TIME(
      "response.deserialization_time", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.MILLISECOND,
      "Time to deserialize the response payload in milliseconds", setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD)
  ),

  /**
   * Batch streaming progress time in milliseconds, dimensioned by delivery progress.
   */
  RESPONSE_BATCH_STREAM_PROGRESS_TIME(
      "response.batch_stream_progress_time", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.MILLISECOND,
      "Batch streaming progress time in milliseconds",
      setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD, VENICE_DELIVERY_PROGRESS)
  );

  private final MetricEntity entity;

  ClientMetricEntity(
      MetricType metricType,
      MetricUnit unit,
      String description,
      Set<VeniceMetricsDimensions> dimensions) {
    this.entity = new MetricEntity(this.name().toLowerCase(), metricType, unit, description, dimensions);
  }

  ClientMetricEntity(
      String name,
      MetricType metricType,
      MetricUnit unit,
      String description,
      Set<VeniceMetricsDimensions> dimensions) {
    this.entity = new MetricEntity(name, metricType, unit, description, dimensions);
  }

  @Override
  public MetricEntity getMetricEntity() {
    return entity;
  }
}
