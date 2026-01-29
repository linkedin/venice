package com.linkedin.venice.client.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_METHOD;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_REJECTION_REASON;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_RETRY_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_ROUTE_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STREAM_PROGRESS;
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
  RETRY_CALL_COUNT(
      "retry.call_count", MetricType.COUNTER, MetricUnit.NUMBER, "Count of all retry requests for client",
      setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD, VENICE_REQUEST_RETRY_TYPE)
  ),

  /**
   * Client retry key count.
   */
  RETRY_REQUEST_KEY_COUNT(
      "retry.request.key_count", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.NUMBER,
      "Key count of retry requests for client", setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD)
  ),

  /**
   * Client retry response key count.
   */
  RETRY_RESPONSE_KEY_COUNT(
      "retry.response.key_count", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.NUMBER,
      "Key count of retry responses for client", setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD)
  ),

  /**
   * Request serialization time in milliseconds.
   */
  REQUEST_SERIALIZATION_TIME(
      "request.serialization_time", MetricType.HISTOGRAM, MetricUnit.MILLISECOND,
      "Time to serialize the request payload in milliseconds", setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD)
  ),

  /**
   * Time between client submitting a request and beginning to handle the response.
   */
  CALL_SUBMISSION_TO_HANDLING_TIME(
      MetricType.HISTOGRAM, MetricUnit.MILLISECOND,
      "Time between submitting the request and starting to handle the response, in milliseconds",
      setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD)
  ),

  /**
   * Client duplicate key count.
   */
  REQUEST_DUPLICATE_KEY_COUNT(
      "request.duplicate_key_count", MetricType.COUNTER, MetricUnit.NUMBER,
      "Duplicate key count of requests for client",
      setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD, VENICE_RESPONSE_STATUS_CODE_CATEGORY)
  ),

  /**
   * The timeout duration (in milliseconds) that was configured for client Future.
   */
  REQUEST_TIMEOUT_REQUESTED_DURATION(
      "request.timeout.requested_duration", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.MILLISECOND,
      "The timeout duration (in milliseconds) that was configured for client Future",
      setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD)
  ),

  /**
   * Count of all requests routed to different hosts.
   */
  ROUTE_CALL_COUNT(
      "route.call_count", MetricType.COUNTER, MetricUnit.NUMBER,
      "Count of all requests routed to different instances in a cluster",
      setOf(
          VENICE_STORE_NAME,
          VENICE_CLUSTER_NAME,
          VENICE_REQUEST_METHOD,
          VENICE_ROUTE_NAME,
          VENICE_RESPONSE_STATUS_CODE_CATEGORY,
          HTTP_RESPONSE_STATUS_CODE_CATEGORY,
          HTTP_RESPONSE_STATUS_CODE)
  ),

  /**
   * Time taken for requests routed to different hosts.
   */
  ROUTE_CALL_TIME(
      "route.call_time", MetricType.HISTOGRAM, MetricUnit.MILLISECOND,
      "Time taken for requests routed to different instances in a cluster",
      setOf(
          VENICE_STORE_NAME,
          VENICE_CLUSTER_NAME,
          VENICE_REQUEST_METHOD,
          VENICE_ROUTE_NAME,
          VENICE_RESPONSE_STATUS_CODE_CATEGORY,
          HTTP_RESPONSE_STATUS_CODE_CATEGORY,
          HTTP_RESPONSE_STATUS_CODE)
  ),

  /**
   * Pending request count for requests routed to different hosts.
   */
  ROUTE_REQUEST_PENDING_COUNT(
      "route.request.pending_count", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.NUMBER,
      "Pending request count for requests routed to different instances in a cluster",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_REQUEST_METHOD, VENICE_ROUTE_NAME)
  ),

  /**
   * Request rejection ratio for requests routed to different hosts.
   */
  ROUTE_REQUEST_REJECTION_RATIO(
      "route.request.rejection_ratio", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.NUMBER,
      "Request rejection ratio for requests routed to different instances in a cluster",
      setOf(
          VENICE_STORE_NAME,
          VENICE_CLUSTER_NAME,
          VENICE_ROUTE_NAME,
          VENICE_REQUEST_METHOD,
          VENICE_REQUEST_REJECTION_REASON)
  ),

  /**
   * Response decompression time in milliseconds.
   */
  RESPONSE_DECOMPRESSION_TIME(
      "response.decompression_time", MetricType.HISTOGRAM, MetricUnit.MILLISECOND,
      "Time to decompress the response payload in milliseconds", setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD)
  ),

  /**
   * Response deserialization time in milliseconds.
   */
  RESPONSE_DESERIALIZATION_TIME(
      "response.deserialization_time", MetricType.HISTOGRAM, MetricUnit.MILLISECOND,
      "Time to deserialize the response payload in milliseconds", setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD)
  ),

  /**
   * Batch streaming progress time in milliseconds: Elapsed time from when the client starts receiving the response
   * to when the first/P50th/P90th record arrives and is processed.
   */
  RESPONSE_BATCH_STREAM_PROGRESS_TIME(
      "response.batch_stream_progress_time", MetricType.HISTOGRAM, MetricUnit.MILLISECOND,
      "Batch streaming progress time in milliseconds",
      setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD, VENICE_STREAM_PROGRESS)
  ),

  /**
   * Ratio of keys that were successfully retrieved to the total number of keys requested before timeout.
   */
  REQUEST_TIMEOUT_PARTIAL_RESPONSE_RATIO(
      "request.timeout.partial_response_ratio", MetricType.HISTOGRAM, MetricUnit.NUMBER,
      "Ratio of keys that were successfully retrieved to the total number of keys requested before timeout",
      setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD)
  ),

  /**
   * Count of requests that timed out on the client side based on the future.get(timeout).
   */
  REQUEST_TIMEOUT_COUNT(
      "request.timeout.count", MetricType.COUNTER, MetricUnit.NUMBER,
      "Count of requests that timed out on the client side", setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD)
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
