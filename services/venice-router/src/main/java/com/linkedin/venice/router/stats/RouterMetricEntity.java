package com.linkedin.venice.router.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_MESSAGE_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_METHOD;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_RETRY_ABORT_REASON;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_RETRY_TYPE;
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
 * List all Metric entities for router
 */
public enum RouterMetricEntity implements ModuleMetricEntityInterface {
  /**
   * Count of all requests during response handling along with response codes
   */
  CALL_COUNT(
      MetricType.COUNTER, MetricUnit.NUMBER, "Count of all requests during response handling along with response codes",
      setOf(
          VENICE_STORE_NAME,
          VENICE_CLUSTER_NAME,
          VENICE_REQUEST_METHOD,
          HTTP_RESPONSE_STATUS_CODE,
          HTTP_RESPONSE_STATUS_CODE_CATEGORY,
          VENICE_RESPONSE_STATUS_CODE_CATEGORY)
  ),
  /**
   * Latency based on all responses
   */
  CALL_TIME(
      MetricType.HISTOGRAM, MetricUnit.MILLISECOND, "Latency based on all responses",
      setOf(
          VENICE_STORE_NAME,
          VENICE_CLUSTER_NAME,
          VENICE_REQUEST_METHOD,
          HTTP_RESPONSE_STATUS_CODE,
          HTTP_RESPONSE_STATUS_CODE_CATEGORY,
          VENICE_RESPONSE_STATUS_CODE_CATEGORY)
  ),
  /**
   * Size of request and response in bytes
   */
  CALL_SIZE(
      MetricType.HISTOGRAM, MetricUnit.BYTES, "Size of request and response in bytes",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_REQUEST_METHOD, VENICE_MESSAGE_TYPE)
  ),
  /**
   * Key Size: Measures the size of keys in the request
   * - For single get, request = key size
   * - for multi gets, measure each key in the request
   */
  KEY_SIZE(
      MetricType.HISTOGRAM, MetricUnit.BYTES, "Size of keys in bytes",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_REQUEST_METHOD)
  ),
  /**
   * Count of keys during response handling along with response codes
   */
  KEY_COUNT(
      MetricType.HISTOGRAM, MetricUnit.NUMBER, "Count of keys during response handling along with response codes",
      setOf(
          VENICE_STORE_NAME,
          VENICE_CLUSTER_NAME,
          VENICE_REQUEST_METHOD,
          HTTP_RESPONSE_STATUS_CODE,
          HTTP_RESPONSE_STATUS_CODE_CATEGORY,
          VENICE_RESPONSE_STATUS_CODE_CATEGORY)
  ),
  /**
   * Count of retries triggered
   */
  RETRY_COUNT(
      MetricType.COUNTER, MetricUnit.NUMBER, "Count of retries triggered",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_REQUEST_METHOD, VENICE_REQUEST_RETRY_TYPE)
  ),
  /**
   * Count of allowed retry requests
   */
  ALLOWED_RETRY_COUNT(
      MetricType.COUNTER, MetricUnit.NUMBER, "Count of allowed retry requests",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_REQUEST_METHOD)
  ),
  /**
   * Count of disallowed retry requests
   */
  DISALLOWED_RETRY_COUNT(
      MetricType.COUNTER, MetricUnit.NUMBER, "Count of disallowed retry requests",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_REQUEST_METHOD)
  ),
  /**
   * Count of aborted retry requests
   */
  ABORTED_RETRY_COUNT(
      MetricType.COUNTER, MetricUnit.NUMBER, "Count of aborted retry requests",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_REQUEST_METHOD, VENICE_REQUEST_RETRY_ABORT_REASON)
  ),
  /**
   * Retry delay time: Time in milliseconds between the original request and the retry request
   */
  RETRY_DELAY(
      MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.MILLISECOND, "Retry delay time",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_REQUEST_METHOD)
  );

  private final MetricEntity metricEntity;

  RouterMetricEntity(
      MetricType metricType,
      MetricUnit unit,
      String description,
      Set<VeniceMetricsDimensions> dimensionsList) {
    this.metricEntity = new MetricEntity(this.name().toLowerCase(), metricType, unit, description, dimensionsList);
  }

  @Override
  public MetricEntity getMetricEntity() {
    return metricEntity;
  }
}
