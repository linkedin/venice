package com.linkedin.venice.router.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_METHOD;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_RETRY_ABORT_REASON;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_RETRY_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_VALIDATION_OUTCOME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import java.util.Set;


/**
 * List all Metric entities for router
 */
public enum RouterMetricEntity {
  INCOMING_CALL_COUNT(
      MetricType.COUNTER, MetricUnit.NUMBER, "Count of all incoming requests",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_REQUEST_METHOD)
  ),
  CALL_COUNT(
      MetricType.COUNTER, MetricUnit.NUMBER, "Count of all requests with response details",
      setOf(
          VENICE_STORE_NAME,
          VENICE_CLUSTER_NAME,
          VENICE_REQUEST_METHOD,
          HTTP_RESPONSE_STATUS_CODE,
          HTTP_RESPONSE_STATUS_CODE_CATEGORY,
          VENICE_RESPONSE_STATUS_CODE_CATEGORY)
  ),
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
  INCOMING_KEY_COUNT(
      MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.NUMBER, "Count of keys in all requests",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_REQUEST_METHOD, VENICE_REQUEST_VALIDATION_OUTCOME)
  ),
  KEY_COUNT(
      MetricType.HISTOGRAM, MetricUnit.NUMBER, "Count of keys in all responses",
      setOf(
          VENICE_STORE_NAME,
          VENICE_CLUSTER_NAME,
          VENICE_REQUEST_METHOD,
          HTTP_RESPONSE_STATUS_CODE,
          HTTP_RESPONSE_STATUS_CODE_CATEGORY,
          VENICE_RESPONSE_STATUS_CODE_CATEGORY)
  ),
  RETRY_COUNT(
      MetricType.COUNTER, MetricUnit.NUMBER, "Count of retries triggered",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_REQUEST_METHOD, VENICE_REQUEST_RETRY_TYPE)
  ),
  ALLOWED_RETRY_COUNT(
      MetricType.COUNTER, MetricUnit.NUMBER, "Count of allowed retry requests",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_REQUEST_METHOD)
  ),
  DISALLOWED_RETRY_COUNT(
      MetricType.COUNTER, MetricUnit.NUMBER, "Count of disallowed retry requests",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_REQUEST_METHOD)
  ),
  RETRY_DELAY(
      MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.MILLISECOND, "Retry delay time",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_REQUEST_METHOD)
  ),
  ABORTED_RETRY_COUNT(
      MetricType.COUNTER, MetricUnit.NUMBER, "Count of aborted retry requests",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_REQUEST_METHOD, VENICE_REQUEST_RETRY_ABORT_REASON)
  );

  private final MetricEntity metricEntity;

  RouterMetricEntity(
      MetricType metricType,
      MetricUnit unit,
      String description,
      Set<VeniceMetricsDimensions> dimensionsList) {
    this.metricEntity = new MetricEntity(this.name().toLowerCase(), metricType, unit, description, dimensionsList);
  }

  public MetricEntity getMetricEntity() {
    return metricEntity;
  }
}
