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

import com.linkedin.venice.stats.MetricEntity;
import java.util.concurrent.TimeUnit;


/**
 * List all Metric entities for router
 */
public class RouterMetricEntities {
  public static final MetricEntity INCOMING_CALL_COUNT = new MetricEntity(
      "incoming_call_count",
      MetricEntity.MetricType.COUNTER,
      "Number",
      "Count of all incoming requests",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_REQUEST_METHOD));

  public static final MetricEntity CALL_COUNT = new MetricEntity(
      "call_count",
      MetricEntity.MetricType.COUNTER,
      "Number",
      "Count of all requests with response details",
      setOf(
          VENICE_STORE_NAME,
          VENICE_CLUSTER_NAME,
          VENICE_REQUEST_METHOD,
          HTTP_RESPONSE_STATUS_CODE,
          HTTP_RESPONSE_STATUS_CODE_CATEGORY,
          VENICE_RESPONSE_STATUS_CODE_CATEGORY));

  public static final MetricEntity CALL_TIME = new MetricEntity(
      "call_time",
      MetricEntity.MetricType.HISTOGRAM,
      TimeUnit.MILLISECONDS.name(),
      "Latency based on all responses",
      setOf(
          VENICE_STORE_NAME,
          VENICE_CLUSTER_NAME,
          VENICE_REQUEST_METHOD,
          HTTP_RESPONSE_STATUS_CODE_CATEGORY,
          VENICE_RESPONSE_STATUS_CODE_CATEGORY));

  public static final MetricEntity CALL_KEY_COUNT = new MetricEntity(
      "call_key_count",
      MetricEntity.MetricType.HISTOGRAM_WITHOUT_BUCKETS,
      "Number",
      "Count of keys in multi key requests",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_REQUEST_METHOD, VENICE_REQUEST_VALIDATION_OUTCOME));

  public static final MetricEntity RETRY_COUNT = new MetricEntity(
      "retry_count",
      MetricEntity.MetricType.COUNTER,
      "Number",
      "Count of retries triggered",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_REQUEST_METHOD, VENICE_REQUEST_RETRY_TYPE));

  public static final MetricEntity ALLOWED_RETRY_COUNT = new MetricEntity(
      "allowed_retry_count",
      MetricEntity.MetricType.COUNTER,
      "Number",
      "Count of allowed retry requests",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_REQUEST_METHOD));

  public static final MetricEntity DISALLOWED_RETRY_COUNT = new MetricEntity(
      "disallowed_retry_count",
      MetricEntity.MetricType.COUNTER,
      "Number",
      "Count of disallowed retry requests",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_REQUEST_METHOD));

  public static final MetricEntity RETRY_DELAY = new MetricEntity(
      "retry_delay",
      MetricEntity.MetricType.HISTOGRAM_WITHOUT_BUCKETS,
      TimeUnit.MILLISECONDS.name(),
      "Retry delay time",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_REQUEST_METHOD));

  public static final MetricEntity ABORTED_RETRY_COUNT = new MetricEntity(
      "aborted_retry_count",
      MetricEntity.MetricType.COUNTER,
      "Number",
      "Count of aborted retry requests",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_REQUEST_METHOD, VENICE_REQUEST_RETRY_ABORT_REASON));
}
