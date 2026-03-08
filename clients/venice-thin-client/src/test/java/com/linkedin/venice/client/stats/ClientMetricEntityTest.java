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

import com.linkedin.venice.stats.metrics.AbstractModuleMetricEntityTest;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import java.util.HashMap;
import java.util.Map;


public class ClientMetricEntityTest extends AbstractModuleMetricEntityTest<ClientMetricEntity> {
  public ClientMetricEntityTest() {
    super(ClientMetricEntity.class);
  }

  @Override
  protected Map<ClientMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<ClientMetricEntity, MetricEntityExpectation> map = new HashMap<>();
    map.put(
        ClientMetricEntity.RETRY_CALL_COUNT,
        new MetricEntityExpectation(
            "retry.call_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of all retry requests for client",
            setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD, VENICE_REQUEST_RETRY_TYPE)));
    map.put(
        ClientMetricEntity.RETRY_REQUEST_KEY_COUNT,
        new MetricEntityExpectation(
            "retry.request.key_count",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.NUMBER,
            "Key count of retry requests for client",
            setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD)));
    map.put(
        ClientMetricEntity.RETRY_RESPONSE_KEY_COUNT,
        new MetricEntityExpectation(
            "retry.response.key_count",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.NUMBER,
            "Key count of retry responses for client",
            setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD)));
    map.put(
        ClientMetricEntity.REQUEST_SERIALIZATION_TIME,
        new MetricEntityExpectation(
            "request.serialization_time",
            MetricType.HISTOGRAM,
            MetricUnit.MILLISECOND,
            "Time to serialize the request payload in milliseconds",
            setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD)));
    map.put(
        ClientMetricEntity.CALL_SUBMISSION_TO_HANDLING_TIME,
        new MetricEntityExpectation(
            "call_submission_to_handling_time",
            MetricType.HISTOGRAM,
            MetricUnit.MILLISECOND,
            "Time between submitting the request and starting to handle the response, in milliseconds",
            setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD)));
    map.put(
        ClientMetricEntity.REQUEST_DUPLICATE_KEY_COUNT,
        new MetricEntityExpectation(
            "request.duplicate_key_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Duplicate key count of requests for client",
            setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD, VENICE_RESPONSE_STATUS_CODE_CATEGORY)));
    map.put(
        ClientMetricEntity.REQUEST_TIMEOUT_REQUESTED_DURATION,
        new MetricEntityExpectation(
            "request.timeout.requested_duration",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.MILLISECOND,
            "The timeout duration (in milliseconds) that was configured for client Future",
            setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD)));
    map.put(
        ClientMetricEntity.ROUTE_CALL_COUNT,
        new MetricEntityExpectation(
            "route.call_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of all requests routed to different instances in a cluster",
            setOf(
                VENICE_STORE_NAME,
                VENICE_CLUSTER_NAME,
                VENICE_REQUEST_METHOD,
                VENICE_ROUTE_NAME,
                VENICE_RESPONSE_STATUS_CODE_CATEGORY,
                HTTP_RESPONSE_STATUS_CODE_CATEGORY,
                HTTP_RESPONSE_STATUS_CODE)));
    map.put(
        ClientMetricEntity.ROUTE_CALL_TIME,
        new MetricEntityExpectation(
            "route.call_time",
            MetricType.HISTOGRAM,
            MetricUnit.MILLISECOND,
            "Time taken for requests routed to different instances in a cluster",
            setOf(
                VENICE_STORE_NAME,
                VENICE_CLUSTER_NAME,
                VENICE_REQUEST_METHOD,
                VENICE_ROUTE_NAME,
                VENICE_RESPONSE_STATUS_CODE_CATEGORY,
                HTTP_RESPONSE_STATUS_CODE_CATEGORY,
                HTTP_RESPONSE_STATUS_CODE)));
    map.put(
        ClientMetricEntity.ROUTE_REQUEST_PENDING_COUNT,
        new MetricEntityExpectation(
            "route.request.pending_count",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.NUMBER,
            "Pending request count for requests routed to different instances in a cluster",
            setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_REQUEST_METHOD, VENICE_ROUTE_NAME)));
    map.put(
        ClientMetricEntity.ROUTE_REQUEST_REJECTION_RATIO,
        new MetricEntityExpectation(
            "route.request.rejection_ratio",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.NUMBER,
            "Request rejection ratio for requests routed to different instances in a cluster",
            setOf(
                VENICE_STORE_NAME,
                VENICE_CLUSTER_NAME,
                VENICE_ROUTE_NAME,
                VENICE_REQUEST_METHOD,
                VENICE_REQUEST_REJECTION_REASON)));
    map.put(
        ClientMetricEntity.RESPONSE_DECOMPRESSION_TIME,
        new MetricEntityExpectation(
            "response.decompression_time",
            MetricType.HISTOGRAM,
            MetricUnit.MILLISECOND,
            "Time to decompress the response payload in milliseconds",
            setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD)));
    map.put(
        ClientMetricEntity.RESPONSE_DESERIALIZATION_TIME,
        new MetricEntityExpectation(
            "response.deserialization_time",
            MetricType.HISTOGRAM,
            MetricUnit.MILLISECOND,
            "Time to deserialize the response payload in milliseconds",
            setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD)));
    map.put(
        ClientMetricEntity.RESPONSE_BATCH_STREAM_PROGRESS_TIME,
        new MetricEntityExpectation(
            "response.batch_stream_progress_time",
            MetricType.HISTOGRAM,
            MetricUnit.MILLISECOND,
            "Batch streaming progress time in milliseconds",
            setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD, VENICE_STREAM_PROGRESS)));
    map.put(
        ClientMetricEntity.REQUEST_TIMEOUT_PARTIAL_RESPONSE_RATIO,
        new MetricEntityExpectation(
            "request.timeout.partial_response_ratio",
            MetricType.HISTOGRAM,
            MetricUnit.NUMBER,
            "Ratio of keys that were successfully retrieved to the total number of keys requested before timeout",
            setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD)));
    map.put(
        ClientMetricEntity.REQUEST_TIMEOUT_COUNT,
        new MetricEntityExpectation(
            "request.timeout.count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of requests that timed out on the client side",
            setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD)));
    return map;
  }
}
