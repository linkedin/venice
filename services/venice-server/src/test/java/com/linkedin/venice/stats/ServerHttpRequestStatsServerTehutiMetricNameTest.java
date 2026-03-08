package com.linkedin.venice.stats;

import com.linkedin.venice.stats.metrics.TehutiMetricNameEnumTestFixture;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class ServerHttpRequestStatsServerTehutiMetricNameTest {
  private static Map<ServerHttpRequestStats.ServerTehutiMetricName, String> expectedMetricNames() {
    Map<ServerHttpRequestStats.ServerTehutiMetricName, String> map = new HashMap<>();
    map.put(ServerHttpRequestStats.ServerTehutiMetricName.SUCCESS_REQUEST, "success_request");
    map.put(ServerHttpRequestStats.ServerTehutiMetricName.ERROR_REQUEST, "error_request");
    map.put(ServerHttpRequestStats.ServerTehutiMetricName.SUCCESS_REQUEST_LATENCY, "success_request_latency");
    map.put(ServerHttpRequestStats.ServerTehutiMetricName.ERROR_REQUEST_LATENCY, "error_request_latency");
    map.put(ServerHttpRequestStats.ServerTehutiMetricName.STORAGE_ENGINE_QUERY_LATENCY, "storage_engine_query_latency");
    map.put(
        ServerHttpRequestStats.ServerTehutiMetricName.STORAGE_ENGINE_READ_COMPUTE_LATENCY,
        "storage_engine_read_compute_latency");
    map.put(
        ServerHttpRequestStats.ServerTehutiMetricName.STORAGE_ENGINE_LARGE_VALUE_LOOKUP,
        "storage_engine_large_value_lookup");
    map.put(ServerHttpRequestStats.ServerTehutiMetricName.REQUEST_KEY_COUNT, "request_key_count");
    map.put(ServerHttpRequestStats.ServerTehutiMetricName.REQUEST_SIZE_IN_BYTES, "request_size_in_bytes");
    map.put(
        ServerHttpRequestStats.ServerTehutiMetricName.STORAGE_EXECUTION_HANDLER_SUBMISSION_WAIT_TIME,
        "storage_execution_handler_submission_wait_time");
    map.put(ServerHttpRequestStats.ServerTehutiMetricName.STORAGE_EXECUTION_QUEUE_LEN, "storage_execution_queue_len");
    map.put(
        ServerHttpRequestStats.ServerTehutiMetricName.STORAGE_ENGINE_READ_COMPUTE_DESERIALIZATION_LATENCY,
        "storage_engine_read_compute_deserialization_latency");
    map.put(
        ServerHttpRequestStats.ServerTehutiMetricName.STORAGE_ENGINE_READ_COMPUTE_SERIALIZATION_LATENCY,
        "storage_engine_read_compute_serialization_latency");
    map.put(ServerHttpRequestStats.ServerTehutiMetricName.DOT_PRODUCT_COUNT, "dot_product_count");
    map.put(ServerHttpRequestStats.ServerTehutiMetricName.COSINE_SIMILARITY_COUNT, "cosine_similarity_count");
    map.put(ServerHttpRequestStats.ServerTehutiMetricName.HADAMARD_PRODUCT_COUNT, "hadamard_product_count");
    map.put(ServerHttpRequestStats.ServerTehutiMetricName.COUNT_OPERATOR_COUNT, "count_operator_count");
    map.put(ServerHttpRequestStats.ServerTehutiMetricName.KEY_NOT_FOUND, "key_not_found");
    map.put(ServerHttpRequestStats.ServerTehutiMetricName.REQUEST_KEY_SIZE, "request_key_size");
    map.put(ServerHttpRequestStats.ServerTehutiMetricName.REQUEST_VALUE_SIZE, "request_value_size");
    map.put(ServerHttpRequestStats.ServerTehutiMetricName.FLUSH_LATENCY, "flush_latency");
    map.put(ServerHttpRequestStats.ServerTehutiMetricName.RESPONSE_SIZE, "response_size");
    return map;
  }

  @Test
  public void testTehutiMetricNames() {
    new TehutiMetricNameEnumTestFixture<>(ServerHttpRequestStats.ServerTehutiMetricName.class, expectedMetricNames())
        .assertAll();
  }
}
