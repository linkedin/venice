package com.linkedin.davinci.stats;

import com.linkedin.venice.stats.metrics.TehutiMetricNameEnumTestFixture;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class KafkaConsumerServiceTehutiMetricNameTest {
  @Test
  public void testTehutiMetricNames() {
    new TehutiMetricNameEnumTestFixture<>(KafkaConsumerServiceStats.TehutiMetricName.class, expectedMetricNames())
        .assertAll();
  }

  private static Map<KafkaConsumerServiceStats.TehutiMetricName, String> expectedMetricNames() {
    Map<KafkaConsumerServiceStats.TehutiMetricName, String> map = new HashMap<>();
    map.put(KafkaConsumerServiceStats.TehutiMetricName.BYTES_PER_POLL, "bytes_per_poll");
    map.put(KafkaConsumerServiceStats.TehutiMetricName.CONSUMER_POLL_RESULT_NUM, "consumer_poll_result_num");
    map.put(KafkaConsumerServiceStats.TehutiMetricName.CONSUMER_POLL_REQUEST, "consumer_poll_request");
    map.put(
        KafkaConsumerServiceStats.TehutiMetricName.CONSUMER_POLL_NON_ZERO_RESULT_NUM,
        "consumer_poll_non_zero_result_num");
    map.put(KafkaConsumerServiceStats.TehutiMetricName.CONSUMER_POLL_REQUEST_LATENCY, "consumer_poll_request_latency");
    map.put(KafkaConsumerServiceStats.TehutiMetricName.CONSUMER_POLL_ERROR, "consumer_poll_error");
    map.put(
        KafkaConsumerServiceStats.TehutiMetricName.CONSUMER_RECORDS_PRODUCING_TO_WRITE_BUFFER_LATENCY,
        "consumer_records_producing_to_write_buffer_latency");
    map.put(KafkaConsumerServiceStats.TehutiMetricName.DETECTED_DELETED_TOPIC_NUM, "detected_deleted_topic_num");
    map.put(
        KafkaConsumerServiceStats.TehutiMetricName.DETECTED_NO_RUNNING_INGESTION_TOPIC_PARTITION_NUM,
        "detected_no_running_ingestion_topic_partition_num");
    map.put(KafkaConsumerServiceStats.TehutiMetricName.DELEGATE_SUBSCRIBE_LATENCY, "delegate_subscribe_latency");
    map.put(
        KafkaConsumerServiceStats.TehutiMetricName.UPDATE_CURRENT_ASSIGNMENT_LATENCY,
        "update_current_assignment_latency");
    map.put(KafkaConsumerServiceStats.TehutiMetricName.IDLE_TIME, "idle_time");
    map.put(
        KafkaConsumerServiceStats.TehutiMetricName.MAX_ELAPSED_TIME_SINCE_LAST_SUCCESSFUL_POLL,
        "max_elapsed_time_since_last_successful_poll");
    return map;
  }
}
