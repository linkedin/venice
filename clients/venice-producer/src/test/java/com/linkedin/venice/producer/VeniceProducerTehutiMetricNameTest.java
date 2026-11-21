package com.linkedin.venice.producer;

import com.linkedin.venice.producer.VeniceProducerMetrics.VeniceProducerTehutiMetricName;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnumTestFixture;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


/**
 * The Tehuti metric names must stay the same as before the OpenTelemetry migration to not break existing dashboards
 * and alerts.
 */
public class VeniceProducerTehutiMetricNameTest {
  @Test
  public void testTehutiMetricNames() {
    new TehutiMetricNameEnumTestFixture<>(VeniceProducerTehutiMetricName.class, expectedMetricNames()).assertAll();
  }

  private static Map<VeniceProducerTehutiMetricName, String> expectedMetricNames() {
    Map<VeniceProducerTehutiMetricName, String> map = new HashMap<>();
    map.put(VeniceProducerTehutiMetricName.WRITE_OPERATION, "write_operation");
    map.put(VeniceProducerTehutiMetricName.PUT_OPERATION, "put_operation");
    map.put(VeniceProducerTehutiMetricName.DELETE_OPERATION, "delete_operation");
    map.put(VeniceProducerTehutiMetricName.UPDATE_OPERATION, "update_operation");
    map.put(VeniceProducerTehutiMetricName.SUCCESS_WRITE_OPERATION, "success_write_operation");
    map.put(VeniceProducerTehutiMetricName.FAILED_WRITE_OPERATION, "failed_write_operation");
    map.put(VeniceProducerTehutiMetricName.PRODUCE_TO_DURABLE_BUFFER_LATENCY, "produce_to_durable_buffer_latency");
    map.put(VeniceProducerTehutiMetricName.PREPROCESSING_LATENCY, "preprocessing_latency");
    map.put(VeniceProducerTehutiMetricName.QUEUE_WAIT_LATENCY, "queue_wait_latency");
    map.put(
        VeniceProducerTehutiMetricName.CALLER_TO_PUBSUB_PRODUCER_BUFFER_LATENCY,
        "caller_to_pubsub_producer_buffer_latency");
    map.put(VeniceProducerTehutiMetricName.PENDING_WRITE_OPERATION, "pending_write_operation");
    map.put(VeniceProducerTehutiMetricName.END_TO_END_LATENCY, "end_to_end_latency");
    map.put(VeniceProducerTehutiMetricName.TOTAL_WORKER_QUEUE_SIZE, "total_worker_queue_size");
    map.put(VeniceProducerTehutiMetricName.CALLBACK_QUEUE_SIZE, "callback_queue_size");
    return map;
  }
}
