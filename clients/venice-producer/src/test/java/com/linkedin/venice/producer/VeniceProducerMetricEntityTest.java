package com.linkedin.venice.producer;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_PRODUCER_OPERATION;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture.MetricEntityExpectation;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class VeniceProducerMetricEntityTest {
  @Test
  public void testMetricEntities() {
    new ModuleMetricEntityTestFixture<>(VeniceProducerMetricEntity.class, expectedDefinitions()).assertAll();
  }

  private static Map<VeniceProducerMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<VeniceProducerMetricEntity, MetricEntityExpectation> map = new HashMap<>();
    map.put(
        VeniceProducerMetricEntity.CALL_COUNT,
        new MetricEntityExpectation(
            "call_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of completed write requests along with the operation type and outcome",
            setOf(VENICE_STORE_NAME, VENICE_PRODUCER_OPERATION, VENICE_RESPONSE_STATUS_CODE_CATEGORY)));
    map.put(
        VeniceProducerMetricEntity.CALL_TIME,
        new MetricEntityExpectation(
            "call_time",
            MetricType.HISTOGRAM,
            MetricUnit.MILLISECOND,
            "End-to-end latency of successful write requests from submission to future completion",
            setOf(VENICE_STORE_NAME, VENICE_PRODUCER_OPERATION)));
    map.put(
        VeniceProducerMetricEntity.QUEUE_WAIT_TIME,
        new MetricEntityExpectation(
            "queue_wait_time",
            MetricType.HISTOGRAM,
            MetricUnit.MILLISECOND,
            "Time a write request waits in the worker queue before preprocessing starts",
            setOf(VENICE_STORE_NAME, VENICE_PRODUCER_OPERATION)));
    map.put(
        VeniceProducerMetricEntity.PREPROCESSING_TIME,
        new MetricEntityExpectation(
            "preprocessing_time",
            MetricType.HISTOGRAM,
            MetricUnit.MILLISECOND,
            "Time to preprocess a write request (schema lookup and serialization)",
            setOf(VENICE_STORE_NAME, VENICE_PRODUCER_OPERATION)));
    map.put(
        VeniceProducerMetricEntity.PRODUCE_TO_DURABLE_BUFFER_TIME,
        new MetricEntityExpectation(
            "produce_to_durable_buffer_time",
            MetricType.HISTOGRAM,
            MetricUnit.MILLISECOND,
            "Time from the end of preprocessing until the PubSub broker acknowledges a successful write",
            setOf(VENICE_STORE_NAME, VENICE_PRODUCER_OPERATION)));
    map.put(
        VeniceProducerMetricEntity.CALLER_TO_PUBSUB_PRODUCER_BUFFER_TIME,
        new MetricEntityExpectation(
            "caller_to_pubsub_producer_buffer_time",
            MetricType.HISTOGRAM,
            MetricUnit.MILLISECOND,
            "Time from the caller's submission until the write is handed to the PubSub producer's buffer",
            setOf(VENICE_STORE_NAME, VENICE_PRODUCER_OPERATION)));
    map.put(
        VeniceProducerMetricEntity.PENDING_CALL_COUNT,
        new MetricEntityExpectation(
            "pending_call_count",
            MetricType.ASYNC_GAUGE,
            MetricUnit.NUMBER,
            "Count of write requests that were submitted but are not completed yet",
            setOf(VENICE_STORE_NAME)));
    map.put(
        VeniceProducerMetricEntity.WORKER_QUEUE_TASK_COUNT,
        new MetricEntityExpectation(
            "worker.queue.task_count",
            MetricType.ASYNC_GAUGE,
            MetricUnit.NUMBER,
            "Total count of tasks queued across all the producer workers",
            setOf(VENICE_STORE_NAME)));
    map.put(
        VeniceProducerMetricEntity.CALLBACK_QUEUE_TASK_COUNT,
        new MetricEntityExpectation(
            "callback.queue.task_count",
            MetricType.ASYNC_GAUGE,
            MetricUnit.NUMBER,
            "Count of tasks queued in the producer callback executor",
            setOf(VENICE_STORE_NAME)));
    return map;
  }
}
