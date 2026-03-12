package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.KafkaConsumerServiceOtelMetricEntity.CONSUMER_ACTION_TIME;
import static com.linkedin.davinci.stats.KafkaConsumerServiceOtelMetricEntity.PARTITION_ASSIGNMENT_COUNT;
import static com.linkedin.davinci.stats.KafkaConsumerServiceOtelMetricEntity.POLL_BYTES;
import static com.linkedin.davinci.stats.KafkaConsumerServiceOtelMetricEntity.POLL_COUNT;
import static com.linkedin.davinci.stats.KafkaConsumerServiceOtelMetricEntity.POLL_ERROR_COUNT;
import static com.linkedin.davinci.stats.KafkaConsumerServiceOtelMetricEntity.POLL_NON_EMPTY_COUNT;
import static com.linkedin.davinci.stats.KafkaConsumerServiceOtelMetricEntity.POLL_RECORD_COUNT;
import static com.linkedin.davinci.stats.KafkaConsumerServiceOtelMetricEntity.POLL_TIME;
import static com.linkedin.davinci.stats.KafkaConsumerServiceOtelMetricEntity.POLL_TIME_SINCE_LAST_SUCCESS;
import static com.linkedin.davinci.stats.KafkaConsumerServiceOtelMetricEntity.POOL_IDLE_TIME;
import static com.linkedin.davinci.stats.KafkaConsumerServiceOtelMetricEntity.PRODUCE_TO_WRITE_BUFFER_TIME;
import static com.linkedin.davinci.stats.KafkaConsumerServiceOtelMetricEntity.TOPIC_DELETED_COUNT;
import static com.linkedin.davinci.stats.KafkaConsumerServiceOtelMetricEntity.TOPIC_NO_INGESTION_COUNT;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CONSUMER_POOL_ACTION;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture.MetricEntityExpectation;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class KafkaConsumerServiceOtelMetricEntityTest {
  @Test
  public void testMetricEntities() {
    new ModuleMetricEntityTestFixture<>(KafkaConsumerServiceOtelMetricEntity.class, expectedDefinitions()).assertAll();
  }

  private static Map<KafkaConsumerServiceOtelMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<KafkaConsumerServiceOtelMetricEntity, MetricEntityExpectation> map = new HashMap<>();
    map.put(
        POLL_BYTES,
        new MetricEntityExpectation(
            "ingestion.pubsub.consumer.poll.bytes",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.BYTES,
            "Byte size of polled PubSub messages per poll request",
            setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME)));
    map.put(
        POLL_RECORD_COUNT,
        new MetricEntityExpectation(
            "ingestion.pubsub.consumer.poll.record_count",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.NUMBER,
            "Number of records returned per poll request",
            setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME)));
    map.put(
        POLL_COUNT,
        new MetricEntityExpectation(
            "ingestion.pubsub.consumer.poll.count",
            MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES,
            MetricUnit.NUMBER,
            "Total count of poll requests to the PubSub consumer",
            setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME)));
    map.put(
        POLL_TIME,
        new MetricEntityExpectation(
            "ingestion.pubsub.consumer.poll.time",
            MetricType.HISTOGRAM,
            MetricUnit.MILLISECOND,
            "Latency of PubSub consumer poll requests",
            setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME)));
    map.put(
        POLL_NON_EMPTY_COUNT,
        new MetricEntityExpectation(
            "ingestion.pubsub.consumer.poll.non_empty_count",
            MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES,
            MetricUnit.NUMBER,
            "Count of poll requests that returned at least one record",
            setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME)));
    map.put(
        POLL_ERROR_COUNT,
        new MetricEntityExpectation(
            "ingestion.pubsub.consumer.poll.error_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of PubSub consumer poll errors",
            setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME)));
    map.put(
        PRODUCE_TO_WRITE_BUFFER_TIME,
        new MetricEntityExpectation(
            "ingestion.pubsub.consumer.produce_to_write_buffer_time",
            MetricType.HISTOGRAM,
            MetricUnit.MILLISECOND,
            "Latency of producing consumed records to the write buffer",
            setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME)));
    map.put(
        TOPIC_DELETED_COUNT,
        new MetricEntityExpectation(
            "ingestion.pubsub.consumer.topic.deleted_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of detected deleted topics",
            setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME)));
    map.put(
        TOPIC_NO_INGESTION_COUNT,
        new MetricEntityExpectation(
            "ingestion.pubsub.consumer.topic.no_ingestion_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of topic-partitions with no running ingestion task",
            setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME)));
    map.put(
        CONSUMER_ACTION_TIME,
        new MetricEntityExpectation(
            "ingestion.pubsub.consumer.consumer_action.time",
            MetricType.HISTOGRAM,
            MetricUnit.MILLISECOND,
            "Latency of consumer pool actions (subscribe, update assignment)",
            setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_CONSUMER_POOL_ACTION)));
    map.put(
        POOL_IDLE_TIME,
        new MetricEntityExpectation(
            "ingestion.pubsub.consumer.pool.idle_time",
            MetricType.GAUGE,
            MetricUnit.MILLISECOND,
            "Maximum idle time of the consumer pool since last successful poll",
            setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME)));
    map.put(
        POLL_TIME_SINCE_LAST_SUCCESS,
        new MetricEntityExpectation(
            "ingestion.pubsub.consumer.poll.time_since_last_success",
            MetricType.ASYNC_GAUGE,
            MetricUnit.MILLISECOND,
            "Maximum elapsed time since the last successful poll across all consumers in the pool",
            setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME)));
    map.put(
        PARTITION_ASSIGNMENT_COUNT,
        new MetricEntityExpectation(
            "ingestion.pubsub.consumer.partition_assignment.count",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.NUMBER,
            "Raw per-consumer partition assignment counts across the consumer pool",
            setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME)));
    return map;
  }
}
