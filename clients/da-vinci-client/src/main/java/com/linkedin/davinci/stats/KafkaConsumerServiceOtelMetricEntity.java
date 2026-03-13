package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CONSUMER_POOL_ACTION;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import java.util.Set;


/**
 * OTel metric entity definitions for {@link KafkaConsumerServiceStats}.
 * Uses {@code pubsub} (not {@code kafka}) in metric names to align with Venice's PubSub abstraction.
 * Tehuti names are preserved as-is for backward compatibility.
 */
public enum KafkaConsumerServiceOtelMetricEntity implements ModuleMetricEntityInterface {
  POLL_BYTES(
      "ingestion.pubsub.consumer.poll.bytes", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.BYTES,
      "Byte size of polled PubSub messages per poll request", setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME)
  ),

  POLL_RECORD_COUNT(
      "ingestion.pubsub.consumer.poll.record_count", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.NUMBER,
      "Number of records returned per poll request", setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME)
  ),

  POLL_COUNT(
      "ingestion.pubsub.consumer.poll.count", MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES, MetricUnit.NUMBER,
      "Total count of poll requests to the PubSub consumer", setOf(VENICE_CLUSTER_NAME)
  ),

  POLL_TIME(
      "ingestion.pubsub.consumer.poll.time", MetricType.HISTOGRAM, MetricUnit.MILLISECOND,
      "Latency of PubSub consumer poll requests", setOf(VENICE_CLUSTER_NAME)
  ),

  POLL_NON_EMPTY_COUNT(
      "ingestion.pubsub.consumer.poll.non_empty_count", MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES, MetricUnit.NUMBER,
      "Count of poll requests that returned at least one record", setOf(VENICE_CLUSTER_NAME)
  ),

  POLL_ERROR_COUNT(
      "ingestion.pubsub.consumer.poll.error_count", MetricType.COUNTER, MetricUnit.NUMBER,
      "Count of PubSub consumer poll errors", setOf(VENICE_CLUSTER_NAME)
  ),

  PRODUCE_TO_WRITE_BUFFER_TIME(
      "ingestion.pubsub.consumer.produce_to_write_buffer_time", MetricType.HISTOGRAM, MetricUnit.MILLISECOND,
      "Latency of producing consumed records to the write buffer", setOf(VENICE_CLUSTER_NAME)
  ),

  TOPIC_DETECTED_DELETED_COUNT(
      "ingestion.pubsub.consumer.topic.detected_deleted_count", MetricType.COUNTER, MetricUnit.NUMBER,
      "Count of topics detected as deleted", setOf(VENICE_CLUSTER_NAME)
  ),

  ORPHAN_TOPIC_PARTITION_COUNT(
      "ingestion.pubsub.consumer.orphan_subscription_count", MetricType.COUNTER, MetricUnit.NUMBER,
      "Count of topic-partitions assigned to consumer with no running ingestion task", setOf(VENICE_CLUSTER_NAME)
  ),

  /**
   * Latency of consumer pool actions (subscribe, update assignment).
   * Shared OTel instrument differentiated by {@link com.linkedin.venice.stats.dimensions.VeniceConsumerPoolAction}.
   */
  POOL_ACTION_TIME(
      "ingestion.pubsub.consumer.pool_action.time", MetricType.HISTOGRAM, MetricUnit.MILLISECOND,
      "Latency of consumer pool actions (subscribe, update assignment)",
      setOf(VENICE_CLUSTER_NAME, VENICE_CONSUMER_POOL_ACTION)
  ),

  /**
   * Max elapsed time since the last successful poll across all consumers in the pool.
   * Captures the same value as the Tehuti {@code idle_time} sensor. The Tehuti
   * {@code max_elapsed_time_since_last_successful_poll} AsyncGauge is intentionally excluded
   * from OTel because it reads from the same source method
   * ({@link com.linkedin.davinci.kafka.consumer.KafkaConsumerService#getMaxElapsedTimeMSSinceLastPollInConsumerPool})
   * and would be redundant.
   */
  POLL_TIME_SINCE_LAST_SUCCESS(
      "ingestion.pubsub.consumer.poll.time_since_last_success", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
      MetricUnit.MILLISECOND, "Max elapsed time since last successful poll across consumers in the pool",
      setOf(VENICE_CLUSTER_NAME)
  ),

  /** Raw per-consumer partition assignment counts. OTel-only (Tehuti uses 4 pre-computed gauges). */
  PARTITION_ASSIGNMENT_COUNT(
      "ingestion.pubsub.consumer.partition_assignment.count", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
      MetricUnit.NUMBER, "Raw per-consumer partition assignment counts across the consumer pool",
      setOf(VENICE_CLUSTER_NAME)
  );

  private final MetricEntity metricEntity;

  KafkaConsumerServiceOtelMetricEntity(
      String metricName,
      MetricType metricType,
      MetricUnit unit,
      String description,
      Set<VeniceMetricsDimensions> dimensions) {
    this.metricEntity = new MetricEntity(metricName, metricType, unit, description, dimensions);
  }

  @Override
  public MetricEntity getMetricEntity() {
    return metricEntity;
  }
}
