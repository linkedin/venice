package com.linkedin.venice.producer;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_PRODUCER_OPERATION;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.CollectionUtils.setOf;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import java.util.Set;


/**
 * OpenTelemetry metric entities for the Venice online producer. See {@link VeniceProducerMetrics} for how each
 * entity maps to the legacy Tehuti sensors.
 */
public enum VeniceProducerMetricEntity implements ModuleMetricEntityInterface {
  /**
   * Count of completed write requests along with the operation type and outcome.
   */
  CALL_COUNT(
      "call_count", MetricType.COUNTER, MetricUnit.NUMBER,
      "Count of completed write requests along with the operation type and outcome",
      setOf(VENICE_STORE_NAME, VENICE_PRODUCER_OPERATION, VENICE_RESPONSE_STATUS_CODE_CATEGORY)
  ),

  /**
   * End-to-end latency of successful write requests, from the caller's submission to the completion of the future.
   */
  CALL_TIME(
      "call_time", MetricType.HISTOGRAM, MetricUnit.MILLISECOND,
      "End-to-end latency of successful write requests from submission to future completion",
      setOf(VENICE_STORE_NAME, VENICE_PRODUCER_OPERATION, VENICE_RESPONSE_STATUS_CODE_CATEGORY)
  ),

  /**
   * Time a write request waits in the worker queue before preprocessing starts.
   */
  QUEUE_WAIT_TIME(
      "queue_wait_time", MetricType.HISTOGRAM, MetricUnit.MILLISECOND,
      "Time a write request waits in the worker queue before preprocessing starts",
      setOf(VENICE_STORE_NAME, VENICE_PRODUCER_OPERATION)
  ),

  /**
   * Time to preprocess a write request, i.e. schema lookup and serialization.
   */
  PREPROCESSING_TIME(
      "preprocessing_time", MetricType.HISTOGRAM, MetricUnit.MILLISECOND,
      "Time to preprocess a write request (schema lookup and serialization)",
      setOf(VENICE_STORE_NAME, VENICE_PRODUCER_OPERATION)
  ),

  /**
   * Time from the end of preprocessing until the PubSub broker acknowledges a successful write.
   */
  PRODUCE_TO_DURABLE_BUFFER_TIME(
      "produce_to_durable_buffer_time", MetricType.HISTOGRAM, MetricUnit.MILLISECOND,
      "Time from the end of preprocessing until the PubSub broker acknowledges a successful write",
      setOf(VENICE_STORE_NAME, VENICE_PRODUCER_OPERATION)
  ),

  /**
   * Time from the caller's submission until the write is handed to the PubSub producer's buffer.
   */
  CALLER_TO_PUBSUB_PRODUCER_BUFFER_TIME(
      "caller_to_pubsub_producer_buffer_time", MetricType.HISTOGRAM, MetricUnit.MILLISECOND,
      "Time from the caller's submission until the write is handed to the PubSub producer's buffer",
      setOf(VENICE_STORE_NAME, VENICE_PRODUCER_OPERATION)
  ),

  /**
   * Count of write requests that were submitted but are not completed yet.
   */
  PENDING_CALL_COUNT(
      "pending_call_count", MetricType.ASYNC_GAUGE, MetricUnit.NUMBER,
      "Count of write requests that were submitted but are not completed yet", setOf(VENICE_STORE_NAME)
  );

  private final MetricEntity entity;

  VeniceProducerMetricEntity(
      String name,
      MetricType metricType,
      MetricUnit unit,
      String description,
      Set<VeniceMetricsDimensions> dimensions) {
    this.entity = new MetricEntity(name, metricType, unit, description, dimensions);
  }

  @Override
  public MetricEntity getMetricEntity() {
    return entity;
  }
}
