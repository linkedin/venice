package com.linkedin.davinci.stats.ingestion;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_DCR_EVENT;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_DCR_OPERATION;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_DESTINATION_REGION;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_INGESTION_DESTINATION_COMPONENT;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_INGESTION_FAILURE_REASON;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_INGESTION_SOURCE_COMPONENT;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RECORD_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REGION_LOCALITY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REPLICA_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_SOURCE_REGION;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_VERSION_ROLE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_WRITE_COMPUTE_OPERATION;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import java.util.Set;


public enum IngestionOtelMetricEntity implements ModuleMetricEntityInterface {
  INGESTION_TASK_ERROR_COUNT(
      "ingestion.task.error_count", MetricType.ASYNC_GAUGE, MetricUnit.NUMBER,
      "Count of ingestion tasks in error state", setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
  ),

  INGESTION_TASK_PUSH_TIMEOUT_COUNT(
      "ingestion.task.push_timeout_count", MetricType.ASYNC_GAUGE, MetricUnit.NUMBER,
      "Count of ingestion tasks timed out during push operation",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
  ),

  DISK_QUOTA_USED(
      "ingestion.disk_quota.used", MetricType.ASYNC_GAUGE, MetricUnit.RATIO, "Disk quota used for the store version",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
  ),

  INGESTION_RECORDS_CONSUMED(
      "ingestion.records.consumed", MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES, MetricUnit.NUMBER,
      "Records consumed from remote/local topic",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE, VENICE_REPLICA_TYPE)
  ),

  INGESTION_RECORDS_PRODUCED(
      "ingestion.records.produced", MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES, MetricUnit.NUMBER,
      "Records produced to local topic",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE, VENICE_REPLICA_TYPE)
  ),

  INGESTION_BYTES_CONSUMED(
      "ingestion.bytes.consumed", MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES, MetricUnit.BYTES,
      "Bytes consumed from remote/local topic that are successfully processed excluding control/DIV messages",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE, VENICE_REPLICA_TYPE)
  ),

  INGESTION_BYTES_PRODUCED(
      "ingestion.bytes.produced", MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES, MetricUnit.BYTES,
      "Bytes produced to local topic",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE, VENICE_REPLICA_TYPE)
  ),

  INGESTION_SUBSCRIBE_PREP_TIME(
      "ingestion.subscribe.prep.time", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.MILLISECOND,
      "Subscription preparation latency", setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
  ),

  INGESTION_TIME(
      "ingestion.time", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.MILLISECOND,
      "End-to-end processing time from topic poll to storage write",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
  ),

  INGESTION_PRODUCER_CALLBACK_TIME(
      "ingestion.producer.callback.time", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.MILLISECOND,
      "Time spent inside the producer callback after pubsub broker acknowledgement, including result processing and queuing the record to the drainer",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE, VENICE_REPLICA_TYPE)
  ),

  INGESTION_PREPROCESSING_LEADER_TIME(
      "ingestion.preprocessing.leader.time", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.MILLISECOND,
      "Leader-side preprocessing latency during ingestion",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
  ),

  INGESTION_PREPROCESSING_INTERNAL_TIME(
      "ingestion.preprocessing.internal.time", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.MILLISECOND,
      "Internal preprocessing latency during ingestion",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
  ),

  INGESTION_TIME_BETWEEN_COMPONENTS(
      "ingestion.time_between_components", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.MILLISECOND,
      "Ingestion latency between different components of the flow",
      setOf(
          VENICE_STORE_NAME,
          VENICE_CLUSTER_NAME,
          VENICE_VERSION_ROLE,
          VENICE_INGESTION_SOURCE_COMPONENT,
          VENICE_INGESTION_DESTINATION_COMPONENT)
  ),

  INGESTION_PRODUCER_TIME(
      "ingestion.producer.time", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.MILLISECOND,
      "Time from when the leader enqueues a message to when the pubsub producer invokes the callback, measuring broker acknowledgement latency",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
  ),

  BATCH_PROCESSING_REQUEST_COUNT(
      "ingestion.batch_processing.request.count", MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES, MetricUnit.NUMBER,
      "Count of batch processing requests during ingestion",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
  ),

  BATCH_PROCESSING_REQUEST_RECORD_COUNT(
      "ingestion.batch_processing.request.record.count", MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES,
      MetricUnit.NUMBER, "Total records across batch-processing requests",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
  ),

  BATCH_PROCESSING_REQUEST_ERROR_COUNT(
      "ingestion.batch_processing.request.error_count", MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES, MetricUnit.NUMBER,
      "Count of failed batch processing requests during ingestion",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
  ),

  BATCH_PROCESSING_REQUEST_TIME(
      "ingestion.batch_processing.request.time", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.MILLISECOND,
      "Batch processing latency", setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
  ),

  DCR_EVENT_COUNT(
      "ingestion.dcr.event_count", MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES, MetricUnit.NUMBER,
      "Count of DCR outcomes per event type (e.g., PUT, DELETE, UPDATE)",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE, VENICE_DCR_EVENT)
  ),

  DCR_TOTAL_COUNT(
      "ingestion.dcr.total_count", MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES, MetricUnit.NUMBER,
      "Deterministic Conflict Resolution (DCR) total count",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
  ),

  CONSUMER_IDLE_TIME(
      "ingestion.consumer.idle_time", MetricType.ASYNC_GAUGE, MetricUnit.MILLISECOND,
      "Time the ingestion consumer has been idle without polling records",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
  ),

  DUPLICATE_KEY_UPDATE_COUNT(
      "ingestion.key.update.duplicate_count", MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES, MetricUnit.NUMBER,
      "Count of duplicate-key updates during ingestion",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
  ),

  RT_RECORDS_CONSUMED(
      "ingestion.records.consumed_from_real_time_topic", MetricType.COUNTER, MetricUnit.NUMBER,
      "Records consumed from local/remote region real-time topics",
      setOf(
          VENICE_STORE_NAME,
          VENICE_CLUSTER_NAME,
          VENICE_VERSION_ROLE,
          VENICE_SOURCE_REGION,
          VENICE_DESTINATION_REGION,
          VENICE_REGION_LOCALITY)
  ),

  RT_BYTES_CONSUMED(
      "ingestion.bytes.consumed_from_real_time_topic", MetricType.COUNTER, MetricUnit.BYTES,
      "Bytes consumed from local/remote region real-time topics",
      setOf(
          VENICE_STORE_NAME,
          VENICE_CLUSTER_NAME,
          VENICE_VERSION_ROLE,
          VENICE_SOURCE_REGION,
          VENICE_DESTINATION_REGION,
          VENICE_REGION_LOCALITY)
  ),

  CONSUMER_QUEUE_PUT_TIME(
      "ingestion.consumer_queue.put.time", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.MILLISECOND,
      "Time to put consumed records into the consumer queue",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
  ),

  STORAGE_ENGINE_PUT_TIME(
      "ingestion.storage_engine.put.time", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.MILLISECOND,
      "Time to put records into the storage engine", setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
  ),

  STORAGE_ENGINE_DELETE_TIME(
      "ingestion.storage_engine.delete.time", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.MILLISECOND,
      "Time to delete records from the storage engine",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
  ),

  CONSUMER_ACTION_TIME(
      "ingestion.consumer_action.time", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.MILLISECOND,
      "Time to process a batch of consumer actions (subscribe, unsubscribe, etc.)",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
  ),

  LONG_RUNNING_TASK_CHECK_TIME(
      "ingestion.long_running_task.check.time", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.MILLISECOND,
      "Time to check long running task state", setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
  ),

  VIEW_WRITER_PRODUCE_TIME(
      "ingestion.view_writer.produce.time", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.MILLISECOND,
      "Time for the view writer to trigger all writes",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
  ),

  VIEW_WRITER_ACK_TIME(
      "ingestion.view_writer.ack.time", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.MILLISECOND,
      "Time for the view writer to receive acknowledgement for all writes",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
  ),

  PRODUCER_ENQUEUE_TIME(
      "ingestion.producer.enqueue.time", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.MILLISECOND,
      "Time for the leader to execute the synchronous produce call that enqueues a message into the pubsub producer buffer",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
  ),

  PRODUCER_COMPRESS_TIME(
      "ingestion.producer.compress.time", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.MILLISECOND,
      "Time to compress records before producing", setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
  ),

  PRODUCER_SYNCHRONIZE_TIME(
      "ingestion.producer.synchronize.time", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.MILLISECOND,
      "Time spent waiting for the last leader-produced message to be persisted during partition stop or leader handoff",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
  ),

  WRITE_COMPUTE_TIME(
      "ingestion.write_compute.time", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.MILLISECOND,
      "Time to perform write compute operations",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE, VENICE_WRITE_COMPUTE_OPERATION)
  ),

  DCR_LOOKUP_TIME(
      "ingestion.dcr.lookup.time", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.MILLISECOND,
      "Time to look up existing records during conflict resolution",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE, VENICE_RECORD_TYPE)
  ),

  DCR_MERGE_TIME(
      "ingestion.dcr.merge.time", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.MILLISECOND,
      "Time to merge records during conflict resolution",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE, VENICE_DCR_OPERATION)
  ),

  UNEXPECTED_MESSAGE_COUNT(
      "ingestion.unexpected_message.count", MetricType.COUNTER, MetricUnit.NUMBER,
      "Count of unexpected messages encountered during ingestion",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
  ),

  STORE_METADATA_INCONSISTENT_COUNT(
      "ingestion.store_metadata.inconsistent_count", MetricType.COUNTER, MetricUnit.NUMBER,
      "Count of store metadata inconsistency events", setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
  ),

  INGESTION_FAILURE_COUNT(
      "ingestion.failure.count", MetricType.COUNTER, MetricUnit.NUMBER, "Count of ingestion failures by reason",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE, VENICE_INGESTION_FAILURE_REASON)
  ),

  RESUBSCRIPTION_FAILURE_COUNT(
      "ingestion.resubscription_failure.count", MetricType.COUNTER, MetricUnit.NUMBER,
      "Count of resubscription failures during ingestion",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
  ),

  WRITE_COMPUTE_CACHE_HIT_COUNT(
      "ingestion.write_compute.cache.hit_count", MetricType.COUNTER, MetricUnit.NUMBER,
      "Count of write compute cache hits", setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
  ),

  CHECKSUM_VERIFICATION_FAILURE_COUNT(
      "ingestion.checksum_verification_failure.count", MetricType.COUNTER, MetricUnit.NUMBER,
      "Count of checksum verification failures", setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
  ),

  DCR_LOOKUP_CACHE_HIT_COUNT(
      "ingestion.dcr.lookup.cache.hit_count", MetricType.COUNTER, MetricUnit.NUMBER,
      "Cache hits when looking up existing value bytes or replication metadata before conflict resolution",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE, VENICE_RECORD_TYPE)
  ),

  BYTES_CONSUMED_AS_UNCOMPRESSED_SIZE(
      "ingestion.bytes.consumed_as_uncompressed_size", MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES, MetricUnit.BYTES,
      "Bytes consumed from pubsub as uncompressed size",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
  ),

  RECORD_KEY_SIZE(
      "ingestion.record.key_size", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.BYTES,
      "Size of record keys during ingestion", setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
  ),

  RECORD_VALUE_SIZE(
      "ingestion.record.value_size", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.BYTES,
      "Size of record values during ingestion", setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
  ),

  RECORD_ASSEMBLED_SIZE(
      "ingestion.record.assembled_size", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.BYTES,
      "Size of assembled records during ingestion",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE, VENICE_RECORD_TYPE)
  ),

  RECORD_ASSEMBLED_SIZE_RATIO(
      "ingestion.record.assembled_size_ratio", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.RATIO,
      "Ratio of assembled record size to the max record size limit",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
  ),

  INGESTION_TASK_COUNT(
      "ingestion.task.count", MetricType.ASYNC_GAUGE, MetricUnit.NUMBER, "Count of active ingestion tasks",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
  );

  private final MetricEntity metricEntity;

  IngestionOtelMetricEntity(
      String name,
      MetricType metricType,
      MetricUnit unit,
      String description,
      Set<VeniceMetricsDimensions> dimensionsList) {
    this.metricEntity = new MetricEntity(name, metricType, unit, description, dimensionsList);
  }

  @Override
  public MetricEntity getMetricEntity() {
    return metricEntity;
  }
}
