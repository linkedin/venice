package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_DCR_EVENT;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_DESTINATION_REGION;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_INGESTION_DESTINATION_COMPONENT;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_INGESTION_SOURCE_COMPONENT;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REGION_LOCALITY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REGION_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REPLICA_STATE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REPLICA_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_SOURCE_REGION;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_VERSION_ROLE;
import static com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface.getUniqueMetricEntities;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import java.util.Collection;
import java.util.Set;


/**
 * List all metric entities for Venice server (storage node).
 */
public enum ServerMetricEntity implements ModuleMetricEntityInterface {
  INGESTION_HEARTBEAT_DELAY(
      "ingestion.replication.heartbeat.delay", MetricType.HISTOGRAM, MetricUnit.MILLISECOND,
      "Nearline ingestion replication lag",
      setOf(
          VENICE_STORE_NAME,
          VENICE_CLUSTER_NAME,
          VENICE_REGION_NAME,
          VENICE_VERSION_ROLE,
          VENICE_REPLICA_TYPE,
          VENICE_REPLICA_STATE)
  ),

  INGESTION_TASK_ERROR_COUNT(
      "ingestion.task.error_count", MetricType.ASYNC_GAUGE, MetricUnit.NUMBER,
      "Count of ingestion tasks in error state", setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
  ),

  INGESTION_TASK_PUSH_TIMEOUT_COUNT(
      "ingestion.task.push_timeout_count", MetricType.ASYNC_GAUGE, MetricUnit.NUMBER,
      "Count of ingestion tasks timed out during push operation",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
  ),

  WRITE_COMPUTE_OPERATION_FAILURE_CODE(
      "ingestion.write_compute.operation.failure_code", MetricType.ASYNC_GAUGE, MetricUnit.NUMBER,
      "Write-compute operation failure code", setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
  ),

  DISK_QUOTA_USED(
      "disk.quota.used", MetricType.ASYNC_GAUGE, MetricUnit.RATIO, "Disk quota used for the store version",
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
      "Leader producer callback latency (ack wait time)",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE, VENICE_REPLICA_TYPE)
  ),

  INGESTION_PREPROCESSING_LEADER_TIME(
      "ingestion.preprocessing.leader.time", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.MILLISECOND,
      "Preprocessing latency during ingestion", setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
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
      "Latency from leader producing to producer completion",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
  ),

  BATCH_PROCESSING_REQUEST_COUNT(
      "ingestion.batch_processing.request.count", MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES, MetricUnit.NUMBER,
      "Batch processing requests", setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
  ),

  BATCH_PROCESSING_REQUEST_RECORD_COUNT(
      "ingestion.batch_processing.request.record.count", MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES,
      MetricUnit.NUMBER, "Total records across batch-processing requests",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
  ),

  BATCH_PROCESSING_REQUEST_ERROR_COUNT(
      "ingestion.batch_processing.request.error_count", MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES, MetricUnit.NUMBER,
      "Failed batch processing requests", setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
  ),

  BATCH_PROCESSING_REQUEST_TIME(
      "ingestion.batch_processing.request.time", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.MILLISECOND,
      "Batch processing latency", setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
  ),

  DCR_EVENT_COUNT(
      "ingestion.dcr.event_count", MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES, MetricUnit.NUMBER,
      "Deterministic Conflict Resolution (DCR) count specific to certain events",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE, VENICE_DCR_EVENT)
  ),

  DCR_TOTAL_COUNT(
      "ingestion.dcr.total_count", MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES, MetricUnit.NUMBER,
      "Deterministic Conflict Resolution (DCR) total count",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
  ),

  CONSUMER_IDLE_TIME(
      "ingestion.consumer.idle_time", MetricType.ASYNC_GAUGE, MetricUnit.MILLISECOND, "Consumer idle time",
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
  );

  public static final Collection<MetricEntity> SERVER_METRIC_ENTITIES =
      getUniqueMetricEntities(ServerMetricEntity.class);

  private final MetricEntity metricEntity;

  ServerMetricEntity(
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
