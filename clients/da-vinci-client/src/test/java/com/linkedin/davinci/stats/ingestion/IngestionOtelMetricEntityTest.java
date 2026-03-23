package com.linkedin.davinci.stats.ingestion;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_DCR_EVENT;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_DCR_OPERATION;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_DESTINATION_REGION;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_GLOBAL_RT_DIV_ERROR_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_INGESTION_DESTINATION_COMPONENT;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_INGESTION_FAILURE_REASON;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_INGESTION_SOURCE_COMPONENT;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_PARTIAL_UPDATE_OPERATION_PHASE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RECORD_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REGION_LOCALITY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REPLICA_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_SOURCE_REGION;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_VERSION_ROLE;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture.MetricEntityExpectation;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;


public class IngestionOtelMetricEntityTest {
  @Test
  public void testMetricEntities() {
    new ModuleMetricEntityTestFixture<>(IngestionOtelMetricEntity.class, expectedDefinitions()).assertAll();
  }

  private static Map<IngestionOtelMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<IngestionOtelMetricEntity, MetricEntityExpectation> map = new HashMap<>();

    Set<VeniceMetricsDimensions> storeClusterVersion =
        setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE);
    Set<VeniceMetricsDimensions> storeClusterVersionReplica =
        setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE, VENICE_REPLICA_TYPE);
    Set<VeniceMetricsDimensions> storeClusterVersionDcr =
        setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE, VENICE_DCR_EVENT);
    Set<VeniceMetricsDimensions> storeClusterVersionComponents = setOf(
        VENICE_STORE_NAME,
        VENICE_CLUSTER_NAME,
        VENICE_VERSION_ROLE,
        VENICE_INGESTION_SOURCE_COMPONENT,
        VENICE_INGESTION_DESTINATION_COMPONENT);
    Set<VeniceMetricsDimensions> storeClusterVersionRegion = setOf(
        VENICE_STORE_NAME,
        VENICE_CLUSTER_NAME,
        VENICE_VERSION_ROLE,
        VENICE_SOURCE_REGION,
        VENICE_DESTINATION_REGION,
        VENICE_REGION_LOCALITY);
    Set<VeniceMetricsDimensions> storeClusterVersionPartialUpdateOp =
        setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE, VENICE_PARTIAL_UPDATE_OPERATION_PHASE);
    Set<VeniceMetricsDimensions> storeClusterVersionRecordType =
        setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE, VENICE_RECORD_TYPE);
    Set<VeniceMetricsDimensions> storeClusterVersionDcrOp =
        setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE, VENICE_DCR_OPERATION);
    Set<VeniceMetricsDimensions> storeClusterVersionFailureReason =
        setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE, VENICE_INGESTION_FAILURE_REASON);
    Set<VeniceMetricsDimensions> storeClusterVersionGlobalRtDivError =
        setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE, VENICE_GLOBAL_RT_DIV_ERROR_TYPE);

    // ASYNC_GAUGE metrics
    map.put(
        IngestionOtelMetricEntity.INGESTION_TASK_ERROR_COUNT,
        new MetricEntityExpectation(
            "ingestion.task.error_count",
            MetricType.ASYNC_GAUGE,
            MetricUnit.NUMBER,
            "Count of ingestion tasks in error state",
            storeClusterVersion));
    map.put(
        IngestionOtelMetricEntity.INGESTION_TASK_PUSH_TIMEOUT_COUNT,
        new MetricEntityExpectation(
            "ingestion.task.push_timeout_count",
            MetricType.ASYNC_GAUGE,
            MetricUnit.NUMBER,
            "Count of ingestion tasks timed out during push operation",
            storeClusterVersion));
    map.put(
        IngestionOtelMetricEntity.DISK_QUOTA_USED,
        new MetricEntityExpectation(
            "ingestion.disk_quota.used",
            MetricType.ASYNC_DOUBLE_GAUGE,
            MetricUnit.RATIO,
            "Disk quota usage ratio for the store version",
            storeClusterVersion));
    map.put(
        IngestionOtelMetricEntity.CONSUMER_IDLE_TIME,
        new MetricEntityExpectation(
            "ingestion.consumer.idle_time",
            MetricType.ASYNC_GAUGE,
            MetricUnit.MILLISECOND,
            "Time the ingestion consumer has been idle without polling records",
            storeClusterVersion));

    // ASYNC_COUNTER_FOR_HIGH_PERF_CASES metrics with VersionRole + ReplicaType
    map.put(
        IngestionOtelMetricEntity.INGESTION_RECORDS_CONSUMED,
        new MetricEntityExpectation(
            "ingestion.records.consumed",
            MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES,
            MetricUnit.NUMBER,
            "Records consumed from remote/local topic",
            storeClusterVersionReplica));
    map.put(
        IngestionOtelMetricEntity.INGESTION_RECORDS_PRODUCED,
        new MetricEntityExpectation(
            "ingestion.records.produced",
            MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES,
            MetricUnit.NUMBER,
            "Records produced to local topic",
            storeClusterVersionReplica));
    map.put(
        IngestionOtelMetricEntity.INGESTION_BYTES_CONSUMED,
        new MetricEntityExpectation(
            "ingestion.bytes.consumed",
            MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES,
            MetricUnit.BYTES,
            "Bytes consumed from remote/local topic that are successfully processed excluding control/DIV messages",
            storeClusterVersionReplica));
    map.put(
        IngestionOtelMetricEntity.INGESTION_BYTES_PRODUCED,
        new MetricEntityExpectation(
            "ingestion.bytes.produced",
            MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES,
            MetricUnit.BYTES,
            "Bytes produced to local topic",
            storeClusterVersionReplica));

    // MIN_MAX_COUNT_SUM_AGGREGATIONS metrics with VersionRole only
    map.put(
        IngestionOtelMetricEntity.INGESTION_SUBSCRIBE_PREP_TIME,
        new MetricEntityExpectation(
            "ingestion.subscribe.prep.time",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.MILLISECOND,
            "Subscription preparation latency",
            storeClusterVersion));
    map.put(
        IngestionOtelMetricEntity.INGESTION_TIME,
        new MetricEntityExpectation(
            "ingestion.time",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.MILLISECOND,
            "End-to-end processing time from topic poll to storage write",
            storeClusterVersion));
    map.put(
        IngestionOtelMetricEntity.INGESTION_PREPROCESSING_LEADER_TIME,
        new MetricEntityExpectation(
            "ingestion.preprocessing.leader.time",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.MILLISECOND,
            "Leader-side preprocessing latency during ingestion",
            storeClusterVersion));
    map.put(
        IngestionOtelMetricEntity.INGESTION_PREPROCESSING_INTERNAL_TIME,
        new MetricEntityExpectation(
            "ingestion.preprocessing.internal.time",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.MILLISECOND,
            "Internal preprocessing latency during ingestion",
            storeClusterVersion));
    map.put(
        IngestionOtelMetricEntity.INGESTION_PRODUCER_TIME,
        new MetricEntityExpectation(
            "ingestion.producer.time",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.MILLISECOND,
            "Time from when the leader initiates the produce call to when the pubsub producer invokes the acknowledgement callback, measuring the full produce-to-ack round trip",
            storeClusterVersion));
    map.put(
        IngestionOtelMetricEntity.BATCH_PROCESSING_REQUEST_TIME,
        new MetricEntityExpectation(
            "ingestion.batch_processing.request.time",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.MILLISECOND,
            "Batch processing latency",
            storeClusterVersion));

    // MIN_MAX_COUNT_SUM_AGGREGATIONS metric with VersionRole + ReplicaType
    map.put(
        IngestionOtelMetricEntity.INGESTION_PRODUCER_CALLBACK_TIME,
        new MetricEntityExpectation(
            "ingestion.producer.callback.time",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.MILLISECOND,
            "Time spent inside the producer callback after pubsub broker acknowledgement, including result processing and queuing the record to the drainer",
            storeClusterVersionReplica));

    // MIN_MAX_COUNT_SUM_AGGREGATIONS metric with VersionRole + components
    map.put(
        IngestionOtelMetricEntity.INGESTION_TIME_BETWEEN_COMPONENTS,
        new MetricEntityExpectation(
            "ingestion.time_between_components",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.MILLISECOND,
            "Ingestion latency between different components of the flow",
            storeClusterVersionComponents));

    // ASYNC_COUNTER_FOR_HIGH_PERF_CASES metrics with VersionRole only
    map.put(
        IngestionOtelMetricEntity.BATCH_PROCESSING_REQUEST_COUNT,
        new MetricEntityExpectation(
            "ingestion.batch_processing.request.count",
            MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES,
            MetricUnit.NUMBER,
            "Count of batch processing requests during ingestion",
            storeClusterVersion));
    map.put(
        IngestionOtelMetricEntity.BATCH_PROCESSING_REQUEST_RECORD_COUNT,
        new MetricEntityExpectation(
            "ingestion.batch_processing.request.record.count",
            MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES,
            MetricUnit.NUMBER,
            "Total records across batch-processing requests",
            storeClusterVersion));
    map.put(
        IngestionOtelMetricEntity.BATCH_PROCESSING_REQUEST_ERROR_COUNT,
        new MetricEntityExpectation(
            "ingestion.batch_processing.request.error_count",
            MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES,
            MetricUnit.NUMBER,
            "Count of failed batch processing requests during ingestion",
            storeClusterVersion));
    map.put(
        IngestionOtelMetricEntity.DCR_EVENT_COUNT,
        new MetricEntityExpectation(
            "ingestion.dcr.event_count",
            MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES,
            MetricUnit.NUMBER,
            "Count of DCR outcomes per event type (e.g., PUT, DELETE, UPDATE)",
            storeClusterVersionDcr));
    map.put(
        IngestionOtelMetricEntity.DCR_TOTAL_COUNT,
        new MetricEntityExpectation(
            "ingestion.dcr.total_count",
            MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES,
            MetricUnit.NUMBER,
            "Deterministic Conflict Resolution (DCR) total count",
            storeClusterVersion));
    map.put(
        IngestionOtelMetricEntity.DUPLICATE_KEY_UPDATE_COUNT,
        new MetricEntityExpectation(
            "ingestion.key.update.duplicate_count",
            MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES,
            MetricUnit.NUMBER,
            "Count of duplicate-key updates during ingestion",
            storeClusterVersion));

    // COUNTER metrics with region dimensions
    map.put(
        IngestionOtelMetricEntity.RT_RECORDS_CONSUMED,
        new MetricEntityExpectation(
            "ingestion.records.consumed_from_real_time_topic",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Records consumed from local/remote region real-time topics",
            storeClusterVersionRegion));
    map.put(
        IngestionOtelMetricEntity.RT_BYTES_CONSUMED,
        new MetricEntityExpectation(
            "ingestion.bytes.consumed_from_real_time_topic",
            MetricType.COUNTER,
            MetricUnit.BYTES,
            "Bytes consumed from local/remote region real-time topics",
            storeClusterVersionRegion));

    // HostLevelIngestionStats latency metrics
    map.put(
        IngestionOtelMetricEntity.CONSUMER_QUEUE_PUT_TIME,
        new MetricEntityExpectation(
            "ingestion.consumer_queue.put.time",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.MILLISECOND,
            "Time to put consumed records into the consumer queue",
            storeClusterVersion));
    map.put(
        IngestionOtelMetricEntity.STORAGE_ENGINE_PUT_TIME,
        new MetricEntityExpectation(
            "ingestion.storage_engine.put.time",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.MILLISECOND,
            "Time to put records into the storage engine",
            storeClusterVersion));
    map.put(
        IngestionOtelMetricEntity.STORAGE_ENGINE_DELETE_TIME,
        new MetricEntityExpectation(
            "ingestion.storage_engine.delete.time",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.MILLISECOND,
            "Time to delete records from the storage engine",
            storeClusterVersion));
    map.put(
        IngestionOtelMetricEntity.CONSUMER_ACTION_TIME,
        new MetricEntityExpectation(
            "ingestion.consumer_action.time",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.MILLISECOND,
            "Time to process a batch of consumer actions (subscribe, unsubscribe, etc.)",
            storeClusterVersion));
    map.put(
        IngestionOtelMetricEntity.LONG_RUNNING_TASK_CHECK_TIME,
        new MetricEntityExpectation(
            "ingestion.long_running_task.check.time",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.MILLISECOND,
            "Time to check long running task state",
            storeClusterVersion));
    map.put(
        IngestionOtelMetricEntity.VIEW_WRITER_PRODUCE_TIME,
        new MetricEntityExpectation(
            "ingestion.view_writer.produce.time",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.MILLISECOND,
            "Time for the view writer to trigger all writes",
            storeClusterVersion));
    map.put(
        IngestionOtelMetricEntity.VIEW_WRITER_ACK_TIME,
        new MetricEntityExpectation(
            "ingestion.view_writer.ack.time",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.MILLISECOND,
            "End-to-end time from the start of view writing to the completion of all view writer futures, including both produce and acknowledgement phases",
            storeClusterVersion));
    map.put(
        IngestionOtelMetricEntity.PRODUCER_ENQUEUE_TIME,
        new MetricEntityExpectation(
            "ingestion.producer.enqueue.time",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.MILLISECOND,
            "Time for the leader to execute the synchronous produce call that enqueues a message into the pubsub producer buffer",
            storeClusterVersion));
    map.put(
        IngestionOtelMetricEntity.PRODUCER_COMPRESS_TIME,
        new MetricEntityExpectation(
            "ingestion.producer.compress.time",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.MILLISECOND,
            "Time to compress records before producing",
            storeClusterVersion));
    map.put(
        IngestionOtelMetricEntity.PRODUCER_SYNCHRONIZE_TIME,
        new MetricEntityExpectation(
            "ingestion.producer.synchronize.time",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.MILLISECOND,
            "Time spent waiting for the last leader-produced message to be persisted during partition stop or leader handoff",
            storeClusterVersion));
    map.put(
        IngestionOtelMetricEntity.PARTIAL_UPDATE_TIME,
        new MetricEntityExpectation(
            "ingestion.partial_update.time",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.MILLISECOND,
            "Time to perform partial update operations",
            storeClusterVersionPartialUpdateOp));
    map.put(
        IngestionOtelMetricEntity.DCR_LOOKUP_TIME,
        new MetricEntityExpectation(
            "ingestion.dcr.lookup.time",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.MILLISECOND,
            "Time to look up existing records during conflict resolution",
            storeClusterVersionRecordType));
    map.put(
        IngestionOtelMetricEntity.DCR_MERGE_TIME,
        new MetricEntityExpectation(
            "ingestion.dcr.merge.time",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.MILLISECOND,
            "Time to merge records during conflict resolution",
            storeClusterVersionDcrOp));

    // HostLevelIngestionStats count metrics
    map.put(
        IngestionOtelMetricEntity.UNEXPECTED_MESSAGE_COUNT,
        new MetricEntityExpectation(
            "ingestion.unexpected_message.count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of unexpected messages encountered during ingestion",
            storeClusterVersion));
    map.put(
        IngestionOtelMetricEntity.STORE_METADATA_INCONSISTENT_COUNT,
        new MetricEntityExpectation(
            "ingestion.store_metadata.inconsistent_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of store metadata inconsistency events",
            storeClusterVersion));
    map.put(
        IngestionOtelMetricEntity.INGESTION_FAILURE_COUNT,
        new MetricEntityExpectation(
            "ingestion.failure.count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of ingestion failures by reason",
            storeClusterVersionFailureReason));
    map.put(
        IngestionOtelMetricEntity.RESUBSCRIPTION_FAILURE_COUNT,
        new MetricEntityExpectation(
            "ingestion.resubscription_failure.count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of resubscription failures during ingestion",
            storeClusterVersion));
    map.put(
        IngestionOtelMetricEntity.PARTIAL_UPDATE_CACHE_HIT_COUNT,
        new MetricEntityExpectation(
            "ingestion.partial_update.cache.hit_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of partial update cache hits",
            storeClusterVersion));
    map.put(
        IngestionOtelMetricEntity.CHECKSUM_VERIFICATION_FAILURE_COUNT,
        new MetricEntityExpectation(
            "ingestion.checksum_verification_failure.count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of checksum verification failures",
            storeClusterVersion));
    map.put(
        IngestionOtelMetricEntity.DCR_LOOKUP_CACHE_HIT_COUNT,
        new MetricEntityExpectation(
            "ingestion.dcr.lookup.cache.hit_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of cache hits when looking up existing value bytes or replication metadata before conflict resolution",
            storeClusterVersionRecordType));

    // HostLevelIngestionStats size/rate metrics
    map.put(
        IngestionOtelMetricEntity.BYTES_CONSUMED_AS_UNCOMPRESSED_SIZE,
        new MetricEntityExpectation(
            "ingestion.bytes.consumed_as_uncompressed_size",
            MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES,
            MetricUnit.BYTES,
            "Bytes consumed from pubsub as uncompressed size",
            storeClusterVersion));
    map.put(
        IngestionOtelMetricEntity.RECORD_KEY_SIZE,
        new MetricEntityExpectation(
            "ingestion.record.key_size",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.BYTES,
            "Size of record keys during ingestion",
            storeClusterVersion));
    map.put(
        IngestionOtelMetricEntity.RECORD_VALUE_SIZE,
        new MetricEntityExpectation(
            "ingestion.record.value_size",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.BYTES,
            "Size of record values during ingestion",
            storeClusterVersion));
    map.put(
        IngestionOtelMetricEntity.RECORD_ASSEMBLED_SIZE,
        new MetricEntityExpectation(
            "ingestion.record.assembled_size",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.BYTES,
            "Size of assembled records during ingestion",
            storeClusterVersionRecordType));
    map.put(
        IngestionOtelMetricEntity.RECORD_ASSEMBLED_SIZE_RATIO,
        new MetricEntityExpectation(
            "ingestion.record.assembled_size_ratio",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.RATIO,
            "Ratio of assembled record size to the max record size limit",
            storeClusterVersion));

    // HostLevelIngestionStats async gauge metric
    map.put(
        IngestionOtelMetricEntity.INGESTION_TASK_COUNT,
        new MetricEntityExpectation(
            "ingestion.task.count",
            MetricType.ASYNC_GAUGE,
            MetricUnit.NUMBER,
            "Whether an active ingestion task exists for this store version (0 or 1)",
            storeClusterVersion));

    // Global RT DIV metrics
    map.put(
        IngestionOtelMetricEntity.GLOBAL_RT_DIV_SEND_COUNT,
        new MetricEntityExpectation(
            "ingestion.global_rt_div.send_count",
            MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES,
            MetricUnit.NUMBER,
            "Count of Global RT DIV messages successfully produced to the version topic by the leader",
            storeClusterVersion));
    map.put(
        IngestionOtelMetricEntity.GLOBAL_RT_DIV_SEND_SIZE,
        new MetricEntityExpectation(
            "ingestion.global_rt_div.send_size",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.BYTES,
            "Byte size of each Global RT DIV payload produced to the version topic by the leader",
            storeClusterVersion));
    map.put(
        IngestionOtelMetricEntity.GLOBAL_RT_DIV_PERSIST_COUNT,
        new MetricEntityExpectation(
            "ingestion.global_rt_div.persist_count",
            MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES,
            MetricUnit.NUMBER,
            "Count of Global RT DIV states successfully persisted to metadata storage by the drainer",
            storeClusterVersion));
    map.put(
        IngestionOtelMetricEntity.GLOBAL_RT_DIV_VT_SYNC_COUNT,
        new MetricEntityExpectation(
            "ingestion.global_rt_div.vt_sync_count",
            MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES,
            MetricUnit.NUMBER,
            "Count of VT position syncs triggered by Global RT DIV messages",
            storeClusterVersion));
    map.put(
        IngestionOtelMetricEntity.GLOBAL_RT_DIV_ERROR_COUNT,
        new MetricEntityExpectation(
            "ingestion.global_rt_div.error_count",
            MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES,
            MetricUnit.NUMBER,
            "Count of best-effort errors in any Global RT DIV operation phase (send, persist, vt_sync, delete)",
            storeClusterVersionGlobalRtDivError));

    return map;
  }
}
