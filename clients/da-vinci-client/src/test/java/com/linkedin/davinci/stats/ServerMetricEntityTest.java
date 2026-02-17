package com.linkedin.davinci.stats;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.utils.Utils;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class ServerMetricEntityTest {
  @Test
  public void testServerMetricEntities() {
    Map<ServerMetricEntity, MetricEntity> expectedMetrics = new HashMap<>();
    expectedMetrics.put(
        ServerMetricEntity.INGESTION_HEARTBEAT_DELAY,
        new MetricEntity(
            "ingestion.replication.heartbeat.delay",
            MetricType.HISTOGRAM,
            MetricUnit.MILLISECOND,
            "Nearline ingestion replication lag",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_STORE_NAME,
                VeniceMetricsDimensions.VENICE_CLUSTER_NAME,
                VeniceMetricsDimensions.VENICE_REGION_NAME,
                VeniceMetricsDimensions.VENICE_VERSION_ROLE,
                VeniceMetricsDimensions.VENICE_REPLICA_TYPE,
                VeniceMetricsDimensions.VENICE_REPLICA_STATE)));

    expectedMetrics.put(
        ServerMetricEntity.INGESTION_RECORD_DELAY,
        new MetricEntity(
            "ingestion.replication.record.delay",
            MetricType.HISTOGRAM,
            MetricUnit.MILLISECOND,
            "Nearline ingestion record-level replication lag",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_STORE_NAME,
                VeniceMetricsDimensions.VENICE_CLUSTER_NAME,
                VeniceMetricsDimensions.VENICE_REGION_NAME,
                VeniceMetricsDimensions.VENICE_VERSION_ROLE,
                VeniceMetricsDimensions.VENICE_REPLICA_TYPE,
                VeniceMetricsDimensions.VENICE_REPLICA_STATE)));
    
    expectedMetrics.put(
        ServerMetricEntity.INGESTION_TASK_ERROR_COUNT,
        new MetricEntity(
            "ingestion.task.error_count",
            MetricType.ASYNC_GAUGE,
            MetricUnit.NUMBER,
            "Count of ingestion tasks in error state",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_STORE_NAME,
                VeniceMetricsDimensions.VENICE_CLUSTER_NAME,
                VeniceMetricsDimensions.VENICE_VERSION_ROLE)));

    expectedMetrics.put(
        ServerMetricEntity.INGESTION_TASK_PUSH_TIMEOUT_COUNT,
        new MetricEntity(
            "ingestion.task.push_timeout_count",
            MetricType.ASYNC_GAUGE,
            MetricUnit.NUMBER,
            "Count of ingestion tasks timed out during push operation",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_STORE_NAME,
                VeniceMetricsDimensions.VENICE_CLUSTER_NAME,
                VeniceMetricsDimensions.VENICE_VERSION_ROLE)));

    expectedMetrics.put(
        ServerMetricEntity.WRITE_COMPUTE_OPERATION_FAILURE_CODE,
        new MetricEntity(
            "ingestion.write_compute.operation.failure_code",
            MetricType.ASYNC_GAUGE,
            MetricUnit.NUMBER,
            "Write-compute operation failure code",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_STORE_NAME,
                VeniceMetricsDimensions.VENICE_CLUSTER_NAME,
                VeniceMetricsDimensions.VENICE_VERSION_ROLE)));

    expectedMetrics.put(
        ServerMetricEntity.DISK_QUOTA_USED,
        new MetricEntity(
            "disk.quota.used",
            MetricType.ASYNC_GAUGE,
            MetricUnit.RATIO,
            "Disk quota used for the store version",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_STORE_NAME,
                VeniceMetricsDimensions.VENICE_CLUSTER_NAME,
                VeniceMetricsDimensions.VENICE_VERSION_ROLE)));

    expectedMetrics.put(
        ServerMetricEntity.INGESTION_RECORDS_CONSUMED,
        new MetricEntity(
            "ingestion.records.consumed",
            MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES,
            MetricUnit.NUMBER,
            "Records consumed from remote/local topic",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_STORE_NAME,
                VeniceMetricsDimensions.VENICE_CLUSTER_NAME,
                VeniceMetricsDimensions.VENICE_VERSION_ROLE,
                VeniceMetricsDimensions.VENICE_REPLICA_TYPE)));

    expectedMetrics.put(
        ServerMetricEntity.INGESTION_RECORDS_PRODUCED,
        new MetricEntity(
            "ingestion.records.produced",
            MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES,
            MetricUnit.NUMBER,
            "Records produced to local topic",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_STORE_NAME,
                VeniceMetricsDimensions.VENICE_CLUSTER_NAME,
                VeniceMetricsDimensions.VENICE_VERSION_ROLE,
                VeniceMetricsDimensions.VENICE_REPLICA_TYPE)));

    expectedMetrics.put(
        ServerMetricEntity.INGESTION_BYTES_CONSUMED,
        new MetricEntity(
            "ingestion.bytes.consumed",
            MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES,
            MetricUnit.BYTES,
            "Bytes consumed from remote/local topic that are successfully processed excluding control/DIV messages",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_STORE_NAME,
                VeniceMetricsDimensions.VENICE_CLUSTER_NAME,
                VeniceMetricsDimensions.VENICE_VERSION_ROLE,
                VeniceMetricsDimensions.VENICE_REPLICA_TYPE)));

    expectedMetrics.put(
        ServerMetricEntity.INGESTION_BYTES_PRODUCED,
        new MetricEntity(
            "ingestion.bytes.produced",
            MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES,
            MetricUnit.BYTES,
            "Bytes produced to local topic",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_STORE_NAME,
                VeniceMetricsDimensions.VENICE_CLUSTER_NAME,
                VeniceMetricsDimensions.VENICE_VERSION_ROLE,
                VeniceMetricsDimensions.VENICE_REPLICA_TYPE)));

    expectedMetrics.put(
        ServerMetricEntity.INGESTION_SUBSCRIBE_PREP_TIME,
        new MetricEntity(
            "ingestion.subscribe.prep.time",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.MILLISECOND,
            "Subscription preparation latency",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_STORE_NAME,
                VeniceMetricsDimensions.VENICE_CLUSTER_NAME,
                VeniceMetricsDimensions.VENICE_VERSION_ROLE)));

    expectedMetrics.put(
        ServerMetricEntity.INGESTION_TIME,
        new MetricEntity(
            "ingestion.time",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.MILLISECOND,
            "End-to-end processing time from topic poll to storage write",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_STORE_NAME,
                VeniceMetricsDimensions.VENICE_CLUSTER_NAME,
                VeniceMetricsDimensions.VENICE_VERSION_ROLE)));

    expectedMetrics.put(
        ServerMetricEntity.INGESTION_PRODUCER_CALLBACK_TIME,
        new MetricEntity(
            "ingestion.producer.callback.time",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.MILLISECOND,
            "Leader producer callback latency (ack wait time)",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_STORE_NAME,
                VeniceMetricsDimensions.VENICE_CLUSTER_NAME,
                VeniceMetricsDimensions.VENICE_VERSION_ROLE,
                VeniceMetricsDimensions.VENICE_REPLICA_TYPE)));

    expectedMetrics.put(
        ServerMetricEntity.INGESTION_PREPROCESSING_LEADER_TIME,
        new MetricEntity(
            "ingestion.preprocessing.leader.time",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.MILLISECOND,
            "Preprocessing latency during ingestion",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_STORE_NAME,
                VeniceMetricsDimensions.VENICE_CLUSTER_NAME,
                VeniceMetricsDimensions.VENICE_VERSION_ROLE)));

    expectedMetrics.put(
        ServerMetricEntity.INGESTION_PREPROCESSING_INTERNAL_TIME,
        new MetricEntity(
            "ingestion.preprocessing.internal.time",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.MILLISECOND,
            "Internal preprocessing latency during ingestion",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_STORE_NAME,
                VeniceMetricsDimensions.VENICE_CLUSTER_NAME,
                VeniceMetricsDimensions.VENICE_VERSION_ROLE)));

    expectedMetrics.put(
        ServerMetricEntity.INGESTION_TIME_BETWEEN_COMPONENTS,
        new MetricEntity(
            "ingestion.time_between_components",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.MILLISECOND,
            "Ingestion latency between different components of the flow",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_STORE_NAME,
                VeniceMetricsDimensions.VENICE_CLUSTER_NAME,
                VeniceMetricsDimensions.VENICE_VERSION_ROLE,
                VeniceMetricsDimensions.VENICE_INGESTION_SOURCE_COMPONENT,
                VeniceMetricsDimensions.VENICE_INGESTION_DESTINATION_COMPONENT)));

    expectedMetrics.put(
        ServerMetricEntity.INGESTION_PRODUCER_TIME,
        new MetricEntity(
            "ingestion.producer.time",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.MILLISECOND,
            "Latency from leader producing to producer completion",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_STORE_NAME,
                VeniceMetricsDimensions.VENICE_CLUSTER_NAME,
                VeniceMetricsDimensions.VENICE_VERSION_ROLE)));

    expectedMetrics.put(
        ServerMetricEntity.BATCH_PROCESSING_REQUEST_COUNT,
        new MetricEntity(
            "ingestion.batch_processing.request.count",
            MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES,
            MetricUnit.NUMBER,
            "Batch processing requests",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_STORE_NAME,
                VeniceMetricsDimensions.VENICE_CLUSTER_NAME,
                VeniceMetricsDimensions.VENICE_VERSION_ROLE)));

    expectedMetrics.put(
        ServerMetricEntity.BATCH_PROCESSING_REQUEST_RECORD_COUNT,
        new MetricEntity(
            "ingestion.batch_processing.request.record.count",
            MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES,
            MetricUnit.NUMBER,
            "Total records across batch-processing requests",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_STORE_NAME,
                VeniceMetricsDimensions.VENICE_CLUSTER_NAME,
                VeniceMetricsDimensions.VENICE_VERSION_ROLE)));

    expectedMetrics.put(
        ServerMetricEntity.BATCH_PROCESSING_REQUEST_ERROR_COUNT,
        new MetricEntity(
            "ingestion.batch_processing.request.error_count",
            MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES,
            MetricUnit.NUMBER,
            "Failed batch processing requests",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_STORE_NAME,
                VeniceMetricsDimensions.VENICE_CLUSTER_NAME,
                VeniceMetricsDimensions.VENICE_VERSION_ROLE)));

    expectedMetrics.put(
        ServerMetricEntity.BATCH_PROCESSING_REQUEST_TIME,
        new MetricEntity(
            "ingestion.batch_processing.request.time",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.MILLISECOND,
            "Batch processing latency",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_STORE_NAME,
                VeniceMetricsDimensions.VENICE_CLUSTER_NAME,
                VeniceMetricsDimensions.VENICE_VERSION_ROLE)));

    expectedMetrics.put(
        ServerMetricEntity.DCR_EVENT_COUNT,
        new MetricEntity(
            "ingestion.dcr.event_count",
            MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES,
            MetricUnit.NUMBER,
            "Deterministic Conflict Resolution (DCR) count specific to certain events",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_STORE_NAME,
                VeniceMetricsDimensions.VENICE_CLUSTER_NAME,
                VeniceMetricsDimensions.VENICE_VERSION_ROLE,
                VeniceMetricsDimensions.VENICE_DCR_EVENT)));

    expectedMetrics.put(
        ServerMetricEntity.DCR_TOTAL_COUNT,
        new MetricEntity(
            "ingestion.dcr.total_count",
            MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES,
            MetricUnit.NUMBER,
            "Deterministic Conflict Resolution (DCR) total count",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_STORE_NAME,
                VeniceMetricsDimensions.VENICE_CLUSTER_NAME,
                VeniceMetricsDimensions.VENICE_VERSION_ROLE)));

    expectedMetrics.put(
        ServerMetricEntity.CONSUMER_IDLE_TIME,
        new MetricEntity(
            "ingestion.consumer.idle_time",
            MetricType.ASYNC_GAUGE,
            MetricUnit.MILLISECOND,
            "Consumer idle time",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_STORE_NAME,
                VeniceMetricsDimensions.VENICE_CLUSTER_NAME,
                VeniceMetricsDimensions.VENICE_VERSION_ROLE)));

    expectedMetrics.put(
        ServerMetricEntity.DUPLICATE_KEY_UPDATE_COUNT,
        new MetricEntity(
            "ingestion.key.update.duplicate_count",
            MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES,
            MetricUnit.NUMBER,
            "Count of duplicate-key updates during ingestion",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_STORE_NAME,
                VeniceMetricsDimensions.VENICE_CLUSTER_NAME,
                VeniceMetricsDimensions.VENICE_VERSION_ROLE)));

    expectedMetrics.put(
        ServerMetricEntity.RT_RECORDS_CONSUMED,
        new MetricEntity(
            "ingestion.records.consumed_from_real_time_topic",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Records consumed from local/remote region real-time topics",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_STORE_NAME,
                VeniceMetricsDimensions.VENICE_CLUSTER_NAME,
                VeniceMetricsDimensions.VENICE_VERSION_ROLE,
                VeniceMetricsDimensions.VENICE_SOURCE_REGION,
                VeniceMetricsDimensions.VENICE_DESTINATION_REGION,
                VeniceMetricsDimensions.VENICE_REGION_LOCALITY)));

    expectedMetrics.put(
        ServerMetricEntity.RT_BYTES_CONSUMED,
        new MetricEntity(
            "ingestion.bytes.consumed_from_real_time_topic",
            MetricType.COUNTER,
            MetricUnit.BYTES,
            "Bytes consumed from local/remote region real-time topics",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_STORE_NAME,
                VeniceMetricsDimensions.VENICE_CLUSTER_NAME,
                VeniceMetricsDimensions.VENICE_VERSION_ROLE,
                VeniceMetricsDimensions.VENICE_SOURCE_REGION,
                VeniceMetricsDimensions.VENICE_DESTINATION_REGION,
                VeniceMetricsDimensions.VENICE_REGION_LOCALITY)));

    for (ServerMetricEntity metric: ServerMetricEntity.values()) {
      MetricEntity actual = metric.getMetricEntity();
      MetricEntity expected = expectedMetrics.get(metric);

      assertNotNull(expected, "No expected definition for " + metric.name());
      assertNotNull(actual.getMetricName(), "Metric name should not be null for " + metric.name());
      assertEquals(actual.getMetricName(), expected.getMetricName(), "Unexpected metric name for " + metric.name());
      assertNotNull(actual.getMetricType(), "Metric type should not be null for " + metric.name());
      assertEquals(actual.getMetricType(), expected.getMetricType(), "Unexpected metric type for " + metric.name());
      assertNotNull(actual.getUnit(), "Metric unit should not be null for " + metric.name());
      assertEquals(actual.getUnit(), expected.getUnit(), "Unexpected metric unit for " + metric.name());
      assertNotNull(actual.getDescription(), "Metric description should not be null for " + metric.name());
      assertEquals(
          actual.getDescription(),
          expected.getDescription(),
          "Unexpected metric description for " + metric.name());
      assertNotNull(actual.getDimensionsList(), "Metric dimensions should not be null for " + metric.name());
      assertEquals(
          actual.getDimensionsList(),
          expected.getDimensionsList(),
          "Unexpected metric dimensions for " + metric.name());
    }
  }

  @Test
  public void testGetMetricEntityNotNull() {
    // Verify that getMetricEntity() never returns null for any enum value
    for (ServerMetricEntity entity: ServerMetricEntity.values()) {
      assertNotNull(entity.getMetricEntity(), "getMetricEntity() should not return null for " + entity.name());
    }
  }
}
