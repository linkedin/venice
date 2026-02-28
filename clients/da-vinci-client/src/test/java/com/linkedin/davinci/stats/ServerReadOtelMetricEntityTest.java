package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.ServerReadOtelMetricEntity.READ_CALL_COUNT;
import static com.linkedin.davinci.stats.ServerReadOtelMetricEntity.READ_CALL_TIME;
import static com.linkedin.davinci.stats.ServerReadOtelMetricEntity.READ_REQUEST_KEY_COUNT;
import static com.linkedin.davinci.stats.ServerReadOtelMetricEntity.READ_REQUEST_KEY_SIZE;
import static com.linkedin.davinci.stats.ServerReadOtelMetricEntity.READ_REQUEST_SIZE;
import static com.linkedin.davinci.stats.ServerReadOtelMetricEntity.READ_RESPONSE_FLUSH_TIME;
import static com.linkedin.davinci.stats.ServerReadOtelMetricEntity.READ_RESPONSE_KEY_NOT_FOUND_COUNT;
import static com.linkedin.davinci.stats.ServerReadOtelMetricEntity.READ_RESPONSE_SIZE;
import static com.linkedin.davinci.stats.ServerReadOtelMetricEntity.READ_RESPONSE_VALUE_SIZE;
import static com.linkedin.davinci.stats.ServerReadOtelMetricEntity.STORAGE_ENGINE_COMPUTE_OPERATION_COUNT;
import static com.linkedin.davinci.stats.ServerReadOtelMetricEntity.STORAGE_ENGINE_QUERY_CHUNKED_VALUE_COUNT;
import static com.linkedin.davinci.stats.ServerReadOtelMetricEntity.STORAGE_ENGINE_QUERY_DESERIALIZATION_TIME;
import static com.linkedin.davinci.stats.ServerReadOtelMetricEntity.STORAGE_ENGINE_QUERY_SERIALIZATION_TIME;
import static com.linkedin.davinci.stats.ServerReadOtelMetricEntity.STORAGE_ENGINE_QUERY_TIME;
import static com.linkedin.davinci.stats.ServerReadOtelMetricEntity.STORAGE_ENGINE_QUEUE_SIZE;
import static com.linkedin.davinci.stats.ServerReadOtelMetricEntity.STORAGE_ENGINE_QUEUE_WAIT_TIME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CHUNKING_STATUS;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_READ_COMPUTE_OPERATION_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_METHOD;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.Utils.setOf;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import java.util.Set;
import org.testng.annotations.Test;


public class ServerReadOtelMetricEntityTest {
  @Test
  public void testMetricEntityCount() {
    assertEquals(ServerReadOtelMetricEntity.values().length, 16, "Expected 16 metric entities");
  }

  @Test
  public void testMetricEntityDefinitions() {
    Set<VeniceMetricsDimensions> storeClusterRequestType =
        setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_REQUEST_METHOD);

    Set<VeniceMetricsDimensions> storeClusterRequestTypeHttpStatus = setOf(
        VENICE_STORE_NAME,
        VENICE_CLUSTER_NAME,
        VENICE_REQUEST_METHOD,
        HTTP_RESPONSE_STATUS_CODE,
        HTTP_RESPONSE_STATUS_CODE_CATEGORY,
        VENICE_RESPONSE_STATUS_CODE_CATEGORY);

    Set<VeniceMetricsDimensions> storeClusterRequestTypeChunking =
        setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_REQUEST_METHOD, VENICE_CHUNKING_STATUS);

    Set<VeniceMetricsDimensions> storeClusterComputeOp =
        setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_READ_COMPUTE_OPERATION_TYPE);

    // 4-enum metrics (HTTP status dims)
    assertMetricEntity(
        READ_CALL_TIME.getMetricEntity(),
        "read.call_time",
        MetricType.HISTOGRAM,
        MetricUnit.MILLISECOND,
        "Server-side read request latency",
        storeClusterRequestTypeHttpStatus);
    assertMetricEntity(
        READ_CALL_COUNT.getMetricEntity(),
        "read.call_count",
        MetricType.COUNTER,
        MetricUnit.NUMBER,
        "Server-side read request count",
        storeClusterRequestTypeHttpStatus);
    assertMetricEntity(
        READ_RESPONSE_SIZE.getMetricEntity(),
        "read.response.size",
        MetricType.HISTOGRAM,
        MetricUnit.BYTES,
        "Size of the read response payload",
        storeClusterRequestTypeHttpStatus);
    assertMetricEntity(
        READ_RESPONSE_VALUE_SIZE.getMetricEntity(),
        "read.response.value_size",
        MetricType.HISTOGRAM,
        MetricUnit.BYTES,
        "Size of values in a read response",
        storeClusterRequestTypeHttpStatus);

    // 1-enum metric (chunking status)
    assertMetricEntity(
        STORAGE_ENGINE_QUERY_TIME.getMetricEntity(),
        "storage_engine.query.time",
        MetricType.HISTOGRAM,
        MetricUnit.MILLISECOND,
        "Storage engine query latency",
        storeClusterRequestTypeChunking);

    // base metrics (request type only)
    assertMetricEntity(
        READ_REQUEST_KEY_COUNT.getMetricEntity(),
        "read.request.key_count",
        MetricType.HISTOGRAM,
        MetricUnit.NUMBER,
        "Number of keys in a read request",
        storeClusterRequestType);
    assertMetricEntity(
        READ_REQUEST_SIZE.getMetricEntity(),
        "read.request.size",
        MetricType.HISTOGRAM,
        MetricUnit.BYTES,
        "Size of the read request payload",
        storeClusterRequestType);
    assertMetricEntity(
        READ_REQUEST_KEY_SIZE.getMetricEntity(),
        "read.request.key_size",
        MetricType.HISTOGRAM,
        MetricUnit.BYTES,
        "Size of keys in a read request",
        storeClusterRequestType);
    assertMetricEntity(
        STORAGE_ENGINE_QUEUE_WAIT_TIME.getMetricEntity(),
        "storage_engine.queue.wait_time",
        MetricType.HISTOGRAM,
        MetricUnit.MILLISECOND,
        "Time spent waiting in the storage execution handler queue",
        storeClusterRequestType);
    assertMetricEntity(
        STORAGE_ENGINE_QUEUE_SIZE.getMetricEntity(),
        "storage_engine.queue.size",
        MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
        MetricUnit.NUMBER,
        "Number of pending tasks in the storage execution handler queue",
        storeClusterRequestType);
    assertMetricEntity(
        STORAGE_ENGINE_QUERY_DESERIALIZATION_TIME.getMetricEntity(),
        "storage_engine.query.deserialization_time",
        MetricType.HISTOGRAM,
        MetricUnit.MILLISECOND,
        "Time spent deserializing values for read-compute operations",
        storeClusterRequestTypeChunking);
    assertMetricEntity(
        STORAGE_ENGINE_QUERY_SERIALIZATION_TIME.getMetricEntity(),
        "storage_engine.query.serialization_time",
        MetricType.HISTOGRAM,
        MetricUnit.MILLISECOND,
        "Time spent serializing results for read-compute operations",
        storeClusterRequestType);
    assertMetricEntity(
        READ_RESPONSE_FLUSH_TIME.getMetricEntity(),
        "read.response.flush_time",
        MetricType.HISTOGRAM,
        MetricUnit.MILLISECOND,
        "Time spent flushing the read response to the network",
        storeClusterRequestType);
    assertMetricEntity(
        READ_RESPONSE_KEY_NOT_FOUND_COUNT.getMetricEntity(),
        "read.response.key.not_found_count",
        MetricType.COUNTER,
        MetricUnit.NUMBER,
        "Count of keys not found during read operations",
        storeClusterRequestType);
    assertMetricEntity(
        STORAGE_ENGINE_QUERY_CHUNKED_VALUE_COUNT.getMetricEntity(),
        "storage_engine.query.chunked_value_count",
        MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
        MetricUnit.NUMBER,
        "Per-request count of values requiring multi-chunk large value reassembly in the storage engine",
        storeClusterRequestType);

    // 1-enum metric (compute op type)
    assertMetricEntity(
        STORAGE_ENGINE_COMPUTE_OPERATION_COUNT.getMetricEntity(),
        "storage_engine.read.compute.operation_count",
        MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
        MetricUnit.NUMBER,
        "Count of read-compute operations by operation type",
        storeClusterComputeOp);
  }

  private static void assertMetricEntity(
      MetricEntity entity,
      String expectedName,
      MetricType expectedType,
      MetricUnit expectedUnit,
      String expectedDescription,
      Set<VeniceMetricsDimensions> expectedDimensions) {
    assertEquals(entity.getMetricName(), expectedName, "metric name mismatch");
    assertEquals(entity.getMetricType(), expectedType, expectedName + " metric type mismatch");
    assertEquals(entity.getUnit(), expectedUnit, expectedName + " unit mismatch");
    assertEquals(entity.getDescription(), expectedDescription, expectedName + " description mismatch");
    assertEquals(entity.getDimensionsList(), expectedDimensions, expectedName + " dimensions mismatch");
  }
}
