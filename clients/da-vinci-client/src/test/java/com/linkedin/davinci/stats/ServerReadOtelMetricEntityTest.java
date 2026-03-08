package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CHUNKING_STATUS;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_READ_COMPUTE_OPERATION_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_METHOD;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.AbstractModuleMetricEntityTest;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


public class ServerReadOtelMetricEntityTest extends AbstractModuleMetricEntityTest<ServerReadOtelMetricEntity> {
  public ServerReadOtelMetricEntityTest() {
    super(ServerReadOtelMetricEntity.class);
  }

  @Override
  protected Map<ServerReadOtelMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<ServerReadOtelMetricEntity, MetricEntityExpectation> map = new HashMap<>();

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
    Set<VeniceMetricsDimensions> storeCluster = setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME);
    Set<VeniceMetricsDimensions> storeClusterChunking =
        setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_CHUNKING_STATUS);
    Set<VeniceMetricsDimensions> storeClusterComputeOp =
        setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_READ_COMPUTE_OPERATION_TYPE);

    map.put(
        ServerReadOtelMetricEntity.READ_CALL_TIME,
        new MetricEntityExpectation(
            "read.call_time",
            MetricType.HISTOGRAM,
            MetricUnit.MILLISECOND,
            "Server-side read request latency",
            storeClusterRequestTypeHttpStatus));
    map.put(
        ServerReadOtelMetricEntity.READ_CALL_COUNT,
        new MetricEntityExpectation(
            "read.call_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Server-side read request count",
            storeClusterRequestTypeHttpStatus));
    map.put(
        ServerReadOtelMetricEntity.READ_REQUEST_KEY_COUNT,
        new MetricEntityExpectation(
            "read.request.key_count",
            MetricType.HISTOGRAM,
            MetricUnit.NUMBER,
            "Number of keys in a read request",
            storeClusterRequestType));
    map.put(
        ServerReadOtelMetricEntity.READ_REQUEST_SIZE,
        new MetricEntityExpectation(
            "read.request.size",
            MetricType.HISTOGRAM,
            MetricUnit.BYTES,
            "Size of the read request payload",
            storeClusterRequestType));
    map.put(
        ServerReadOtelMetricEntity.READ_RESPONSE_SIZE,
        new MetricEntityExpectation(
            "read.response.size",
            MetricType.HISTOGRAM,
            MetricUnit.BYTES,
            "Size of the read response payload",
            storeClusterRequestTypeHttpStatus));
    map.put(
        ServerReadOtelMetricEntity.READ_REQUEST_KEY_SIZE,
        new MetricEntityExpectation(
            "read.request.key_size",
            MetricType.HISTOGRAM,
            MetricUnit.BYTES,
            "Size of keys in a read request",
            storeClusterRequestType));
    map.put(
        ServerReadOtelMetricEntity.READ_RESPONSE_VALUE_SIZE,
        new MetricEntityExpectation(
            "read.response.value_size",
            MetricType.HISTOGRAM,
            MetricUnit.BYTES,
            "Size of values in a read response",
            storeClusterRequestTypeHttpStatus));
    map.put(
        ServerReadOtelMetricEntity.STORAGE_ENGINE_QUERY_TIME,
        new MetricEntityExpectation(
            "storage_engine.query.time",
            MetricType.HISTOGRAM,
            MetricUnit.MILLISECOND,
            "Storage engine query latency",
            storeClusterRequestTypeChunking));
    map.put(
        ServerReadOtelMetricEntity.STORAGE_ENGINE_QUERY_CHUNKED_VALUE_COUNT,
        new MetricEntityExpectation(
            "storage_engine.query.chunked_value_count",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.NUMBER,
            "Per-request count of values requiring multi-chunk large value reassembly in the storage engine",
            storeClusterRequestType));
    map.put(
        ServerReadOtelMetricEntity.STORAGE_ENGINE_QUEUE_WAIT_TIME,
        new MetricEntityExpectation(
            "storage_engine.queue.wait_time",
            MetricType.HISTOGRAM,
            MetricUnit.MILLISECOND,
            "Time spent waiting in the storage execution handler queue",
            storeClusterRequestType));
    map.put(
        ServerReadOtelMetricEntity.STORAGE_ENGINE_QUEUE_SIZE,
        new MetricEntityExpectation(
            "storage_engine.queue.size",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.NUMBER,
            "Number of pending tasks in the storage execution handler queue",
            storeClusterRequestType));
    map.put(
        ServerReadOtelMetricEntity.STORAGE_ENGINE_READ_COMPUTE_DESERIALIZATION_TIME,
        new MetricEntityExpectation(
            "storage_engine.read.compute.deserialization_time",
            MetricType.HISTOGRAM,
            MetricUnit.MILLISECOND,
            "Time spent deserializing values for read-compute operations",
            storeClusterChunking));
    map.put(
        ServerReadOtelMetricEntity.STORAGE_ENGINE_READ_COMPUTE_SERIALIZATION_TIME,
        new MetricEntityExpectation(
            "storage_engine.read.compute.serialization_time",
            MetricType.HISTOGRAM,
            MetricUnit.MILLISECOND,
            "Time spent serializing results for read-compute operations",
            storeCluster));
    map.put(
        ServerReadOtelMetricEntity.STORAGE_ENGINE_READ_COMPUTE_EXECUTION_COUNT,
        new MetricEntityExpectation(
            "storage_engine.read.compute.execution_count",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.NUMBER,
            "Count of read-compute executions by operation type",
            storeClusterComputeOp));
    map.put(
        ServerReadOtelMetricEntity.STORAGE_ENGINE_READ_COMPUTE_EXECUTION_TIME,
        new MetricEntityExpectation(
            "storage_engine.read.compute.execution_time",
            MetricType.HISTOGRAM,
            MetricUnit.MILLISECOND,
            "Time spent executing read-compute operations",
            storeCluster));
    map.put(
        ServerReadOtelMetricEntity.READ_RESPONSE_FLUSH_TIME,
        new MetricEntityExpectation(
            "read.response.flush_time",
            MetricType.HISTOGRAM,
            MetricUnit.MILLISECOND,
            "Time spent flushing the read response to the network",
            storeClusterRequestType));
    map.put(
        ServerReadOtelMetricEntity.READ_RESPONSE_KEY_NOT_FOUND_COUNT,
        new MetricEntityExpectation(
            "read.response.key.not_found_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of keys not found during read operations",
            storeClusterRequestType));

    return map;
  }
}
