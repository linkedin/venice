package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.controller.stats.ErrorPartitionStats.ErrorPartitionOtelMetricEntity;
import com.linkedin.venice.stats.metrics.AbstractModuleMetricEntityTest;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import java.util.HashMap;
import java.util.Map;


public class ErrorPartitionOtelMetricEntityTest extends AbstractModuleMetricEntityTest<ErrorPartitionOtelMetricEntity> {
  public ErrorPartitionOtelMetricEntityTest() {
    super(ErrorPartitionOtelMetricEntity.class);
  }

  @Override
  protected Map<ErrorPartitionOtelMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<ErrorPartitionOtelMetricEntity, MetricEntityExpectation> map = new HashMap<>();
    map.put(
        ErrorPartitionOtelMetricEntity.ERROR_PARTITION_RESET_ATTEMPT_COUNT,
        new MetricEntityExpectation(
            "partition.error_partition.reset.attempt_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Total partitions reset across all operations",
            setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME)));
    map.put(
        ErrorPartitionOtelMetricEntity.ERROR_PARTITION_RESET_ERROR_COUNT,
        new MetricEntityExpectation(
            "partition.error_partition.reset.error_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Failed reset operations for a store",
            setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME)));
    map.put(
        ErrorPartitionOtelMetricEntity.ERROR_PARTITION_RESET_RECOVERED_PARTITION_COUNT,
        new MetricEntityExpectation(
            "partition.error_partition.reset.recovered_partition_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Partitions recovered after reset",
            setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME)));
    map.put(
        ErrorPartitionOtelMetricEntity.ERROR_PARTITION_RESET_UNRECOVERABLE_PARTITION_COUNT,
        new MetricEntityExpectation(
            "partition.error_partition.reset.unrecoverable_partition_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Partitions declared unrecoverable after hitting reset limit",
            setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME)));
    map.put(
        ErrorPartitionOtelMetricEntity.ERROR_PARTITION_PROCESSING_ERROR_COUNT,
        new MetricEntityExpectation(
            "partition.error_partition.processing.error_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Unexpected failures in the error partition processing cycle",
            setOf(VENICE_CLUSTER_NAME)));
    map.put(
        ErrorPartitionOtelMetricEntity.ERROR_PARTITION_PROCESSING_TIME,
        new MetricEntityExpectation(
            "partition.error_partition.processing.time",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.MILLISECOND,
            "Time for each complete error partition processing cycle",
            setOf(VENICE_CLUSTER_NAME)));
    return map;
  }
}
