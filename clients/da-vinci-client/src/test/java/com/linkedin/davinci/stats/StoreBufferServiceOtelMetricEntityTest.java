package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_DRAINER_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture.MetricEntityExpectation;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class StoreBufferServiceOtelMetricEntityTest {
  @Test
  public void testMetricEntities() {
    new ModuleMetricEntityTestFixture<>(StoreBufferServiceOtelMetricEntity.class, expectedDefinitions()).assertAll();
  }

  private static Map<StoreBufferServiceOtelMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<StoreBufferServiceOtelMetricEntity, MetricEntityExpectation> map = new HashMap<>();
    map.put(
        StoreBufferServiceOtelMetricEntity.MEMORY_USED,
        new MetricEntityExpectation(
            "drainer.memory.used",
            MetricType.ASYNC_GAUGE,
            MetricUnit.BYTES,
            "Total memory used across all drainer queues",
            setOf(VENICE_CLUSTER_NAME, VENICE_DRAINER_TYPE)));
    map.put(
        StoreBufferServiceOtelMetricEntity.MEMORY_REMAINING,
        new MetricEntityExpectation(
            "drainer.memory.remaining",
            MetricType.ASYNC_GAUGE,
            MetricUnit.BYTES,
            "Total remaining memory capacity across all drainer queues",
            setOf(VENICE_CLUSTER_NAME, VENICE_DRAINER_TYPE)));
    map.put(
        StoreBufferServiceOtelMetricEntity.MEMORY_USED_PER_WRITER_MAX,
        new MetricEntityExpectation(
            "drainer.writer.memory.max_used",
            MetricType.ASYNC_GAUGE,
            MetricUnit.BYTES,
            "Maximum memory used by any single drainer writer",
            setOf(VENICE_CLUSTER_NAME, VENICE_DRAINER_TYPE)));
    map.put(
        StoreBufferServiceOtelMetricEntity.MEMORY_USED_PER_WRITER_MIN,
        new MetricEntityExpectation(
            "drainer.writer.memory.min_used",
            MetricType.ASYNC_GAUGE,
            MetricUnit.BYTES,
            "Minimum memory used by any single drainer writer",
            setOf(VENICE_CLUSTER_NAME, VENICE_DRAINER_TYPE)));
    map.put(
        StoreBufferServiceOtelMetricEntity.PROCESSING_TIME,
        new MetricEntityExpectation(
            "drainer.record.processing.time",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.MILLISECOND,
            "Time spent processing each record in the drainer",
            setOf(VENICE_CLUSTER_NAME, VENICE_DRAINER_TYPE, VENICE_STORE_NAME)));
    map.put(
        StoreBufferServiceOtelMetricEntity.PROCESSING_ERROR_COUNT,
        new MetricEntityExpectation(
            "drainer.record.processing.error_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of errors encountered while processing records in the drainer",
            setOf(VENICE_CLUSTER_NAME, VENICE_DRAINER_TYPE, VENICE_STORE_NAME)));
    return map;
  }
}
