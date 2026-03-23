package com.linkedin.davinci.stats;

import com.linkedin.venice.stats.metrics.TehutiMetricNameEnumTestFixture;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class StoreBufferServiceTehutiMetricNameTest {
  @Test
  public void testTehutiMetricNames() {
    new TehutiMetricNameEnumTestFixture<>(StoreBufferServiceStats.TehutiMetricName.class, expectedMetricNames())
        .assertAll();
  }

  private static Map<StoreBufferServiceStats.TehutiMetricName, String> expectedMetricNames() {
    Map<StoreBufferServiceStats.TehutiMetricName, String> map = new HashMap<>();
    map.put(StoreBufferServiceStats.TehutiMetricName.TOTAL_MEMORY_USAGE, "total_memory_usage");
    map.put(StoreBufferServiceStats.TehutiMetricName.TOTAL_REMAINING_MEMORY, "total_remaining_memory");
    map.put(StoreBufferServiceStats.TehutiMetricName.MAX_MEMORY_USAGE_PER_WRITER, "max_memory_usage_per_writer");
    map.put(StoreBufferServiceStats.TehutiMetricName.MIN_MEMORY_USAGE_PER_WRITER, "min_memory_usage_per_writer");
    map.put(StoreBufferServiceStats.TehutiMetricName.INTERNAL_PROCESSING_LATENCY, "internal_processing_latency");
    map.put(StoreBufferServiceStats.TehutiMetricName.INTERNAL_PROCESSING_ERROR, "internal_processing_error");
    return map;
  }
}
