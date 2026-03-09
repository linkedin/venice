package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_ADAPTIVE_THROTTLER_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture.MetricEntityExpectation;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class AdaptiveThrottlingOtelMetricEntityTest {
  @Test
  public void testMetricEntities() {
    new ModuleMetricEntityTestFixture<>(AdaptiveThrottlingOtelMetricEntity.class, expectedDefinitions()).assertAll();
  }

  private static Map<AdaptiveThrottlingOtelMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<AdaptiveThrottlingOtelMetricEntity, MetricEntityExpectation> map = new HashMap<>();
    map.put(
        AdaptiveThrottlingOtelMetricEntity.RECORD_COUNT,
        new MetricEntityExpectation(
            "adaptive_throttler.record_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of records observed by each adaptive record-count ingestion throttler",
            setOf(VENICE_CLUSTER_NAME, VENICE_ADAPTIVE_THROTTLER_TYPE)));
    map.put(
        AdaptiveThrottlingOtelMetricEntity.BYTE_COUNT,
        new MetricEntityExpectation(
            "adaptive_throttler.byte_count",
            MetricType.COUNTER,
            MetricUnit.BYTES,
            "Count of bytes observed by the adaptive bandwidth ingestion throttler",
            setOf(VENICE_CLUSTER_NAME, VENICE_ADAPTIVE_THROTTLER_TYPE)));
    return map;
  }
}
