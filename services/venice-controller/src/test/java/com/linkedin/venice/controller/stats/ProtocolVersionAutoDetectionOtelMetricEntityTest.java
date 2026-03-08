package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.controller.stats.ProtocolVersionAutoDetectionStats.ProtocolVersionAutoDetectionOtelMetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture.MetricEntityExpectation;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class ProtocolVersionAutoDetectionOtelMetricEntityTest {
  private static Map<ProtocolVersionAutoDetectionOtelMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<ProtocolVersionAutoDetectionOtelMetricEntity, MetricEntityExpectation> map = new HashMap<>();
    map.put(
        ProtocolVersionAutoDetectionOtelMetricEntity.PROTOCOL_VERSION_AUTO_DETECTION_FAILURE_COUNT,
        new MetricEntityExpectation(
            "protocol_version_auto_detection.consecutive_failure_count",
            MetricType.GAUGE,
            MetricUnit.NUMBER,
            "Consecutive failures in protocol version auto-detection",
            setOf(VENICE_CLUSTER_NAME)));
    map.put(
        ProtocolVersionAutoDetectionOtelMetricEntity.PROTOCOL_VERSION_AUTO_DETECTION_TIME,
        new MetricEntityExpectation(
            "protocol_version_auto_detection.time",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.MILLISECOND,
            "Latency of protocol version auto-detection",
            setOf(VENICE_CLUSTER_NAME)));
    return map;
  }

  @Test
  public void testMetricEntities() {
    new ModuleMetricEntityTestFixture<>(ProtocolVersionAutoDetectionOtelMetricEntity.class, expectedDefinitions())
        .assertAll();
  }
}
