package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.HeartbeatMonitoringOtelMetricEntity.HEARTBEAT_MONITORING_EXCEPTION_COUNT;
import static com.linkedin.davinci.stats.HeartbeatMonitoringOtelMetricEntity.HEARTBEAT_MONITORING_HEARTBEAT_COUNT;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_HEARTBEAT_COMPONENT;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture.MetricEntityExpectation;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class HeartbeatMonitoringOtelMetricEntityTest {
  @Test
  public void testMetricEntities() {
    new ModuleMetricEntityTestFixture<>(HeartbeatMonitoringOtelMetricEntity.class, expectedDefinitions()).assertAll();
  }

  private static Map<HeartbeatMonitoringOtelMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<HeartbeatMonitoringOtelMetricEntity, MetricEntityExpectation> map = new HashMap<>();
    map.put(
        HEARTBEAT_MONITORING_EXCEPTION_COUNT,
        new MetricEntityExpectation(
            "ingestion.heartbeat_monitoring.exception_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Number of exceptions caught in the heartbeat monitoring service threads",
            setOf(VENICE_CLUSTER_NAME, VENICE_HEARTBEAT_COMPONENT)));
    map.put(
        HEARTBEAT_MONITORING_HEARTBEAT_COUNT,
        new MetricEntityExpectation(
            "ingestion.heartbeat_monitoring.heartbeat_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Liveness count for the heartbeat monitoring service threads",
            setOf(VENICE_CLUSTER_NAME, VENICE_HEARTBEAT_COMPONENT)));
    return map;
  }
}
