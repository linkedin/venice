package com.linkedin.davinci.stats;

import com.linkedin.venice.stats.metrics.TehutiMetricNameEnumTestFixture;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class HeartbeatMonitoringTehutiMetricNameTest {
  @Test
  public void testTehutiMetricNames() {
    new TehutiMetricNameEnumTestFixture<>(HeartbeatMonitoringServiceStats.TehutiMetricName.class, expectedMetricNames())
        .assertAll();
  }

  private static Map<HeartbeatMonitoringServiceStats.TehutiMetricName, String> expectedMetricNames() {
    Map<HeartbeatMonitoringServiceStats.TehutiMetricName, String> map = new HashMap<>();
    map.put(
        HeartbeatMonitoringServiceStats.TehutiMetricName.HEARTBEAT_MONITOR_SERVICE_EXCEPTION_COUNT,
        "heartbeat-monitor-service-exception-count");
    map.put(HeartbeatMonitoringServiceStats.TehutiMetricName.HEARTBEAT_REPORTER, "heartbeat-reporter");
    map.put(HeartbeatMonitoringServiceStats.TehutiMetricName.HEARTBEAT_LOGGER, "heartbeat-logger");
    map.put(
        HeartbeatMonitoringServiceStats.TehutiMetricName.HEARTBEAT_MONITOR_VERSION_NOT_FOUND_FOR_LAG_MONITOR,
        "heartbeat-monitor-version-not-found-for-lag-monitor");
    return map;
  }
}
