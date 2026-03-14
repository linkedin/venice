package com.linkedin.venice.stats;

import com.linkedin.venice.stats.metrics.TehutiMetricNameEnumTestFixture;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class ServerConnectionTehutiMetricNameTest {
  @Test
  public void testTehutiMetricNames() {
    new TehutiMetricNameEnumTestFixture<>(ServerConnectionStats.TehutiMetricName.class, expectedMetricNames())
        .assertAll();
  }

  private static Map<ServerConnectionStats.TehutiMetricName, String> expectedMetricNames() {
    Map<ServerConnectionStats.TehutiMetricName, String> map = new HashMap<>();
    map.put(ServerConnectionStats.TehutiMetricName.ROUTER_CONNECTION_REQUEST, "router_connection_request");
    map.put(ServerConnectionStats.TehutiMetricName.CLIENT_CONNECTION_REQUEST, "client_connection_request");
    map.put(ServerConnectionStats.TehutiMetricName.NEW_CONNECTION_SETUP_LATENCY, "new_connection_setup_latency");
    return map;
  }
}
