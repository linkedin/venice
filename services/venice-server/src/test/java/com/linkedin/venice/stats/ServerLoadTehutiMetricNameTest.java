package com.linkedin.venice.stats;

import com.linkedin.venice.stats.metrics.TehutiMetricNameEnumTestFixture;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class ServerLoadTehutiMetricNameTest {
  @Test
  public void testTehutiMetricNames() {
    new TehutiMetricNameEnumTestFixture<>(ServerLoadStats.TehutiMetricName.class, expectedMetricNames()).assertAll();
  }

  private static Map<ServerLoadStats.TehutiMetricName, String> expectedMetricNames() {
    Map<ServerLoadStats.TehutiMetricName, String> map = new HashMap<>();
    map.put(ServerLoadStats.TehutiMetricName.REJECTED_REQUEST, "rejected_request");
    map.put(ServerLoadStats.TehutiMetricName.ACCEPTED_REQUEST, "accepted_request");
    map.put(ServerLoadStats.TehutiMetricName.REJECTION_RATIO, "rejection_ratio");
    return map;
  }
}
