package com.linkedin.venice.stats.routing;

import com.linkedin.venice.stats.metrics.TehutiMetricNameEnumTestFixture;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class HelixGroupTehutiMetricNameTest {
  private static Map<HelixGroupStats.HelixGroupTehutiMetricName, String> expectedMetricNames() {
    Map<HelixGroupStats.HelixGroupTehutiMetricName, String> map = new HashMap<>();
    map.put(HelixGroupStats.HelixGroupTehutiMetricName.GROUP_COUNT, "group_count");
    map.put(HelixGroupStats.HelixGroupTehutiMetricName.GROUP_REQUEST, "group_request");
    map.put(HelixGroupStats.HelixGroupTehutiMetricName.GROUP_PENDING_REQUEST, "group_pending_request");
    map.put(HelixGroupStats.HelixGroupTehutiMetricName.GROUP_RESPONSE_WAITING_TIME, "group_response_waiting_time");
    return map;
  }

  @Test
  public void testTehutiMetricNames() {
    new TehutiMetricNameEnumTestFixture<>(HelixGroupStats.HelixGroupTehutiMetricName.class, expectedMetricNames())
        .assertAll();
  }
}
