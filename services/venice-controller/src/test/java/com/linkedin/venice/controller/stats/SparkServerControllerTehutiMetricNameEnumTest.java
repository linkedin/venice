package com.linkedin.venice.controller.stats;

import com.linkedin.venice.controller.stats.SparkServerStats.ControllerTehutiMetricNameEnum;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnumTestFixture;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class SparkServerControllerTehutiMetricNameEnumTest {
  private static Map<ControllerTehutiMetricNameEnum, String> expectedMetricNames() {
    Map<ControllerTehutiMetricNameEnum, String> map = new HashMap<>();
    map.put(ControllerTehutiMetricNameEnum.REQUEST, "request");
    map.put(ControllerTehutiMetricNameEnum.FINISHED_REQUEST, "finished_request");
    map.put(ControllerTehutiMetricNameEnum.CURRENT_IN_FLIGHT_REQUEST, "current_in_flight_request");
    map.put(ControllerTehutiMetricNameEnum.SUCCESSFUL_REQUEST, "successful_request");
    map.put(ControllerTehutiMetricNameEnum.FAILED_REQUEST, "failed_request");
    map.put(ControllerTehutiMetricNameEnum.SUCCESSFUL_REQUEST_LATENCY, "successful_request_latency");
    map.put(ControllerTehutiMetricNameEnum.FAILED_REQUEST_LATENCY, "failed_request_latency");
    return map;
  }

  @Test
  public void testTehutiMetricNames() {
    new TehutiMetricNameEnumTestFixture<>(ControllerTehutiMetricNameEnum.class, expectedMetricNames()).assertAll();
  }
}
