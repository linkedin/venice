package com.linkedin.venice.controller.lingeringjob;

import com.linkedin.venice.controller.lingeringjob.HeartbeatBasedCheckerStats.HeartbeatCheckerTehutiMetricNameEnum;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnumTestFixture;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class HeartbeatCheckerTehutiMetricNameEnumTest {
  private static Map<HeartbeatCheckerTehutiMetricNameEnum, String> expectedMetricNames() {
    Map<HeartbeatCheckerTehutiMetricNameEnum, String> map = new HashMap<>();
    map.put(HeartbeatCheckerTehutiMetricNameEnum.CHECK_JOB_HAS_HEARTBEAT_FAILED, "check_job_has_heartbeat_failed");
    map.put(HeartbeatCheckerTehutiMetricNameEnum.TIMEOUT_HEARTBEAT_CHECK, "timeout_heartbeat_check");
    map.put(HeartbeatCheckerTehutiMetricNameEnum.NON_TIMEOUT_HEARTBEAT_CHECK, "non_timeout_heartbeat_check");
    return map;
  }

  @Test
  public void testTehutiMetricNames() {
    new TehutiMetricNameEnumTestFixture<>(HeartbeatCheckerTehutiMetricNameEnum.class, expectedMetricNames())
        .assertAll();
  }
}
