package com.linkedin.venice.stats;

import com.linkedin.venice.stats.metrics.TehutiMetricNameEnumTestFixture;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class ThreadPoolTehutiMetricNameEnumTest {
  private static Map<ThreadPoolStats.ThreadPoolTehutiMetricNameEnum, String> expectedMetricNames() {
    Map<ThreadPoolStats.ThreadPoolTehutiMetricNameEnum, String> map = new HashMap<>();
    map.put(ThreadPoolStats.ThreadPoolTehutiMetricNameEnum.QUEUED_TASK_COUNT, "queued_task_count");
    return map;
  }

  @Test
  public void testTehutiMetricNames() {
    new TehutiMetricNameEnumTestFixture<>(ThreadPoolStats.ThreadPoolTehutiMetricNameEnum.class, expectedMetricNames())
        .assertAll();
  }
}
