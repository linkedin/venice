package com.linkedin.venice.stats;

import com.linkedin.venice.stats.metrics.TehutiMetricNameEnumTestFixture;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class ThreadPoolTehutiMetricNameEnumTest {
  private static Map<ThreadPoolStats.ThreadPoolTehutiMetricNameEnum, String> expectedMetricNames() {
    Map<ThreadPoolStats.ThreadPoolTehutiMetricNameEnum, String> map = new HashMap<>();
    map.put(ThreadPoolStats.ThreadPoolTehutiMetricNameEnum.ACTIVE_THREAD_COUNT, "active_thread_count");
    map.put(ThreadPoolStats.ThreadPoolTehutiMetricNameEnum.QUEUED_TASK_COUNT, "queued_task_count");
    map.put(ThreadPoolStats.ThreadPoolTehutiMetricNameEnum.REJECTED_TASK_COUNT, "rejected_task_count");
    map.put(ThreadPoolStats.ThreadPoolTehutiMetricNameEnum.CALLER_RUN_COUNT, "caller_run_count");
    map.put(ThreadPoolStats.ThreadPoolTehutiMetricNameEnum.CALLER_RUN_TIME, "caller_run_time");
    return map;
  }

  @Test
  public void testTehutiMetricNames() {
    new TehutiMetricNameEnumTestFixture<>(ThreadPoolStats.ThreadPoolTehutiMetricNameEnum.class, expectedMetricNames())
        .assertAll();
  }
}
