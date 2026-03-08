package com.linkedin.venice.stats;

import com.linkedin.venice.stats.metrics.AbstractTehutiMetricNameEnumTest;
import java.util.HashMap;
import java.util.Map;


public class ThreadPoolTehutiMetricNameEnumTest
    extends AbstractTehutiMetricNameEnumTest<ThreadPoolStats.ThreadPoolTehutiMetricNameEnum> {
  public ThreadPoolTehutiMetricNameEnumTest() {
    super(ThreadPoolStats.ThreadPoolTehutiMetricNameEnum.class);
  }

  @Override
  protected Map<ThreadPoolStats.ThreadPoolTehutiMetricNameEnum, String> expectedMetricNames() {
    Map<ThreadPoolStats.ThreadPoolTehutiMetricNameEnum, String> map = new HashMap<>();
    map.put(ThreadPoolStats.ThreadPoolTehutiMetricNameEnum.QUEUED_TASK_COUNT, "queued_task_count");
    return map;
  }
}
