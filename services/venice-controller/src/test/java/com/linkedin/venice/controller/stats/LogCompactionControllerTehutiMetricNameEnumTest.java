package com.linkedin.venice.controller.stats;

import com.linkedin.venice.controller.stats.LogCompactionStats.ControllerTehutiMetricNameEnum;
import com.linkedin.venice.stats.metrics.AbstractTehutiMetricNameEnumTest;
import java.util.HashMap;
import java.util.Map;


public class LogCompactionControllerTehutiMetricNameEnumTest
    extends AbstractTehutiMetricNameEnumTest<ControllerTehutiMetricNameEnum> {
  public LogCompactionControllerTehutiMetricNameEnumTest() {
    super(ControllerTehutiMetricNameEnum.class);
  }

  @Override
  protected Map<ControllerTehutiMetricNameEnum, String> expectedMetricNames() {
    Map<ControllerTehutiMetricNameEnum, String> map = new HashMap<>();
    map.put(ControllerTehutiMetricNameEnum.REPUSH_CALL_COUNT, "repush_call_count");
    map.put(ControllerTehutiMetricNameEnum.COMPACTION_ELIGIBLE_STATE, "compaction_eligible_state");
    map.put(
        ControllerTehutiMetricNameEnum.STORE_NOMINATED_FOR_COMPACTION_COUNT,
        "store_nominated_for_compaction_count");
    map.put(ControllerTehutiMetricNameEnum.STORE_COMPACTION_TRIGGERED_COUNT, "store_compaction_triggered_count");
    return map;
  }
}
