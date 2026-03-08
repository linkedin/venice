package com.linkedin.venice.controller.stats;

import com.linkedin.venice.controller.stats.DisabledPartitionStats.DisabledPartitionTehutiMetricNameEnum;
import com.linkedin.venice.stats.metrics.AbstractTehutiMetricNameEnumTest;
import java.util.HashMap;
import java.util.Map;


public class DisabledPartitionTehutiMetricNameEnumTest
    extends AbstractTehutiMetricNameEnumTest<DisabledPartitionTehutiMetricNameEnum> {
  public DisabledPartitionTehutiMetricNameEnumTest() {
    super(DisabledPartitionTehutiMetricNameEnum.class);
  }

  @Override
  protected Map<DisabledPartitionTehutiMetricNameEnum, String> expectedMetricNames() {
    Map<DisabledPartitionTehutiMetricNameEnum, String> map = new HashMap<>();
    map.put(DisabledPartitionTehutiMetricNameEnum.DISABLED_PARTITION_COUNT, "disabled_partition_count");
    return map;
  }
}
