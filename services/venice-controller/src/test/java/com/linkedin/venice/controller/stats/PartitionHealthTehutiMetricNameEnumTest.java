package com.linkedin.venice.controller.stats;

import com.linkedin.venice.controller.stats.PartitionHealthStats.PartitionHealthTehutiMetricNameEnum;
import com.linkedin.venice.stats.metrics.AbstractTehutiMetricNameEnumTest;
import java.util.HashMap;
import java.util.Map;


public class PartitionHealthTehutiMetricNameEnumTest
    extends AbstractTehutiMetricNameEnumTest<PartitionHealthTehutiMetricNameEnum> {
  public PartitionHealthTehutiMetricNameEnumTest() {
    super(PartitionHealthTehutiMetricNameEnum.class);
  }

  @Override
  protected Map<PartitionHealthTehutiMetricNameEnum, String> expectedMetricNames() {
    Map<PartitionHealthTehutiMetricNameEnum, String> map = new HashMap<>();
    // Custom metric name (camelCase) to preserve backward compatibility with existing dashboards
    map.put(PartitionHealthTehutiMetricNameEnum.UNDER_REPLICATED_PARTITION, "underReplicatedPartition");
    return map;
  }
}
