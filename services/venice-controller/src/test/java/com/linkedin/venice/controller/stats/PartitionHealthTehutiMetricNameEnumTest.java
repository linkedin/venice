package com.linkedin.venice.controller.stats;

import com.linkedin.venice.controller.stats.PartitionHealthStats.PartitionHealthTehutiMetricNameEnum;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnumTestFixture;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class PartitionHealthTehutiMetricNameEnumTest {
  private static Map<PartitionHealthTehutiMetricNameEnum, String> expectedMetricNames() {
    Map<PartitionHealthTehutiMetricNameEnum, String> map = new HashMap<>();
    // Custom metric name (camelCase) to preserve backward compatibility with existing dashboards
    map.put(PartitionHealthTehutiMetricNameEnum.UNDER_REPLICATED_PARTITION, "underReplicatedPartition");
    return map;
  }

  @Test
  public void testTehutiMetricNames() {
    new TehutiMetricNameEnumTestFixture<>(PartitionHealthTehutiMetricNameEnum.class, expectedMetricNames()).assertAll();
  }
}
