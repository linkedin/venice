package com.linkedin.venice.controller.stats;

import com.linkedin.venice.controller.stats.DisabledPartitionStats.DisabledPartitionTehutiMetricNameEnum;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnumTestFixture;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class DisabledPartitionTehutiMetricNameEnumTest {
  private static Map<DisabledPartitionTehutiMetricNameEnum, String> expectedMetricNames() {
    Map<DisabledPartitionTehutiMetricNameEnum, String> map = new HashMap<>();
    map.put(DisabledPartitionTehutiMetricNameEnum.DISABLED_PARTITION_COUNT, "disabled_partition_count");
    return map;
  }

  @Test
  public void testTehutiMetricNames() {
    new TehutiMetricNameEnumTestFixture<>(DisabledPartitionTehutiMetricNameEnum.class, expectedMetricNames())
        .assertAll();
  }
}
