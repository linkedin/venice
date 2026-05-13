package com.linkedin.davinci.stats;

import com.linkedin.venice.stats.metrics.TehutiMetricNameEnumTestFixture;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class StuckConsumerRepairTehutiMetricNameTest {
  @Test
  public void testTehutiMetricNames() {
    new TehutiMetricNameEnumTestFixture<>(StuckConsumerRepairStats.TehutiMetricName.class, expectedMetricNames())
        .assertAll();
  }

  private static Map<StuckConsumerRepairStats.TehutiMetricName, String> expectedMetricNames() {
    Map<StuckConsumerRepairStats.TehutiMetricName, String> map = new HashMap<>();
    map.put(StuckConsumerRepairStats.TehutiMetricName.STUCK_CONSUMER_FOUND, "stuck_consumer_found");
    map.put(StuckConsumerRepairStats.TehutiMetricName.INGESTION_TASK_REPAIR, "ingestion_task_repair");
    map.put(StuckConsumerRepairStats.TehutiMetricName.REPAIR_FAILURE, "repair_failure");
    return map;
  }
}
