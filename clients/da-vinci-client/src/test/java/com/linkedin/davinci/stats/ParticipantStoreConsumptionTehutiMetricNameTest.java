package com.linkedin.davinci.stats;

import com.linkedin.venice.stats.metrics.TehutiMetricNameEnumTestFixture;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class ParticipantStoreConsumptionTehutiMetricNameTest {
  @Test
  public void testTehutiMetricNames() {
    new TehutiMetricNameEnumTestFixture<>(
        ParticipantStoreConsumptionStats.TehutiMetricName.class,
        expectedMetricNames()).assertAll();
  }

  private static Map<ParticipantStoreConsumptionStats.TehutiMetricName, String> expectedMetricNames() {
    Map<ParticipantStoreConsumptionStats.TehutiMetricName, String> map = new HashMap<>();
    map.put(ParticipantStoreConsumptionStats.TehutiMetricName.KILL_PUSH_JOB_LATENCY, "kill_push_job_latency");
    map.put(ParticipantStoreConsumptionStats.TehutiMetricName.KILLED_PUSH_JOBS, "killed_push_jobs");
    map.put(ParticipantStoreConsumptionStats.TehutiMetricName.FAILED_INITIALIZATION, "failed_initialization");
    map.put(
        ParticipantStoreConsumptionStats.TehutiMetricName.KILL_PUSH_JOB_FAILED_CONSUMPTION,
        "kill_push_job_failed_consumption");
    map.put(ParticipantStoreConsumptionStats.TehutiMetricName.HEARTBEAT, "heartbeat");
    return map;
  }
}
