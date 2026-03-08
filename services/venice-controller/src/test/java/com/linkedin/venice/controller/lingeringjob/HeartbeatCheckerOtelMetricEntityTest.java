package com.linkedin.venice.controller.lingeringjob;

import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture.MetricEntityExpectation;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class HeartbeatCheckerOtelMetricEntityTest {
  private static Map<HeartbeatCheckerOtelMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<HeartbeatCheckerOtelMetricEntity, MetricEntityExpectation> map = new HashMap<>();
    map.put(
        HeartbeatCheckerOtelMetricEntity.BATCH_JOB_HEARTBEAT_CHECK_FAILURE_COUNT,
        new MetricEntityExpectation(
            "batch_job_heartbeat.check_failure_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Failed heartbeat check operations",
            Collections.emptySet()));
    map.put(
        HeartbeatCheckerOtelMetricEntity.BATCH_JOB_HEARTBEAT_TIMEOUT_COUNT,
        new MetricEntityExpectation(
            "batch_job_heartbeat.timeout_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Batch jobs timed out based on heartbeat",
            Collections.emptySet()));
    map.put(
        HeartbeatCheckerOtelMetricEntity.BATCH_JOB_HEARTBEAT_ACTIVE_COUNT,
        new MetricEntityExpectation(
            "batch_job_heartbeat.active_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Batch jobs with valid heartbeat",
            Collections.emptySet()));
    return map;
  }

  @Test
  public void testMetricEntities() {
    new ModuleMetricEntityTestFixture<>(HeartbeatCheckerOtelMetricEntity.class, expectedDefinitions()).assertAll();
  }
}
