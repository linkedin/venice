package com.linkedin.venice.controller.stats;

import com.linkedin.venice.controller.stats.AddVersionLatencyStats.AddVersionLatencyTehutiMetricNameEnum;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnumTestFixture;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class AddVersionLatencyTehutiMetricNameEnumTest {
  private static Map<AddVersionLatencyTehutiMetricNameEnum, String> expectedMetricNames() {
    Map<AddVersionLatencyTehutiMetricNameEnum, String> map = new HashMap<>();
    map.put(
        AddVersionLatencyTehutiMetricNameEnum.ADD_VERSION_RETIRE_OLD_VERSIONS_LATENCY,
        "add_version_retire_old_versions_latency");
    map.put(
        AddVersionLatencyTehutiMetricNameEnum.ADD_VERSION_RESOURCE_ASSIGNMENT_WAIT_LATENCY,
        "add_version_resource_assignment_wait_latency");
    map.put(
        AddVersionLatencyTehutiMetricNameEnum.ADD_VERSION_CREATION_FAILURE_LATENCY,
        "add_version_creation_failure_latency");
    map.put(
        AddVersionLatencyTehutiMetricNameEnum.ADD_VERSION_EXISTING_SOURCE_HANDLING_LATENCY,
        "add_version_existing_source_handling_latency");
    map.put(
        AddVersionLatencyTehutiMetricNameEnum.ADD_VERSION_START_OF_PUSH_LATENCY,
        "add_version_start_of_push_latency");
    map.put(
        AddVersionLatencyTehutiMetricNameEnum.ADD_VERSION_BATCH_TOPIC_CREATION_LATENCY,
        "add_version_batch_topic_creation_latency");
    map.put(
        AddVersionLatencyTehutiMetricNameEnum.ADD_VERSION_HELIX_RESOURCE_CREATION_LATENCY,
        "add_version_helix_resource_creation_latency");
    return map;
  }

  @Test
  public void testTehutiMetricNames() {
    new TehutiMetricNameEnumTestFixture<>(AddVersionLatencyTehutiMetricNameEnum.class, expectedMetricNames())
        .assertAll();
  }
}
