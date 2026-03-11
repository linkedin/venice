package com.linkedin.davinci.stats;

import static org.testng.Assert.assertEquals;

import com.linkedin.venice.stats.dimensions.VeniceAdaptiveThrottlerType;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnumTestFixture;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class AdaptiveThrottlingTehutiMetricNameTest {
  @Test
  public void testTehutiMetricNames() {
    new TehutiMetricNameEnumTestFixture<>(AdaptiveThrottlingServiceStats.TehutiMetricName.class, expectedMetricNames())
        .assertAll();
  }

  /**
   * Validates that every {@link VeniceAdaptiveThrottlerType} is handled by
   * {@code AdaptiveThrottlingServiceStats} — the constructor eagerly creates a metric state
   * for each type. If the two enums fall out of sync, this test catches it at test time.
   */
  @Test
  public void testEnumParityWithVeniceAdaptiveThrottlerType() {
    assertEquals(
        AdaptiveThrottlingServiceStats.TehutiMetricName.values().length,
        VeniceAdaptiveThrottlerType.values().length,
        "TehutiMetricName and VeniceAdaptiveThrottlerType must have the same number of constants");
  }

  private static Map<AdaptiveThrottlingServiceStats.TehutiMetricName, String> expectedMetricNames() {
    Map<AdaptiveThrottlingServiceStats.TehutiMetricName, String> map = new HashMap<>();
    map.put(
        AdaptiveThrottlingServiceStats.TehutiMetricName.KAFKA_CONSUMPTION_RECORDS_COUNT,
        "kafka_consumption_records_count");
    map.put(AdaptiveThrottlingServiceStats.TehutiMetricName.KAFKA_CONSUMPTION_BANDWIDTH, "kafka_consumption_bandwidth");
    map.put(
        AdaptiveThrottlingServiceStats.TehutiMetricName.CURRENT_VERSION_AA_WC_LEADER_RECORDS_COUNT,
        "current_version_aa_wc_leader_records_count");
    map.put(
        AdaptiveThrottlingServiceStats.TehutiMetricName.CURRENT_VERSION_NON_AA_WC_LEADER_RECORDS_COUNT,
        "current_version_non_aa_wc_leader_records_count");
    map.put(
        AdaptiveThrottlingServiceStats.TehutiMetricName.NON_CURRENT_VERSION_AA_WC_LEADER_RECORDS_COUNT,
        "non_current_version_aa_wc_leader_records_count");
    map.put(
        AdaptiveThrottlingServiceStats.TehutiMetricName.NON_CURRENT_VERSION_NON_AA_WC_LEADER_RECORDS_COUNT,
        "non_current_version_non_aa_wc_leader_records_count");
    return map;
  }
}
