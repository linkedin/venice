package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class VeniceAdaptiveThrottlerTypeTest {
  @Test
  public void testDimensionInterface() {
    Map<VeniceAdaptiveThrottlerType, String> expectedValues =
        CollectionUtils.<VeniceAdaptiveThrottlerType, String>mapBuilder()
            .put(VeniceAdaptiveThrottlerType.PUBSUB_CONSUMPTION_RECORDS_COUNT, "pubsub_consumption_records_count")
            .put(VeniceAdaptiveThrottlerType.PUBSUB_CONSUMPTION_BANDWIDTH, "pubsub_consumption_bandwidth")
            .put(
                VeniceAdaptiveThrottlerType.CURRENT_VERSION_AA_WC_LEADER_RECORDS_COUNT,
                "current_version_aa_wc_leader_records_count")
            .put(
                VeniceAdaptiveThrottlerType.CURRENT_VERSION_NON_AA_WC_LEADER_RECORDS_COUNT,
                "current_version_non_aa_wc_leader_records_count")
            .put(
                VeniceAdaptiveThrottlerType.NON_CURRENT_VERSION_AA_WC_LEADER_RECORDS_COUNT,
                "non_current_version_aa_wc_leader_records_count")
            .put(
                VeniceAdaptiveThrottlerType.NON_CURRENT_VERSION_NON_AA_WC_LEADER_RECORDS_COUNT,
                "non_current_version_non_aa_wc_leader_records_count")
            .build();
    new VeniceDimensionTestFixture<>(
        VeniceAdaptiveThrottlerType.class,
        VeniceMetricsDimensions.VENICE_ADAPTIVE_THROTTLER_TYPE,
        expectedValues).assertAll();
  }
}
