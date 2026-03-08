package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class RejectionReasonTest {
  @Test
  public void testDimensionInterface() {
    Map<RejectionReason, String> expectedValues = CollectionUtils.<RejectionReason, String>mapBuilder()
        .put(RejectionReason.THROTTLED_BY_LOAD_CONTROLLER, "throttled_by_load_controller")
        .put(RejectionReason.NO_REPLICAS_AVAILABLE, "no_replicas_available")
        .build();
    new VeniceDimensionTestFixture<>(
        RejectionReason.class,
        VeniceMetricsDimensions.VENICE_REQUEST_REJECTION_REASON,
        expectedValues).assertAll();
  }
}
