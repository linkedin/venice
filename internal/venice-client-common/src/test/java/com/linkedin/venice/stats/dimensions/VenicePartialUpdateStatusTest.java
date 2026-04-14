package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class VenicePartialUpdateStatusTest {
  @Test
  public void testDimensionInterface() {
    Map<VenicePartialUpdateStatus, String> expectedValues =
        CollectionUtils.<VenicePartialUpdateStatus, String>mapBuilder()
            .put(VenicePartialUpdateStatus.PARTIAL_UPDATE_ENABLED, "partial_update_enabled")
            .put(VenicePartialUpdateStatus.PARTIAL_UPDATE_DISABLED, "partial_update_disabled")
            .build();
    new VeniceDimensionTestFixture<>(
        VenicePartialUpdateStatus.class,
        VeniceMetricsDimensions.VENICE_PARTIAL_UPDATE_STATUS,
        expectedValues).assertAll();
  }
}
