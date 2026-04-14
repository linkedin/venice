package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class VeniceWriteComputeStatusTest {
  @Test
  public void testDimensionInterface() {
    Map<VeniceWriteComputeStatus, String> expectedValues =
        CollectionUtils.<VeniceWriteComputeStatus, String>mapBuilder()
            .put(VeniceWriteComputeStatus.WRITE_COMPUTE_ENABLED, "write_compute_enabled")
            .put(VeniceWriteComputeStatus.WRITE_COMPUTE_DISABLED, "write_compute_disabled")
            .build();
    new VeniceDimensionTestFixture<>(
        VeniceWriteComputeStatus.class,
        VeniceMetricsDimensions.VENICE_WRITE_COMPUTE_STATUS,
        expectedValues).assertAll();
  }
}
