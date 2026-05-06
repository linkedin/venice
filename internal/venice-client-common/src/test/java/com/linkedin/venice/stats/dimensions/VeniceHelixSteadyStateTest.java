package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class VeniceHelixSteadyStateTest {
  @Test
  public void testDimensionInterface() {
    Map<VeniceHelixSteadyState, String> expectedValues = CollectionUtils.<VeniceHelixSteadyState, String>mapBuilder()
        .put(VeniceHelixSteadyState.ERROR, "error")
        .put(VeniceHelixSteadyState.STANDBY, "standby")
        .put(VeniceHelixSteadyState.LEADER, "leader")
        .build();
    new VeniceDimensionTestFixture<>(
        VeniceHelixSteadyState.class,
        VeniceMetricsDimensions.VENICE_HELIX_STATE,
        expectedValues).assertAll();
  }
}
