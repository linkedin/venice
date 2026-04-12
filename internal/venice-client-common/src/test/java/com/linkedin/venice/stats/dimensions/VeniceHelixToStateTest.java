package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class VeniceHelixToStateTest {
  @Test
  public void testDimensionInterface() {
    Map<VeniceHelixToState, String> expectedValues = CollectionUtils.<VeniceHelixToState, String>mapBuilder()
        .put(VeniceHelixToState.OFFLINE, "offline")
        .put(VeniceHelixToState.STANDBY, "standby")
        .put(VeniceHelixToState.LEADER, "leader")
        .put(VeniceHelixToState.ERROR, "error")
        .put(VeniceHelixToState.DROPPED, "dropped")
        .build();
    new VeniceDimensionTestFixture<>(
        VeniceHelixToState.class,
        VeniceMetricsDimensions.VENICE_HELIX_TO_STATE,
        expectedValues).assertAll();
  }
}
