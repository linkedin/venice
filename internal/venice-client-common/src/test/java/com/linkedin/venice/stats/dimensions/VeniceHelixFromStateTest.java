package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class VeniceHelixFromStateTest {
  @Test
  public void testDimensionInterface() {
    Map<VeniceHelixFromState, String> expectedValues = CollectionUtils.<VeniceHelixFromState, String>mapBuilder()
        .put(VeniceHelixFromState.OFFLINE, "offline")
        .put(VeniceHelixFromState.STANDBY, "standby")
        .put(VeniceHelixFromState.LEADER, "leader")
        .put(VeniceHelixFromState.ERROR, "error")
        .put(VeniceHelixFromState.DROPPED, "dropped")
        .build();
    new VeniceDimensionTestFixture<>(
        VeniceHelixFromState.class,
        VeniceMetricsDimensions.VENICE_HELIX_FROM_STATE,
        expectedValues).assertAll();
  }
}
