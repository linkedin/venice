package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.helix.HelixState;
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

  /**
   * Guards against drift between {@link HelixState} and this enum: every Helix state that can
   * appear as a transition source must be representable here so the defensive {@code valueOf}
   * catch in {@code ParticipantStateTransitionStats#recordInProgressOtel} stays unreachable in
   * practice. {@link HelixState#UNKNOWN} is excluded — Helix never dispatches transitions
   * from/to it.
   */
  @Test
  public void testCoversAllHelixStatesExceptUnknown() {
    for (HelixState helixState: HelixState.values()) {
      if (helixState == HelixState.UNKNOWN) {
        continue;
      }
      VeniceHelixFromState.valueOf(helixState.name());
    }
  }
}
