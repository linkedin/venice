package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.helix.HelixState;
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

  /**
   * Guards against drift between {@link HelixState} and this enum: every Helix state that can
   * appear as a transition destination must be representable here so the defensive
   * {@code valueOf} catch in {@code ParticipantStateTransitionStats#recordInProgressOtel} stays
   * unreachable in practice. {@link HelixState#UNKNOWN} is excluded — Helix never dispatches
   * transitions from/to it.
   */
  @Test
  public void testCoversAllHelixStatesExceptUnknown() {
    for (HelixState helixState: HelixState.values()) {
      if (helixState == HelixState.UNKNOWN) {
        continue;
      }
      VeniceHelixToState.valueOf(helixState.name());
    }
  }
}
