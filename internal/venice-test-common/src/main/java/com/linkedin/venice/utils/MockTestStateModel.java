package com.linkedin.venice.utils;

import com.linkedin.venice.helix.HelixState;
import org.apache.helix.HelixDefinedState;
import org.apache.helix.model.StateModelDefinition;


public class MockTestStateModel {
  public static final String UNIT_TEST_STATE_MODEL = "MockTestStateModel";

  public static StateModelDefinition getDefinition() {

    StateModelDefinition.Builder builder = new StateModelDefinition.Builder(UNIT_TEST_STATE_MODEL);

    builder.addState(HelixState.ONLINE.toString(), 1)
        .addState(HelixState.OFFLINE.toString())
        .addState(HelixState.BOOTSTRAP.toString())
        .addState(HelixState.DROPPED.toString())
        .initialState(HelixState.OFFLINE.toString())
        .addTransition(HelixState.OFFLINE.toString(), HelixState.BOOTSTRAP.toString())
        .addTransition(HelixState.BOOTSTRAP.toString(), HelixState.ONLINE.toString())
        .addTransition(HelixState.ONLINE.toString(), HelixState.OFFLINE.toString())
        .addTransition(HelixState.OFFLINE.toString(), HelixDefinedState.DROPPED.toString())
        .dynamicUpperBound(HelixState.ONLINE.toString(), "R");

    return builder.build();
  }
}
