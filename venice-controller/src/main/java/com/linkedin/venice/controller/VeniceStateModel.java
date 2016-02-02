package com.linkedin.venice.controller;

import org.apache.helix.HelixDefinedState;
import org.apache.helix.model.StateModelDefinition;
import com.linkedin.venice.helix.State;


/**
 * Venice Partition state model generator describes the transition for segment states.
 *
 * Online -> Offline
 * Offline -> Online
 * Offline -> Dropped
 */
public class VeniceStateModel {

  public static final String PARTITION_ONLINE_OFFLINE_STATE_MODEL = "PartitionOnlineOfflineModel";

  private static final int PRIORITY_HIGHEST = 1;

  private static final String UPPER_BOUND_REPLICATION_FACTOR = "R";

  public static StateModelDefinition getDefinition() {

    StateModelDefinition.Builder builder = new StateModelDefinition.Builder(PARTITION_ONLINE_OFFLINE_STATE_MODEL);

    // States and their priority in which we want the instances to be in.
    builder.addState(State.ONLINE_STATE, PRIORITY_HIGHEST);
    builder.addState(State.OFFLINE_STATE);
    builder.addState(HelixDefinedState.DROPPED.toString());

    builder.initialState(State.OFFLINE_STATE);

    // Valid transitions between the states.
    builder.addTransition(State.OFFLINE_STATE, State.ONLINE_STATE);
    builder.addTransition(State.ONLINE_STATE, State.OFFLINE_STATE);
    builder.addTransition(State.OFFLINE_STATE, HelixDefinedState.DROPPED.toString());

    // States constraints
    /*
     * Dynamic constraint: R means it should be derived based on the replication factor for the cluster
     * this allows a different replication factor for each resource without having to define a new state model.
     */
    builder.dynamicUpperBound(State.ONLINE_STATE, UPPER_BOUND_REPLICATION_FACTOR);

    return builder.build();
  }
}
