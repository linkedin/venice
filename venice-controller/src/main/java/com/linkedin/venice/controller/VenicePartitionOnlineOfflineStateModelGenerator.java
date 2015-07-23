package com.linkedin.venice.controller;

import org.apache.helix.model.StateModelDefinition;


/**
 * Venice Partition state model generator describes the transition for segment states.
 *
 * Online -> Offline
 * Offline -> Online
 * Offline -> Dropped
 */
public class VenicePartitionOnlineOfflineStateModelGenerator {

  public static final String PARTITION_ONLINE_OFFLINE_STATE_MODEL = "PartitionOnlineOfflineModel";

  public static final String ONLINE_STATE = "ONLINE";
  public static final String OFFLINE_STATE = "OFFLINE";
  public static final String DROPPED_STATE = "DROPPED";

  public static StateModelDefinition generatePartitionStateModelDefinition() {

    StateModelDefinition.Builder builder = new StateModelDefinition.Builder(PARTITION_ONLINE_OFFLINE_STATE_MODEL);

    // States and their priority in which we want the instances to be in.
    builder.addState(ONLINE_STATE, 1);
    builder.addState(OFFLINE_STATE, 2);
    builder.addState(DROPPED_STATE, 3);

    builder.initialState(OFFLINE_STATE);

    // Valid transitions between the states.
    builder.addTransition(OFFLINE_STATE, ONLINE_STATE);
    builder.addTransition(ONLINE_STATE, OFFLINE_STATE);
    builder.addTransition(OFFLINE_STATE, DROPPED_STATE);

    // States constraints
    /*
     * Dynamic constraint: R means it should be derived based on the replication factor for the cluster
     * this allows a different replication factor for each resource without having to define a new state model.
     */
    builder.dynamicUpperBound(ONLINE_STATE, "R");

    return builder.build();
  }
}
