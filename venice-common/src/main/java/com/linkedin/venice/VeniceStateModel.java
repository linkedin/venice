package com.linkedin.venice;

import com.linkedin.venice.helix.HelixState;
import org.apache.helix.HelixDefinedState;
import org.apache.helix.model.StateModelDefinition;


/**
 * Venice Partition state model generator describes the transition for segment states.
 *
 * Online -> Offline
 * Offline -> Bootstrap
 * Bootstrap -> Online
 * Offline -> Dropped
 */
public class VeniceStateModel {

  public static final String PARTITION_ONLINE_OFFLINE_STATE_MODEL = "PartitionOnlineOfflineModel";

  public static final String PARTITION_LEADER_FOLLOWER_STATE_MODEL= "LeaderStandby";

  private static final String UPPER_BOUND_REPLICATION_FACTOR = "R";

  public static StateModelDefinition getDefinition() {

    StateModelDefinition.Builder builder = new StateModelDefinition.Builder(PARTITION_ONLINE_OFFLINE_STATE_MODEL);

    /**
     * States and their priority in which we want the instances to be in.
     *
     * The implication is that transitioning to a state which has a higher number is
     * considered a "downward" transition by Helix. Downward transitions are not
     * throttled.
     *
     * Therefore, with the priority scheme outlined below, and the valid transitions
     * defined further below, we allow the following transitions to occur unthrottled:
     *
     * ONLINE -> OFFLINE
     * BOOTSTRAP -> OFFLINE
     * ONLINE -> ERROR
     * BOOTSTRAP -> ERROR
     * OFFLINE -> ERROR
     * Any -> DROPPED
     */
    builder.addState(HelixState.ONLINE.toString(), 1);
    builder.addState(HelixState.BOOTSTRAP.toString() ,2);
    builder.addState(HelixState.OFFLINE.toString(), 2);
    builder.addState(HelixDefinedState.ERROR.toString(), 3);
    builder.addState(HelixDefinedState.DROPPED.toString(), 4);

    builder.initialState(HelixState.OFFLINE.toString());

    // Valid transitions between the states.
    builder.addTransition(HelixState.OFFLINE.toString(), HelixState.BOOTSTRAP.toString());
    builder.addTransition(HelixState.BOOTSTRAP.toString(), HelixState.ONLINE.toString());
    builder.addTransition(HelixState.ONLINE.toString(), HelixState.OFFLINE.toString());
    builder.addTransition(HelixState.BOOTSTRAP.toString(), HelixState.OFFLINE.toString());
    builder.addTransition(HelixState.OFFLINE.toString(), HelixDefinedState.DROPPED.toString());

    // States constraints
    /*
     * Dynamic constraint: R means it should be derived based on the replication factor for the cluster
     * this allows a different replication factor for each resource without having to define a new state model.
     */
    builder.dynamicUpperBound(HelixState.ONLINE.toString(), UPPER_BOUND_REPLICATION_FACTOR);

    return builder.build();
  }
}
