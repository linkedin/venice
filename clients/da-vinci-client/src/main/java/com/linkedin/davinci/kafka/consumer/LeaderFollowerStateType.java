package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.exceptions.VeniceMessageException;
import java.util.HashMap;
import java.util.Map;


public enum LeaderFollowerStateType {
  /**
   * This partition is the leader.
   */
  LEADER(0),
  /**
   * This partition is a follower.
   */
  STANDBY(1),
  /**
   * This partition is in the transition process from follower to leader, but it hasn't finished doing all the
   * status check yet; so it's still running as a follower, but it will switch to the leader role at anytime.
   */
  IN_TRANSITION_FROM_STANDBY_TO_LEADER(2),
  /**
   * This partition is paused from follower to leader transition, to avoid two leaders in the process of store
   * migration; so it's still running as a follower and won't switch to the leader role until the end of migration.
   */
  PAUSE_TRANSITION_FROM_STANDBY_TO_LEADER(3);

  private final int value;
  private static final Map<Integer, LeaderFollowerStateType> LEADER_FOLLOWER_STATES_TYPE_MAP =
      getLeaderFollowerStatesTypeMap();

  LeaderFollowerStateType(int value) {
    this.value = value;
  }

  public static LeaderFollowerStateType valueOf(int value) {
    LeaderFollowerStateType type = LEADER_FOLLOWER_STATES_TYPE_MAP.get(value);
    if (type == null) {
      throw new VeniceMessageException("Invalid ingestion command type: " + value);
    }
    return type;
  }

  public int getValue() {
    return value;
  }

  private static Map<Integer, LeaderFollowerStateType> getLeaderFollowerStatesTypeMap() {
    Map<Integer, LeaderFollowerStateType> intToTypeMap = new HashMap<>();
    for (LeaderFollowerStateType type: LeaderFollowerStateType.values()) {
      intToTypeMap.put(type.value, type);
    }
    return intToTypeMap;
  }
}
