package com.linkedin.venice.kafka.consumer;

public enum LeaderFollowerStateType {
  /**
   * This partition is the leader.
   */
  LEADER,
  /**
   * This partition is a follower.
   */
  STANDBY,
  /**
   * This partition is in the transition process from follower to leader, but it hasn't finished doing all the
   * status check yet; so it's still running as a follower, but it will switch to the leader role at anytime.
   */
  IN_TRANSITION_FROM_STANDBY_TO_LEADER;
}
