package com.linkedin.davinci.kafka.consumer;

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
  IN_TRANSITION_FROM_STANDBY_TO_LEADER,
  /**
   * This partition is paused from follower to leader transition, to avoid two leaders in the process of store
   * migration; so it's still running as a follower and won't switch to the leader role until the end of migration.
   */
  PAUSE_TRANSITION_FROM_STANDBY_TO_LEADER;
}
