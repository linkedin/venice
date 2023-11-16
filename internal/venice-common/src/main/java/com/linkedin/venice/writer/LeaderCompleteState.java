package com.linkedin.venice.writer;

/**
 * Completion state of the leader partition.
 */
public enum LeaderCompleteState {
  /**
   * Default state for the standby to know that the feature is not supported and
   * don't consider leader's completion status for its completion.
   */
  NOT_SUPPORTED,
  /**
   * Leader partition is marked completed
   */
  LEADER_COMPLETED,
  /**
   * Leader partition is not marked completed yet
   */
  LEADER_NOT_COMPLETED;

  public static LeaderCompleteState getLeaderCompleteState(boolean isLeaderCompleted) {
    if (isLeaderCompleted) {
      return LEADER_COMPLETED;
    } else {
      return LEADER_NOT_COMPLETED;
    }
  }
}
