package com.linkedin.venice.writer;

import com.linkedin.venice.utils.VeniceEnumValue;


/**
 * Completion state of the leader partition.
 */
public enum LeaderCompleteState implements VeniceEnumValue {
  /**
   * Leader partition is not marked completed yet
   */
  LEADER_NOT_COMPLETED(0),
  /**
   * Leader partition is marked completed
   */
  LEADER_COMPLETED(1),
  /**
   * Default state for the standby to know that the feature is not supported, to
   * not consider leader's completion status for its completion.
   */
  NOT_SUPPORTED(2);

  private final int value;

  LeaderCompleteState(int value) {
    this.value = value;
  }

  public static LeaderCompleteState getLeaderCompleteState(boolean isLeaderCompleted) {
    if (isLeaderCompleted) {
      return LEADER_COMPLETED;
    } else {
      return LEADER_NOT_COMPLETED;
    }
  }

  @Override
  public int getValue() {
    return value;
  }
}
