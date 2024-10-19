package com.linkedin.venice.writer;

import com.linkedin.venice.utils.EnumUtils;
import com.linkedin.venice.utils.VeniceEnumValue;
import java.util.List;


/**
 * Completion state of the leader partition.
 */
public enum LeaderCompleteState implements VeniceEnumValue {
  /**
   * Leader partition is not completed yet: Default state
   */
  LEADER_NOT_COMPLETED(0),
  /**
   * Leader partition is completed
   */
  LEADER_COMPLETED(1);

  private final int value;
  private static final List<LeaderCompleteState> TYPES = EnumUtils.getEnumValuesList(LeaderCompleteState.class);

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

  public static LeaderCompleteState valueOf(int value) {
    return EnumUtils.valueOf(TYPES, value, LeaderCompleteState.class);
  }

  @Override
  public int getValue() {
    return value;
  }
}
