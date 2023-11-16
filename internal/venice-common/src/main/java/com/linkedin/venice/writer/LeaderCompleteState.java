package com.linkedin.venice.writer;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.EnumUtils;
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
  private static final LeaderCompleteState[] TYPES_ARRAY = EnumUtils.getEnumValuesArray(LeaderCompleteState.class);

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
    try {
      return TYPES_ARRAY[value];
    } catch (IndexOutOfBoundsException e) {
      throw new VeniceException("Invalid LeaderCompleteState: " + value);
    }
  }

  @Override
  public int getValue() {
    return value;
  }
}
