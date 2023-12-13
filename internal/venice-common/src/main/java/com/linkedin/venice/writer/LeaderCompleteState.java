package com.linkedin.venice.writer;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.EnumUtils;
import com.linkedin.venice.utils.VeniceEnumValue;


/**
 * Completion state of the leader partition.
 */
public enum LeaderCompleteState implements VeniceEnumValue {
  /**
   * Leader partition is not completed yet
   */
  LEADER_NOT_COMPLETED(0),
  /**
   * Leader partition is completed
   */
  LEADER_COMPLETED(1),
  /**
   * Default state for the partition until VENICE_LEADER_COMPLETION_STATE_HEADER is received from leader partition
   * and the state is updated. This works in conjunction with PartitionConsumptionState#isFirstHeartBeatSOSReceived
   * to decide whether to use this state or not. This will be used only during the transition phase to avoid 2 phase
   * deployment and once all servers are running the latest code, this can be removed.
   */
  LEADER_COMPLETE_STATE_UNKNOWN(2);

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
