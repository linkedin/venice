package com.linkedin.venice.status;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;


public enum PushJobDetailsStatus {
  STARTED(0), COMPLETED(1), ERROR(2), NOT_CREATED(3), UNKNOWN(4), TOPIC_CREATED(5), WRITE_COMPLETED(6), KILLED(7),
  END_OF_PUSH_RECEIVED(8), START_OF_INCREMENTAL_PUSH_RECEIVED(9), END_OF_INCREMENTAL_PUSH_RECEIVED(10);

  private static final Set<Integer> TERMINAL_STATUSES =
      new HashSet<>(Arrays.asList(COMPLETED.getValue(), ERROR.getValue(), KILLED.getValue()));
  private final int value;

  PushJobDetailsStatus(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }

  public static boolean isTerminal(int status) {
    return TERMINAL_STATUSES.contains(status);
  }
}
