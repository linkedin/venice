package com.linkedin.venice.status;

import com.linkedin.venice.utils.EnumUtils;
import com.linkedin.venice.utils.VeniceEnumValue;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public enum PushJobDetailsStatus implements VeniceEnumValue {
  STARTED(0), COMPLETED(1), ERROR(2), NOT_CREATED(3), UNKNOWN(4), TOPIC_CREATED(5), DATA_WRITER_COMPLETED(6), KILLED(7),
  END_OF_PUSH_RECEIVED(8), START_OF_INCREMENTAL_PUSH_RECEIVED(9), END_OF_INCREMENTAL_PUSH_RECEIVED(10);

  private static final Set<Integer> TERMINAL_STATUSES =
      new HashSet<>(Arrays.asList(COMPLETED.getValue(), ERROR.getValue(), KILLED.getValue()));

  private static final Set<PushJobDetailsStatus> TERMINAL_FAILED_STATUSES = new HashSet<>(Arrays.asList(ERROR, KILLED));
  private final int value;

  PushJobDetailsStatus(int value) {
    this.value = value;
  }

  private static final List<PushJobDetailsStatus> TYPES = EnumUtils.getEnumValuesList(PushJobDetailsStatus.class);

  @Override
  public int getValue() {
    return value;
  }

  public static boolean isTerminal(int status) {
    return TERMINAL_STATUSES.contains(status);
  }

  public static boolean isSucceeded(PushJobDetailsStatus status) {
    return status == COMPLETED;
  }

  public static boolean isFailed(PushJobDetailsStatus status) {
    return TERMINAL_FAILED_STATUSES.contains(status);
  }

  public static PushJobDetailsStatus valueOf(int value) {
    return EnumUtils.valueOf(TYPES, value, PushJobDetailsStatus.class);
  }
}
