package com.linkedin.venice.meta;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.HashMap;
import java.util.Map;


/**
 * Enums of the policies used to decide how buffer replay start timestamps are calculated.
 */
public enum BufferReplayPolicy {
  /**
   * Default value. Replay all records from 'rewindTimeInSeconds' seconds before EOP call was received at the controller.
   */
  REWIND_FROM_EOP(0),
  /**
   * Replay all records from 'rewindTimeInSeconds' seconds before SOP call was received at the controller.
   */
  REWIND_FROM_SOP(1);

  private final int value;

  private static final Map<Integer, BufferReplayPolicy> BUFFER_REPLAY_POLICY_MAP = getBufferReplayPolicyMap();

  BufferReplayPolicy(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }

  private static Map<Integer, BufferReplayPolicy> getBufferReplayPolicyMap() {
    final Map<Integer, BufferReplayPolicy> intToTypeMap = new HashMap<>();
    for (BufferReplayPolicy style: BufferReplayPolicy.values()) {
      intToTypeMap.put(style.value, style);
    }

    return intToTypeMap;
  }

  public static BufferReplayPolicy valueOf(int value) {
    final BufferReplayPolicy style = BUFFER_REPLAY_POLICY_MAP.get(value);
    if (style == null) {
      throw new VeniceException("Invalid buffer replay policy: " + value);
    }

    return style;
  }
}
