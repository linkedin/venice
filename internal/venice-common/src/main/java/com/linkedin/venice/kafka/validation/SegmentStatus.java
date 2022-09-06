package com.linkedin.venice.kafka.validation;

import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import java.util.HashMap;
import java.util.Map;


public enum SegmentStatus {
  /** Did not receive {@link ControlMessageType#START_OF_SEGMENT} */
  NOT_STARTED(0, false),
  /** Received {@link ControlMessageType#START_OF_SEGMENT} but not {@link ControlMessageType#END_OF_SEGMENT} */
  IN_PROGRESS(1, false),
  /** Received {@link ControlMessageType#END_OF_SEGMENT} with finalSegment = false. More segments should come from the same producer. */
  END_OF_INTERMEDIATE_SEGMENT(2, true),
  /** Received {@link ControlMessageType#END_OF_SEGMENT} with finalSegment = true. */
  END_OF_FINAL_SEGMENT(3, true);

  private final int value;
  private final boolean terminal;
  private static final Map<Integer, SegmentStatus> TYPE_MAP = getTypeMap();

  SegmentStatus(int value, boolean terminal) {
    this.value = value;
    this.terminal = terminal;
  }

  public int getValue() {
    return value;
  }

  public boolean isTerminal() {
    return terminal;
  }

  private static Map<Integer, SegmentStatus> getTypeMap() {
    Map<Integer, SegmentStatus> intToTypeMap = new HashMap<>();
    for (SegmentStatus type: SegmentStatus.values()) {
      intToTypeMap.put(type.value, type);
    }
    return intToTypeMap;
  }

  public static SegmentStatus valueOf(int value) {
    SegmentStatus type = TYPE_MAP.get(value);
    if (type == null) {
      throw new VeniceMessageException("Invalid SegmentStatus: " + value);
    }
    return type;
  }
}
