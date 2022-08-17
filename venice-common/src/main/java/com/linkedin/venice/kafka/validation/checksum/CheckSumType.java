package com.linkedin.venice.kafka.validation.checksum;

import com.linkedin.venice.exceptions.VeniceMessageException;
import java.util.HashMap;
import java.util.Map;


/**
 * Types of checksum algorithms supported by Venice's Data Ingest Validation.
 */
public enum CheckSumType {
  NONE(0, true), MD5(1, true), @Deprecated
  ADLER32(2, false), @Deprecated
  CRC32(3, false);

  /** The value is the byte used on the wire format */
  private final int value;
  private final boolean checkpointingSupported;
  private static final Map<Integer, CheckSumType> TYPE_MAP = getTypeMap();

  CheckSumType(int value, boolean checkpointingSupported) {
    this.value = value;
    this.checkpointingSupported = checkpointingSupported;
  }

  public int getValue() {
    return value;
  }

  public boolean isCheckpointingSupported() {
    return checkpointingSupported;
  }

  private static Map<Integer, CheckSumType> getTypeMap() {
    Map<Integer, CheckSumType> intToTypeMap = new HashMap<>();
    for (CheckSumType type: CheckSumType.values()) {
      intToTypeMap.put(type.value, type);
    }
    return intToTypeMap;
  }

  public static CheckSumType valueOf(int value) {
    CheckSumType type = TYPE_MAP.get(value);
    if (type == null) {
      throw new VeniceMessageException("Invalid checksum type: " + value);
    }
    return type;
  }

  public String toString() {
    return "Checksum(" + name() + ")";
  }
}
