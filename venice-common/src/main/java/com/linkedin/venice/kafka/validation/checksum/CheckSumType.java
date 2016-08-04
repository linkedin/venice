package com.linkedin.venice.kafka.validation.checksum;

import com.linkedin.venice.exceptions.VeniceMessageException;

import java.util.HashMap;
import java.util.Map;

/**
 * Types of checksum algorithms supported by Venice's Data Ingest Validation.
 */
public enum CheckSumType {
  NONE(0),
  MD5(1),
  ADLER32(2),
  CRC32(3);

  /** The value is the byte used on the wire format */
  private final int value;
  private static final Map<Integer, CheckSumType> TYPE_MAP = getTypeMap();

  CheckSumType(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }

  private static Map<Integer, CheckSumType> getTypeMap() {
    Map<Integer, CheckSumType> intToTypeMap = new HashMap<>();
    for (CheckSumType type : CheckSumType.values()) {
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
