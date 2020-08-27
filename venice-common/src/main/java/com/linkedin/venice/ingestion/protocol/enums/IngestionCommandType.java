package com.linkedin.venice.ingestion.protocol.enums;

import com.linkedin.venice.exceptions.VeniceMessageException;
import java.util.HashMap;
import java.util.Map;


/**
 * IngestionCommandType is an Enum class for specifying different commands for ingestion isolation.
 */
public enum IngestionCommandType {
  START_CONSUMPTION(0),
  STOP_CONSUMPTION(1),
  KILL_CONSUMPTION(2),
  RESET_CONSUMPTION(3),
  IS_PARTITION_CONSUMING(4);

  private final int value;
  private static final Map<Integer, IngestionCommandType> INGESTION_COMMAND_TYPE_MAP = getIngestionCommandTypeMap();

  private IngestionCommandType(int value) {
    this.value = value;
  }

  public static IngestionCommandType valueOf(int value) {
    IngestionCommandType type = INGESTION_COMMAND_TYPE_MAP.get(value);
    if (type == null) {
      throw new VeniceMessageException("Invalid ingestion command type: " + value);
    }
    return type;
  }

  public int getValue() {
    return value;
  }

  private static Map<Integer, IngestionCommandType> getIngestionCommandTypeMap() {
    Map<Integer, IngestionCommandType> intToTypeMap = new HashMap<>();
    for (IngestionCommandType type : IngestionCommandType.values()) {
      intToTypeMap.put(type.value, type);
    }
    return intToTypeMap;
  }

}

