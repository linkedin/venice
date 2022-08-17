package com.linkedin.venice.ingestion.protocol.enums;

import com.linkedin.venice.exceptions.VeniceMessageException;
import java.util.HashMap;
import java.util.Map;


/**
 * IngestionComponentType is an Enum class for specifying different components for graceful shutdown in forked ingestion process.
 */
public enum IngestionComponentType {
  KAFKA_INGESTION_SERVICE(0), STORAGE_SERVICE(1);

  private final int value;
  private static final Map<Integer, IngestionComponentType> INGESTION_COMPONENT_TYPE_MAP = getIngestionCommandTypeMap();

  private IngestionComponentType(int value) {
    this.value = value;
  }

  public static IngestionComponentType valueOf(int value) {
    IngestionComponentType type = INGESTION_COMPONENT_TYPE_MAP.get(value);
    if (type == null) {
      throw new VeniceMessageException("Invalid shutdown command type: " + value);
    }
    return type;
  }

  public int getValue() {
    return value;
  }

  private static Map<Integer, IngestionComponentType> getIngestionCommandTypeMap() {
    Map<Integer, IngestionComponentType> intToTypeMap = new HashMap<>();
    for (IngestionComponentType type: IngestionComponentType.values()) {
      intToTypeMap.put(type.value, type);
    }
    return intToTypeMap;
  }

}
