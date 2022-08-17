package com.linkedin.venice.meta;

import com.linkedin.venice.exceptions.VeniceMessageException;
import java.util.HashMap;
import java.util.Map;


/**
 * IngestionMode is an Enum class that contains all modes for ingestion.
 */
public enum IngestionMode {
  /**
   * Isolation ingestion is not enabled.
   */
  BUILT_IN(0),
  /**
   * Isolated ingestion is enabled.
   */
  ISOLATED(1);

  private static final Map<Integer, IngestionMode> INTEGER_INGESTION_MODE_MAP = getIngestionModeMap();

  private final int value;

  private IngestionMode(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }

  private static Map<Integer, IngestionMode> getIngestionModeMap() {
    Map<Integer, IngestionMode> intToTypeMap = new HashMap<>();
    for (IngestionMode type: IngestionMode.values()) {
      intToTypeMap.put(type.value, type);
    }
    return intToTypeMap;
  }

  public static IngestionMode valueOf(int value) {
    IngestionMode type = INTEGER_INGESTION_MODE_MAP.get(value);
    if (type == null) {
      throw new VeniceMessageException("Invalid ingestion mode: " + value);
    }
    return type;
  }
}
