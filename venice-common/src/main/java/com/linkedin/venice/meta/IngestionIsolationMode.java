package com.linkedin.venice.meta;

import com.linkedin.venice.exceptions.VeniceMessageException;
import java.util.HashMap;
import java.util.Map;


/**
 * IngestionIsolationMode is an Enum class that contains all modes for ingestion isolation service. Each mode specifies
 * a port number that listener services can bind and listen to.
 */
public enum IngestionIsolationMode {
  /**
   * Ingestion isolation is not enabled.
   */
  NO_OP(0),
  /**
   * Parent process will fork a child process to handle ingestion.
   */
  PARENT_CHILD(1),
  /**
   * An independent service will be deployed to handle ingestion.
   */
  SPLIT_SERVICE(2);

  private static final Map<Integer, IngestionIsolationMode> INTEGER_INGESTION_ISOLATION_MODE_MAP = getIngestionIsolationModeMap();

  private final int value;

  private IngestionIsolationMode(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }

  private static Map<Integer, IngestionIsolationMode> getIngestionIsolationModeMap() {
    Map<Integer, IngestionIsolationMode> intToTypeMap = new HashMap<>();
    for (IngestionIsolationMode type : IngestionIsolationMode.values()) {
      intToTypeMap.put(type.value, type);
    }
    return intToTypeMap;
  }

  public static IngestionIsolationMode valueOf(int value) {
    IngestionIsolationMode type = INTEGER_INGESTION_ISOLATION_MODE_MAP.get(value);
    if (type == null) {
      throw new VeniceMessageException("Invalid ingestion isolation mode: " + value);
    }
    return type;
  }

}
