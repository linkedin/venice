package com.linkedin.venice.ingestion.protocol.enums;

import com.linkedin.venice.exceptions.VeniceMessageException;
import java.util.HashMap;
import java.util.Map;


/**
 * IngestionReportType is an Enum class for specifying different ingestion reports for ingestion isolation.
 */
public enum IngestionReportType {
  COMPLETED(0), ERROR(1), STARTED(2), RESTARTED(3), PROGRESS(4), END_OF_PUSH_RECEIVED(5), @Deprecated
  START_OF_BUFFER_REPLAY_RECEIVED(6), START_OF_INCREMENTAL_PUSH_RECEIVED(7), END_OF_INCREMENTAL_PUSH_RECEIVED(8),
  TOPIC_SWITCH_RECEIVED(9), DATA_RECOVERY_COMPLETED(10);

  private final int value;
  private static final Map<Integer, IngestionReportType> INGESTION_REPORT_TYPE_MAP = getIngestionReportTypeMap();

  IngestionReportType(int value) {
    this.value = value;
  }

  public static IngestionReportType valueOf(int value) {
    IngestionReportType type = INGESTION_REPORT_TYPE_MAP.get(value);
    if (type == null) {
      throw new VeniceMessageException("Invalid ingestion report type: " + value);
    }
    return type;
  }

  public int getValue() {
    return value;
  }

  private static Map<Integer, IngestionReportType> getIngestionReportTypeMap() {
    Map<Integer, IngestionReportType> intToTypeMap = new HashMap<>();
    for (IngestionReportType type: IngestionReportType.values()) {
      intToTypeMap.put(type.value, type);
    }
    return intToTypeMap;
  }
}
