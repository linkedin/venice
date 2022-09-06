package com.linkedin.venice.meta;

import com.linkedin.venice.exceptions.VeniceMessageException;
import java.util.HashMap;
import java.util.Map;


public enum IngestionMetadataUpdateType {
  PUT_OFFSET_RECORD(0), CLEAR_OFFSET_RECORD(1), PUT_STORE_VERSION_STATE(2), CLEAR_STORE_VERSION_STATE(3);

  private static final Map<Integer, IngestionMetadataUpdateType> INGESTION_METADATA_UPDATE_TYPE_MAP =
      getIngestionMetadataUpdateTypeMap();

  private final int value;

  private IngestionMetadataUpdateType(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }

  private static Map<Integer, IngestionMetadataUpdateType> getIngestionMetadataUpdateTypeMap() {
    Map<Integer, IngestionMetadataUpdateType> intToTypeMap = new HashMap<>();
    for (IngestionMetadataUpdateType type: IngestionMetadataUpdateType.values()) {
      intToTypeMap.put(type.value, type);
    }
    return intToTypeMap;
  }

  public static IngestionMetadataUpdateType valueOf(int value) {
    IngestionMetadataUpdateType type = INGESTION_METADATA_UPDATE_TYPE_MAP.get(value);
    if (type == null) {
      throw new VeniceMessageException("Invalid ingestion metadata update type: " + value);
    }
    return type;
  }
}
