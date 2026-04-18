package com.linkedin.venice.meta;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.VeniceEnumValue;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;


/**
 * Enums of the ingestion pause modes for Venice stores.
 * Used as an operational lever during incident response to pause Kafka consumption.
 */
public enum IngestionPauseMode implements VeniceEnumValue {
  /** Default. All ingestion runs normally. */
  NOT_PAUSED(0),
  /** Pause ingestion for the current serving version's RT consumption only.
   * Non-current versions (future pushes, backup versions) are not affected.
   */
  CURRENT_VERSION(1),
  /** Pause ingestion for ALL versions, including the current serving version's RT and VT consumption. */
  ALL_VERSIONS(2);

  private int value;

  IngestionPauseMode(int v) {
    this.value = v;
  }

  private static final Map<Integer, IngestionPauseMode> idMapping = new HashMap<>();
  static {
    Arrays.stream(values()).forEach(s -> idMapping.put(s.value, s));
  }

  public static IngestionPauseMode fromInt(int i) {
    IngestionPauseMode mode = idMapping.get(i);
    if (mode == null) {
      throw new VeniceException("Invalid IngestionPauseMode id: " + i);
    }
    return mode;
  }

  @Override
  public int getValue() {
    return value;
  }
}
