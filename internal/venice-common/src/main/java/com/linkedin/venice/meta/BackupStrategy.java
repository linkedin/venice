package com.linkedin.venice.meta;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;


/**
 * Enums of the strategies used to backup older store versions in Venice.
 */
public enum BackupStrategy {
  /** Keep numVersionsToPreserve number of backup version.
   */
  KEEP_MIN_VERSIONS(0),
  /** Delete versions on SN (including kafka and metadata) to preserve only (numVersionsToPreserve-1)
   * backup versions on new push start.
   */
  DELETE_ON_NEW_PUSH_START(1);

  /** Delete versions on SN (but not kafka and metadata) to preserve only (numVersionsToPreserve-1)
   * backup versions on new push start. So that the deleted versions can be rolled back from Kafka ingestion.
   */
  // KEEP_IN_KAFKA_ONLY,
  /** Keep in user-specified store eg HDD, other DB */
  // KEEP_IN_USER_STORE;
  private int value;

  BackupStrategy(int v) {
    this.value = v;
  }

  private static final Map<Integer, BackupStrategy> idMapping = new HashMap<>();
  static {
    Arrays.stream(values()).forEach(s -> idMapping.put(s.value, s));
  }

  public static BackupStrategy fromInt(int i) {
    BackupStrategy strategy = idMapping.get(i);
    if (strategy == null) {
      throw new VeniceException("Invalid BackupStrategy id: " + i);
    }
    return strategy;
  }
}
