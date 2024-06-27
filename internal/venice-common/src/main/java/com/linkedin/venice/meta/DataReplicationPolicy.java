package com.linkedin.venice.meta;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.HashMap;
import java.util.Map;


/**
 * Enums of the policies used to decide how real-time samza data is replicated.
 * Note: Data replication policy does not play any role for AA enabled hybrid stores.
 */
public enum DataReplicationPolicy {
  /**
   * Default value. Samza job per colo pushes to local real-time topic. Leader SNs replicate data to local version topic.
   */
  NON_AGGREGATE(0),
  /**
   * Single Samza job or Samza job per colo pushes to real-time topic in parent colo. Leader SNs in each colo replicate
   * data from remote real-time topic to local version topic.
   */
  AGGREGATE(1),

  @Deprecated
  NONE(2),

  @Deprecated
  ACTIVE_ACTIVE(3);

  private final int value;

  private static final Map<Integer, DataReplicationPolicy> DATA_REPLICATION_POLICY_MAP = getDataReplicationPolicyMap();

  DataReplicationPolicy(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }

  public static Map<Integer, DataReplicationPolicy> getDataReplicationPolicyMap() {
    final Map<Integer, DataReplicationPolicy> intToTypeMap = new HashMap<>();
    for (DataReplicationPolicy style: DataReplicationPolicy.values()) {
      intToTypeMap.put(style.value, style);
    }

    return intToTypeMap;
  }

  public static DataReplicationPolicy valueOf(int value) {
    final DataReplicationPolicy style = DATA_REPLICATION_POLICY_MAP.get(value);
    if (style == null) {
      throw new VeniceException("Invalid data replication policy: " + value);
    }

    return style;
  }
}
