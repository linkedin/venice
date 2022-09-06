package com.linkedin.davinci.replication;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.HashMap;
import java.util.Map;


public enum BatchConflictResolutionPolicy {
  /**
   * Incoming data from Batch push should always be overwritten by real-time updates.
   */
  BATCH_WRITE_LOSES(0),
  /**
   * Start-Of-Push Control message's timestamp should be treated as the last update replication timestamp for all batch
   * records, and hybrid writes wins only when their own logicalTimestamp are higher.
   */
  USE_START_OF_PUSH_TIMESTAMP(1),
  /**
   * Per-record replication metadata is provided by the push job and stored for each key, enabling full conflict
   * resolution granularity on a per field basis, just like when merging concurrent update operations.
   */
  USE_PER_RECORD_LOGICAL_TIMESTAMP(2);

  private final int value;

  private static final Map<Integer, BatchConflictResolutionPolicy> BATCH_CONFLICT_RESOLUTION_POLICY_MAP =
      getBatchConflictResolutionPolicyMap();

  BatchConflictResolutionPolicy(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }

  private static Map<Integer, BatchConflictResolutionPolicy> getBatchConflictResolutionPolicyMap() {
    Map<Integer, BatchConflictResolutionPolicy> intToTypeMap = new HashMap<>();
    for (BatchConflictResolutionPolicy policy: BatchConflictResolutionPolicy.values()) {
      intToTypeMap.put(policy.value, policy);
    }

    return intToTypeMap;
  }

  public static BatchConflictResolutionPolicy valueOf(int value) {
    BatchConflictResolutionPolicy policy = BATCH_CONFLICT_RESOLUTION_POLICY_MAP.get(value);
    if (policy == null) {
      throw new VeniceException("Invalid batch conflict resolution policy type: " + value);
    }

    return policy;
  }
}
