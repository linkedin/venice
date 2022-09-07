package com.linkedin.venice.meta;

import com.fasterxml.jackson.annotation.JsonEnumDefaultValue;
import com.linkedin.venice.exceptions.VeniceException;
import java.util.HashMap;
import java.util.Map;


/**
 * Enums of the strategies used to decide how stores with incremental pushes and real time pushes reconcile the data.
 */
@Deprecated
public enum IncrementalPushPolicy {
  /**
   * Default behavior for current stores.
   */
  PUSH_TO_VERSION_TOPIC(0),

  /**
   * Behavior of incremental push is the same as it is for real time Samza jobs.
   */
  @JsonEnumDefaultValue
  INCREMENTAL_PUSH_SAME_AS_REAL_TIME(1);

  private final int value;

  private static final Map<Integer, IncrementalPushPolicy> INCREMENTAL_PUSH_POLICY_MAP = getIncrementalPushPolicyMap();

  IncrementalPushPolicy(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }

  private static Map<Integer, IncrementalPushPolicy> getIncrementalPushPolicyMap() {
    Map<Integer, IncrementalPushPolicy> intToTypeMap = new HashMap<>();
    for (IncrementalPushPolicy policy: IncrementalPushPolicy.values()) {
      intToTypeMap.put(policy.value, policy);
    }

    return intToTypeMap;
  }

  public static IncrementalPushPolicy valueOf(int value) {
    IncrementalPushPolicy policy = INCREMENTAL_PUSH_POLICY_MAP.get(value);
    if (policy == null) {
      throw new VeniceException("Invalid incremental push policy: " + value);
    }

    return policy;
  }
}
