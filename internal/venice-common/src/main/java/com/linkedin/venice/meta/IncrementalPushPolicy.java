package com.linkedin.venice.meta;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;


/**
 * Enums of the strategies used to decide how stores with incremental pushes and real time pushes reconcile the data.
 */
public enum IncrementalPushPolicy {
  /**
   * Default behavior for current stores.
   */
  PUSH_TO_VERSION_TOPIC(0, true, false),

  /**
   * Behavior of incremental push is the same as it is for real time Samza jobs.
   */
  INCREMENTAL_PUSH_SAME_AS_REAL_TIME(1, false, true);

  private final int value;
  private final boolean compatibleWithNonHybridStores;
  private final boolean compatibleWithHybridStores;

  private static final Map<Integer, IncrementalPushPolicy> INCREMENTAL_PUSH_POLICY_MAP = getIncrementalPushPolicyMap();

  IncrementalPushPolicy(int value, boolean compatibleWithNonHybridStores, boolean compatibleWithHybridStores) {
    this.value = value;
    this.compatibleWithNonHybridStores = compatibleWithNonHybridStores;
    this.compatibleWithHybridStores = compatibleWithHybridStores;
  }

  public int getValue() {
    return value;
  }

  public boolean isCompatibleWithNonHybridStores() {
    return compatibleWithNonHybridStores;
  }

  public boolean isCompatibleWithHybridStores() {
    return compatibleWithHybridStores;
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

  public static Optional<IncrementalPushPolicy> optionalValueOf(int value) {
    return Optional.of(valueOf(value));
  }
}
