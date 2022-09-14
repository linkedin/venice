package com.linkedin.venice.hadoop.input.kafka.ttl;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.HashMap;
import java.util.Map;


/**
 * The policy controls the TTL behavior regarding how batch writes are treated.
 * As of today (09/15/2022), only real-time write can be TTLed/supported.
 */
public enum TTLResolutionPolicy {
  /**
   * Reject batch writes as they don't contain RMD to perform TTL logic, so only real-time write. can be TTLed
   */
  REJECT_BATCH_WRITE(0),

  /**
   * Bypass all the batch data and don't perform TTL on them.
   * Note this policy is NOT supported yet.
   * In order to support this, H2V job has to differentiate batch write and real-time write.
   */
  BYPASS_BATCH_WRITE(1),

  /**
   * Allow both batch writes and real-time writes be TTLed.
   * Note this policy is NOT supported yet.
   * In order to support this, a reliable timestamp has to be designed/identified for batch writes.
   */
  ACCEPT_BATCH_WRITE(2);

  private final int value;

  private static final Map<Integer, TTLResolutionPolicy> TTL_RESOLUTION_POLICIES = getTTLResolutionPolicies();

  private static Map<Integer, TTLResolutionPolicy> getTTLResolutionPolicies() {
    Map<Integer, TTLResolutionPolicy> policies = new HashMap<>();
    for (TTLResolutionPolicy policy: TTLResolutionPolicy.values()) {
      policies.put(policy.value, policy);
    }
    return policies;
  }

  TTLResolutionPolicy(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }

  public static TTLResolutionPolicy valueOf(int value) {
    TTLResolutionPolicy policy = TTL_RESOLUTION_POLICIES.get(value);
    if (policy == null) {
      throw new VeniceException("Invalid TTL resolution policy type: " + value);
    }
    return policy;
  }

}
