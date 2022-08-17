package com.linkedin.venice.helix;

/**
 * An Enum enumerating all valid types of {@link HelixPartitionState}. This is the customized
 * per partition state contrary to the states defined in the state model.
 */
public enum HelixPartitionState {
  OFFLINE_PUSH, HYBRID_STORE_QUOTA
}
