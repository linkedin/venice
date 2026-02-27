package com.linkedin.venice.stats.dimensions;

/**
 * Dimension to represent the replica state of a Venice storage node.
 */
public enum ReplicaState implements VeniceDimensionInterface {
  READY_TO_SERVE, CATCHING_UP;

  /**
   * All the instances of this Enum should have the same dimension name.
   * Refer {@link VeniceDimensionInterface#getDimensionName()} for more details.
   */
  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.VENICE_REPLICA_STATE;
  }
}
