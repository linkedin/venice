package com.linkedin.venice.stats.dimensions;

/**
 * Dimension to represent the replica type of Venice storage node.
 */
public enum ReplicaType implements VeniceDimensionInterface {
  LEADER, FOLLOWER;

  /**
   * All the instances of this Enum should have the same dimension name.
   * Refer {@link VeniceDimensionInterface#getDimensionName()} for more details.
   */
  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.VENICE_REPLICA_TYPE;
  }
}
