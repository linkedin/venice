package com.linkedin.venice.stats.dimensions;

/**
 * Dimension to represent the replica state of a Venice storage node.
 */
public enum ReplicaState implements VeniceDimensionInterface {
  READY_TO_SERVE, CATCHING_UP;

  private final String replicaState;

  ReplicaState() {
    this.replicaState = name().toLowerCase();
  }

  /**
   * All the instances of this Enum should have the same dimension name.
   * Refer {@link VeniceDimensionInterface#getDimensionName()} for more details.
   */
  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.VENICE_REPLICA_STATE;
  }

  @Override
  public String getDimensionValue() {
    return this.replicaState;
  }
}
