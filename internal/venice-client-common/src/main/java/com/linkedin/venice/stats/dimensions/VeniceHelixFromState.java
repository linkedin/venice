package com.linkedin.venice.stats.dimensions;

/**
 * Helix "from" state for partition state transition metrics.
 * Same values as {@link VeniceHelixToState} but maps to a different dimension key
 * ({@link VeniceMetricsDimensions#VENICE_HELIX_FROM_STATE}).
 * Values are a subset of {@link com.linkedin.venice.helix.HelixState} constants,
 * excluding {@code UNKNOWN}.
 */
public enum VeniceHelixFromState implements VeniceDimensionInterface {
  OFFLINE, STANDBY, LEADER, ERROR, DROPPED;

  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.VENICE_HELIX_FROM_STATE;
  }
}
