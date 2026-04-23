package com.linkedin.venice.stats.dimensions;

/**
 * Helix "to" state for partition state transition metrics.
 * Same values as {@link VeniceHelixFromState} but maps to a different dimension key
 * ({@link VeniceMetricsDimensions#VENICE_HELIX_TO_STATE}).
 * Values match {@link com.linkedin.venice.helix.HelixState} constants.
 */
public enum VeniceHelixToState implements VeniceDimensionInterface {
  OFFLINE, STANDBY, LEADER, ERROR, DROPPED;

  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.VENICE_HELIX_TO_STATE;
  }
}
