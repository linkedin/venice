package com.linkedin.venice.stats.dimensions;

/**
 * Helix steady states for partition state metrics. Values must match
 * {@code ParticipantStateTransitionStats.ENABLED_STEADY_STATES} — a guard test
 * in {@code ParticipantStateTransitionStatsOtelTest} enforces this.
 */
public enum VeniceHelixSteadyState implements VeniceDimensionInterface {
  ERROR, STANDBY, LEADER;

  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.VENICE_HELIX_STATE;
  }
}
