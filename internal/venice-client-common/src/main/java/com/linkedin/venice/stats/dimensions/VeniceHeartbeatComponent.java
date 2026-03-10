package com.linkedin.venice.stats.dimensions;

/**
 * Dimension enum representing the heartbeat monitoring service thread components.
 * Each value corresponds to a distinct background thread within the heartbeat monitoring service.
 *
 * Maps to {@link VeniceMetricsDimensions#VENICE_HEARTBEAT_COMPONENT}.
 */
public enum VeniceHeartbeatComponent implements VeniceDimensionInterface {
  /** The heartbeat reporter thread that publishes lag metrics. */
  REPORTER,
  /** The heartbeat logger thread that logs lag delays and cleans up stale monitors. */
  LOGGER;

  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.VENICE_HEARTBEAT_COMPONENT;
  }
}
