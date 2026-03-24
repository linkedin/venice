package com.linkedin.venice.stats.dimensions;

/**
 * Dimension enum representing the source type of a connection to the server.
 * Maps to {@link VeniceMetricsDimensions#VENICE_CONNECTION_SOURCE}.
 */
public enum VeniceConnectionSource implements VeniceDimensionInterface {
  ROUTER, CLIENT;

  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.VENICE_CONNECTION_SOURCE;
  }
}
