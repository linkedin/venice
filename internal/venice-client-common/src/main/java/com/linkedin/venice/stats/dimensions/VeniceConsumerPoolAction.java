package com.linkedin.venice.stats.dimensions;

/**
 * Dimension enum representing a consumer pool action type.
 * Maps to {@link VeniceMetricsDimensions#VENICE_CONSUMER_POOL_ACTION}.
 */
public enum VeniceConsumerPoolAction implements VeniceDimensionInterface {
  SUBSCRIBE, UPDATE_ASSIGNMENT;

  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.VENICE_CONSUMER_POOL_ACTION;
  }
}
