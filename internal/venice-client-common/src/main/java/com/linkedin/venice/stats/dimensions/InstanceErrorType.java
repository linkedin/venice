package com.linkedin.venice.stats.dimensions;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_INSTANCE_ERROR_TYPE;


public enum InstanceErrorType implements VeniceDimensionInterface {
  /**
   * Blocked instances are temporarily unavailable due to pending requests
   * on the server instance is above a certain threshold.
   */
  BLOCKED,
  /**
   * The instance is unhealthy because it is not responding to health heartbeat checks.
   */
  UNHEALTHY,
  /**
   * The instance is overloaded because it is rejected by load controllers as per the server feedback.
   */
  OVERLOADED;

  private final String type;

  InstanceErrorType() {
    this.type = name().toLowerCase();
  }

  /**
   * All the instances of this Enum should have the same dimension name.
   * Refer {@link VeniceDimensionInterface#getDimensionName()} for more details.
   */
  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VENICE_INSTANCE_ERROR_TYPE;
  }

  @Override
  public String getDimensionValue() {
    return type;
  }
}
