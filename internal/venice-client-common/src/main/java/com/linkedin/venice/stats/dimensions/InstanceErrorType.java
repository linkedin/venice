package com.linkedin.venice.stats.dimensions;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.*;


public enum InstanceErrorType implements VeniceDimensionInterface {
  BLOCKED, UNHEALTHY, OVERLOADED;

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
