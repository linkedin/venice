package com.linkedin.venice.stats.dimensions;

public enum ExecutionStatus implements VeniceDimensionInterface {
  FAILED, SUCCESS;

  private final String status;

  ExecutionStatus() {
    this.status = name().toLowerCase();
  }

  /**
   * All the instances of this Enum should have the same dimension name.
   * Refer {@link VeniceDimensionInterface#getDimensionName()} for more details.
   */
  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.EXECUTION_STATUS;
  }

  @Override
  public String getDimensionValue() {
    return this.status;
  }
}
