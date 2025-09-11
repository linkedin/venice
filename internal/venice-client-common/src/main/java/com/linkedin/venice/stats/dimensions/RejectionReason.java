package com.linkedin.venice.stats.dimensions;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REJECTION_REASON;


public enum RejectionReason implements VeniceDimensionInterface {
  LOAD_CONTROLLER, NO_REPLICAS_AVAILABLE;

  private final String reason;

  RejectionReason() {
    this.reason = name().toLowerCase();
  }

  /**
   * All the instances of this Enum should have the same dimension name.
   * Refer {@link VeniceDimensionInterface#getDimensionName()} for more details.
   */
  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VENICE_REJECTION_REASON;
  }

  @Override
  public String getDimensionValue() {
    return this.reason;
  }
}
