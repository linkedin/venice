package com.linkedin.venice.stats.dimensions;

public enum VeniceClientType implements VeniceDimensionInterface {
  THIN_CLIENT, FAST_CLIENT, DA_VINCI_CLIENT;

  private final String clientType;

  VeniceClientType() {
    this.clientType = name().toLowerCase();
  }

  /**
   * All the instances of this Enum should have the same dimension name.
   * Refer {@link VeniceDimensionInterface#getDimensionName()} for more details.
   */
  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.VENICE_CLIENT_TYPE;
  }

  @Override
  public String getDimensionValue() {
    return this.clientType;
  }
}
