package com.linkedin.venice.stats.dimensions;

public enum RequestRetryType implements VeniceDimensionInterface {
  ERROR_RETRY, LONG_TAIL_RETRY;

  private final String retryType;

  RequestRetryType() {
    this.retryType = name().toLowerCase();
  }

  /**
   * All the instances of this Enum should have the same dimension name.
   * Refer {@link VeniceDimensionInterface#getDimensionName()} for more details.
   */
  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.VENICE_REQUEST_RETRY_TYPE;
  }

  @Override
  public String getDimensionValue() {
    return this.retryType;
  }
}
