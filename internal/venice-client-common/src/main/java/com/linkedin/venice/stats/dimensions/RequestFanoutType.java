package com.linkedin.venice.stats.dimensions;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_FANOUT_TYPE;


public enum RequestFanoutType implements VeniceDimensionInterface {
  ORIGINAL, RETRY;

  private final String type;

  RequestFanoutType() {
    this.type = name().toLowerCase();
  }

  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VENICE_REQUEST_FANOUT_TYPE;
  }

  @Override
  public String getDimensionValue() {
    return type;
  }
}
