package com.linkedin.venice.stats.dimensions;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_DELIVERY_PROGRESS;


/**
 * Streaming delivery progress dimension for batch response timing metrics.
 */
public enum DeliveryProgress implements VeniceDimensionInterface {
  FIRST("first"), PCT_50("50pct"), PCT_90("90pct");

  private final String value;

  DeliveryProgress(String value) {
    this.value = value;
  }

  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VENICE_DELIVERY_PROGRESS;
  }

  @Override
  public String getDimensionValue() {
    return value;
  }
}
