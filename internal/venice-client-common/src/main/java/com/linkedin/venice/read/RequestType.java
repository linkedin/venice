package com.linkedin.venice.read;

import com.linkedin.venice.stats.dimensions.VeniceDimensionInterface;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;


public enum RequestType implements VeniceDimensionInterface {
  SINGLE_GET(""), MULTI_GET("multiget_"), MULTI_GET_STREAMING("multiget_streaming_"), COMPUTE("compute_"),
  COMPUTE_STREAMING("compute_streaming_");

  private final String metricPrefix;

  RequestType(String metricPrefix) {
    this.metricPrefix = metricPrefix;
  }

  public String getMetricPrefix() {
    return this.metricPrefix;
  }

  /**
   * All the instances of this Enum should have the same dimension name.
   * Refer {@link VeniceDimensionInterface#getDimensionName()} for more details.
   */
  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.VENICE_REQUEST_METHOD;
  }

  @Override
  public String getDimensionValue() {
    return name().toLowerCase();
  }
}
