package com.linkedin.venice.stats.dimensions;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STREAM_PROGRESS;


/**
 * Streaming delivery progress dimension for batch response timing metrics.
 */
public enum StreamProgress implements VeniceDimensionInterface {
  FIRST("FirstRecord"), PCT_50("50thPercentileRecord"), PCT_90("90thPercentileRecord");

  private final String value;

  StreamProgress(String value) {
    this.value = value;
  }

  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VENICE_STREAM_PROGRESS;
  }

  @Override
  public String getDimensionValue() {
    return value;
  }
}
