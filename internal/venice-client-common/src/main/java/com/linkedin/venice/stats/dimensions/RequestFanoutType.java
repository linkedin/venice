package com.linkedin.venice.stats.dimensions;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_FANOUT_TYPE;


public enum RequestFanoutType implements VeniceDimensionInterface {
  /**
   * Indicates the initial/original request sent by the client.
   * Use this when the request is being made for the first time, not as a result of a retry.
   */
  ORIGINAL,
  /**
   * Indicates a retry request sent by the client.
   * Use this when the request is being retried after a previous attempt failed or timed out.
   */
  RETRY;

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
