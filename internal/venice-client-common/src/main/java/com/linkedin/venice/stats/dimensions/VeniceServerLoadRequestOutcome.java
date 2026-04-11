package com.linkedin.venice.stats.dimensions;

/** Request outcome for server load control metrics. */
public enum VeniceServerLoadRequestOutcome implements VeniceDimensionInterface {
  /** Request processed by the server (not load-shedded). */
  ACCEPTED,
  /** Request load-shedded before processing due to server overload. */
  REJECTED;

  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.VENICE_SERVER_LOAD_REQUEST_OUTCOME;
  }
}
