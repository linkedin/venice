package com.linkedin.venice.stats.dimensions;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_REJECTION_REASON;


public enum RejectionReason implements VeniceDimensionInterface {
  /**
   * The request was rejected because the load controller is currently limiting or throttling requests.
   */
  THROTTLED_BY_LOAD_CONTROLLER,
  /**
   * The request was rejected because there are no available replicas to handle it.
   */
  NO_REPLICAS_AVAILABLE;

  /**
   * All the instances of this Enum should have the same dimension name.
   * Refer {@link VeniceDimensionInterface#getDimensionName()} for more details.
   */
  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VENICE_REQUEST_REJECTION_REASON;
  }
}
