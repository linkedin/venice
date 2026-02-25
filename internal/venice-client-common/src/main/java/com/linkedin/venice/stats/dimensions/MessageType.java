package com.linkedin.venice.stats.dimensions;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_MESSAGE_TYPE;


public enum MessageType implements VeniceDimensionInterface {
  REQUEST, RESPONSE;

  /**
   * All the instances of this Enum should have the same dimension name.
   * Refer {@link VeniceDimensionInterface#getDimensionName()} for more details.
   */
  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VENICE_MESSAGE_TYPE;
  }
}
