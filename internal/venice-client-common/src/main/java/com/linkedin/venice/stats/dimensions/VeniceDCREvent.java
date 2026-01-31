package com.linkedin.venice.stats.dimensions;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_DCR_EVENT;


/**
 * Dimension to represent deterministic conflict resolution (DCR) events.
 */
public enum VeniceDCREvent implements VeniceDimensionInterface {
  UPDATE_IGNORED, TOMBSTONE_CREATION, TIMESTAMP_REGRESSION_ERROR, OFFSET_REGRESSION_ERROR;

  private final String event;

  VeniceDCREvent() {
    this.event = name().toLowerCase();
  }

  /**
   * All the instances of this Enum should have the same dimension name.
   * Refer {@link VeniceDimensionInterface#getDimensionName()} for more details.
   */
  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VENICE_DCR_EVENT;
  }

  @Override
  public String getDimensionValue() {
    return event;
  }
}
