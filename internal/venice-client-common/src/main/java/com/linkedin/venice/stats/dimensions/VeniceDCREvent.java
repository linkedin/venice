package com.linkedin.venice.stats.dimensions;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_DCR_EVENT;


/**
 * Dimension to represent deterministic conflict resolution (DCR) events.
 */
public enum VeniceDCREvent implements VeniceDimensionInterface {
  /** An update was ignored due to conflict resolution rules */
  UPDATE_IGNORED,
  /** A tombstone (delete marker) was created during conflict resolution */
  TOMBSTONE_CREATION,
  /** A timestamp regression error occurred during conflict resolution */
  TIMESTAMP_REGRESSION_ERROR,
  /** An offset regression error occurred during conflict resolution */
  OFFSET_REGRESSION_ERROR;

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
