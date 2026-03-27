package com.linkedin.venice.stats.dimensions;

/**
 * Dimension enum for drainer type: sorted (in-order) or unsorted record processing.
 */
public enum VeniceDrainerType implements VeniceDimensionInterface {
  SORTED, UNSORTED;

  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.VENICE_DRAINER_TYPE;
  }
}
