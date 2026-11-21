package com.linkedin.venice.stats.dimensions;

/** Write operation type issued through the Venice online producer. */
public enum VeniceProducerOperation implements VeniceDimensionInterface {
  /** Full record put. */
  PUT,
  /** Record delete. */
  DELETE,
  /** Partial update (write compute). */
  UPDATE;

  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.VENICE_PRODUCER_OPERATION;
  }
}
