package com.linkedin.venice.stats.dimensions;

/**
 * Dimension to classify a store's write type for SLO tier classification.
 * Partial update (write compute) stores have higher latency due to read-merge-write on leaders.
 */
public enum VeniceStoreWriteType implements VeniceDimensionInterface {
  /** Store uses full PUT (regular writes). */
  REGULAR_PUT,
  /** Store uses partial update (write compute). */
  PARTIAL_UPDATE;

  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.VENICE_STORE_WRITE_TYPE;
  }
}
