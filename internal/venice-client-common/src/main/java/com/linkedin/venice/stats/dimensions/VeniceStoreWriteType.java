package com.linkedin.venice.stats.dimensions;

/**
 * Dimension to classify a store's write type for SLO tier classification.
 * Partial update (write compute) stores have higher latency due to read-merge-write on leaders.
 */
public enum VeniceStoreWriteType implements VeniceDimensionInterface {
  /** Store uses regular full-value writes. */
  REGULAR,
  /** Store uses write compute (partial update / read-merge-write on leaders). */
  WRITE_COMPUTE;

  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.VENICE_STORE_WRITE_TYPE;
  }
}
