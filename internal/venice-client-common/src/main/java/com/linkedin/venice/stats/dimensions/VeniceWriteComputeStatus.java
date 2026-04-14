package com.linkedin.venice.stats.dimensions;

/**
 * Dimension to represent whether write compute (partial update) is enabled for a store.
 * Used for SLO tier classification: write compute stores have higher latency due to
 * read-merge-write on leaders.
 */
public enum VeniceWriteComputeStatus implements VeniceDimensionInterface {
  /** Write compute (partial update) is enabled for this store. */
  WRITE_COMPUTE_ENABLED,
  /** Write compute is not enabled; store uses full PUT only. */
  WRITE_COMPUTE_DISABLED;

  /**
   * All the instances of this Enum should have the same dimension name.
   * Refer {@link VeniceDimensionInterface#getDimensionName()} for more details.
   */
  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.VENICE_WRITE_COMPUTE_STATUS;
  }
}
