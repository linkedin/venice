package com.linkedin.venice.stats.dimensions;

/**
 * Dimension to represent whether partial update (write compute) is enabled for a store.
 * Used for SLO tier classification: partial update stores have higher latency due to
 * read-merge-write on leaders.
 */
public enum VenicePartialUpdateStatus implements VeniceDimensionInterface {
  /** Partial update (write compute) is enabled for this store. */
  PARTIAL_UPDATE_ENABLED,
  /** Partial update is not enabled; store uses full PUT only. */
  PARTIAL_UPDATE_DISABLED;

  /**
   * All the instances of this Enum should have the same dimension name.
   * Refer {@link VeniceDimensionInterface#getDimensionName()} for more details.
   */
  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.VENICE_PARTIAL_UPDATE_STATUS;
  }
}
