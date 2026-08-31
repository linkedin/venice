package com.linkedin.venice.stats.dimensions;

/**
 * Dimension to classify a store-version's replication mode for SLO tier classification.
 * Active-active stores incur cross-region conflict-resolution overhead on writes, which is not
 * present for non-AA (single-writer-region) stores; separating the two allows different SLO
 * thresholds to be applied per tier.
 */
public enum VeniceReplicationMode implements VeniceDimensionInterface {
  /** Store-version does not have active-active replication enabled. */
  NON_ACTIVE_ACTIVE,
  /** Store-version has active-active replication enabled. */
  ACTIVE_ACTIVE;

  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.VENICE_REPLICATION_MODE;
  }
}
