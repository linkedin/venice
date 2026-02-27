package com.linkedin.venice.stats.dimensions;

/**
 * Dimension to represent whether a value was assembled from multiple storage chunks (large value)
 * or retrieved as a single chunk (small value).
 */
public enum VeniceChunkingStatus implements VeniceDimensionInterface {
  /** Value was assembled from multiple storage chunks (large value). */
  CHUNKED,
  /** Value was retrieved as a single chunk (small value). */
  UNCHUNKED;

  /**
   * All the instances of this Enum should have the same dimension name.
   * Refer {@link VeniceDimensionInterface#getDimensionName()} for more details.
   */
  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.VENICE_CHUNKING_STATUS;
  }
}
