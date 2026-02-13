package com.linkedin.venice.stats.dimensions;

/**
 * Dimension enum representing the type of Venice push operation. Used to differentiate
 * metrics (e.g., push job counts, push start counts) by how data is ingested into Venice.
 *
 * Maps to {@link VeniceMetricsDimensions#VENICE_PUSH_JOB_TYPE}.
 */
public enum VenicePushType implements VeniceDimensionInterface {
  /** A full batch push that creates a new store version with complete dataset replacement. */
  BATCH("batch"),

  /**
   * An incremental push that appends or updates individual records in the current store version
   * without replacing the entire dataset. Writes go to the real-time topic.
   */
  INCREMENTAL("incremental");

  private final String dimensionValue;

  VenicePushType(String dimensionValue) {
    this.dimensionValue = dimensionValue;
  }

  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.VENICE_PUSH_JOB_TYPE;
  }

  @Override
  public String getDimensionValue() {
    return dimensionValue;
  }
}
