package com.linkedin.venice.stats.dimensions;

/** DaVinci record transformer operation type. */
public enum VeniceRecordTransformerOperation implements VeniceDimensionInterface {
  /** Record put (insert/update) operation. */
  PUT,
  /** Record delete operation. */
  DELETE;

  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.VENICE_RECORD_TRANSFORMER_OPERATION;
  }
}
