package com.linkedin.venice.stats.dimensions;

/** Generic operation outcome: success or failure. */
public enum VeniceOperationOutcome implements VeniceDimensionInterface {
  /** Operation completed successfully. */
  SUCCESS,
  /** Operation failed. */
  FAIL;

  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.VENICE_OPERATION_OUTCOME;
  }
}
