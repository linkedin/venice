package com.linkedin.venice.stats.dimensions;

/**
 * Dimension enum representing the result of a Data Integrity Validation (DIV) check on a message.
 * Maps to {@link VeniceMetricsDimensions#VENICE_DIV_RESULT}.
 */
public enum VeniceDIVResult implements VeniceDimensionInterface {
  /** Message passed all DIV checks (sequence number and checksum validation) successfully. */
  SUCCESS,
  /** Incoming sequence number is equal to or smaller than the previous — message already consumed. */
  DUPLICATE,
  /** Incoming sequence number is greater than expected — one or more messages were skipped. */
  MISSING,
  /** Running checksum mismatch detected at end-of-segment — segment data is corrupt. */
  CORRUPTED;

  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.VENICE_DIV_RESULT;
  }
}
