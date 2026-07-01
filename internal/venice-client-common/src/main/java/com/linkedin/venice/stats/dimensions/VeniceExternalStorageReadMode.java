package com.linkedin.venice.stats.dimensions;

/**
 * Dimension values for the {@link VeniceMetricsDimensions#VENICE_EXTERNAL_STORAGE_READ_MODE}
 * dimension, mirroring {@link com.linkedin.venice.meta.ExternalStorageReadMode} for OTel emission.
 *
 * <p>Used by client-side stats to tag metrics with the active read mode (e.g., to break down
 * read counts, failures, and latency by mode).
 *
 * @see com.linkedin.venice.meta.ExternalStorageReadMode
 */
public enum VeniceExternalStorageReadMode implements VeniceDimensionInterface {
  /** All reads served from Venice local storage. */
  VENICE_ONLY,
  /** All user-data reads served from the external storage system. */
  EXTERNAL_ONLY,
  /** Reads served from both Venice and external storage in parallel; external result used for correctness comparison only. */
  DUAL_MODE_CONSISTENCY_CHECK,
  /** Reads served from both Venice and external storage in parallel; first response wins. */
  DUAL_MODE_EARLY_RETURN;

  /**
   * All instances of this enum share the same dimension name.
   * Refer to {@link VeniceDimensionInterface#getDimensionName()} for more details.
   */
  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.VENICE_EXTERNAL_STORAGE_READ_MODE;
  }
}
