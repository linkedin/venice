package com.linkedin.venice.stats.dimensions;

/**
 * Outcome of a read quota enforcement decision: whether the request was allowed,
 * rejected, or allowed without quota check (unintentionally).
 */
public enum QuotaRequestOutcome implements VeniceDimensionInterface {
  /** Request passed quota check and was served normally. */
  ALLOWED,

  /** Request exceeded the store-version quota and was rejected. */
  REJECTED,

  /**
   * Request was allowed without a quota check — either the quota enforcer had not yet
   * initialized, or no rate limiter was allocated for the requested resource.
   */
  ALLOWED_UNINTENTIONALLY;

  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.VENICE_QUOTA_REQUEST_OUTCOME;
  }
}
