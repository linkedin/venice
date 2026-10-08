package com.linkedin.venice.stats.dimensions;

/**
 * Push duration relative to the SLA threshold configured by
 * {@link com.linkedin.venice.ConfigKeys#CONTROLLER_PUSH_JOB_SLA_MS}, independent of the push's execution state.
 * Used only for terminal push-job statuses, for which the job duration is nonnegative.
 *
 * <p>Use alongside {@link VeniceMetricsDimensions#VENICE_PUSH_JOB_EXECUTION_STATE} when evaluating completion
 * SLAs. For example, a user-cancelled push with execution state {@code killed} and bucket {@link #UNDER_SLA}
 * was stopped before Venice's configured completion window elapsed. Such an early user cancellation should
 * not be attributed to Venice failing to meet its completion SLA.
 */
public enum VenicePushJobDurationBucket implements VeniceDimensionInterface {
  /** A push duration strictly below the configured SLA threshold. */
  UNDER_SLA,

  /** A push duration at or above the configured SLA threshold. */
  AT_OR_OVER_SLA;

  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.VENICE_PUSH_JOB_DURATION_BUCKET;
  }
}
