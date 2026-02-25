package com.linkedin.venice.stats.dimensions;

/**
 * Dimension enum representing the terminal status of a Venice push job. Used to categorize
 * push job completions into success, user-caused failures, and system-caused failures within
 * a single consolidated OTel counter metric.
 *
 * <p>The classification of failures into user vs system error is determined by the push job's
 * latest checkpoint. Checkpoints listed in {@code DEFAULT_PUSH_JOB_USER_ERROR_CHECKPOINTS}
 * (e.g., quota exceeded, write ACL failure, duplicate keys, schema validation failures,
 * record too large, concurrent batch push, disk full, memory limit) are classified as
 * {@link #USER_ERROR}. All other failure checkpoints are classified as {@link #SYSTEM_ERROR}.
 *
 * Maps to {@link VeniceMetricsDimensions#VENICE_PUSH_JOB_STATUS}.
 */
public enum VenicePushJobStatus implements VeniceDimensionInterface {
  /** The push job completed successfully and data is available for serving. */
  SUCCESS,

  /**
   * The push job failed due to a user-caused error, such as quota exceeded, write ACL failure,
   * duplicate keys with different values, schema validation failure, record too large, concurrent
   * batch push, dataset changed, invalid input file, DaVinci disk full, or DaVinci memory limit.
   */
  USER_ERROR,

  /**
   * The push job failed due to a system-caused error, such as infrastructure failures, internal
   * service errors, or any failure checkpoint not classified as a user error.
   */
  SYSTEM_ERROR;

  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.VENICE_PUSH_JOB_STATUS;
  }
}
