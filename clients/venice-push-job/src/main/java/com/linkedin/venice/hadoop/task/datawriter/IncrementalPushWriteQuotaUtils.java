package com.linkedin.venice.hadoop.task.datawriter;

import static com.linkedin.venice.vpj.VenicePushJobConstants.INCREMENTAL_PUSH_WRITE_QUOTA_RECORDS_PER_SECOND;

import com.linkedin.venice.exceptions.VeniceException;


/**
 * Driver-side helper for the incremental-push write quota.
 *
 * <p>The configured quota ({@code incremental.push.write.quota.records.per.second}) is a single global job/store
 * budget. Because partition-writer tasks run independently in distributed executors with no shared state, the budget
 * is enforced by a static even split: each of the {@code partitionCount} tasks gets
 * {@code globalRecordsPerSecond / partitionCount}. That division is computed once here, on the driver, and forwarded to
 * the tasks so enforcement never re-derives (or disagrees about) the split.
 */
public final class IncrementalPushWriteQuotaUtils {
  /** Sentinel returned (and forwarded to tasks) when incremental-push throttling does not apply. */
  public static final long THROTTLING_DISABLED = -1;

  private IncrementalPushWriteQuotaUtils() {
  }

  /**
   * Compute the per-partition-writer quota for a push, validating on the driver so a misconfiguration fails fast before
   * the data-writer job is launched.
   *
   * <p>Returns {@link #THROTTLING_DISABLED} when throttling does not apply: batch pushes, incremental pushes to a
   * separate real-time topic (both the push-job and store configs are enabled), or a non-positive global quota.
   * Otherwise returns {@code globalRecordsPerSecond / partitionCount}.
   *
   * @throws VeniceException if a positive quota cannot be split into at least 1 record/sec per partition-writer task
   *         (i.e. {@code partitionCount <= 0}, or {@code globalRecordsPerSecond < partitionCount}).
   */
  public static long perPartitionQuotaForPush(
      boolean isIncrementalPush,
      boolean pushToSeparateRealtimeTopicEnabled,
      boolean versionSeparateRealTimeTopicEnabled,
      long globalRecordsPerSecond,
      int partitionCount) {
    boolean throttlingApplies =
        isIncrementalPush && !(pushToSeparateRealtimeTopicEnabled && versionSeparateRealTimeTopicEnabled);
    if (!throttlingApplies || globalRecordsPerSecond <= 0) {
      return THROTTLING_DISABLED;
    }
    if (partitionCount <= 0) {
      throw new VeniceException(
          "Cannot split the incremental-push write quota across a non-positive partition count " + partitionCount);
    }
    long perPartitionRecordsPerSecond = globalRecordsPerSecond / partitionCount;
    if (perPartitionRecordsPerSecond <= 0) {
      throw new VeniceException(
          "Incremental-push write quota " + globalRecordsPerSecond + "/sec is smaller than the " + partitionCount
              + " partition writers sharing it; each task would get 0/sec. Raise "
              + INCREMENTAL_PUSH_WRITE_QUOTA_RECORDS_PER_SECOND + " to at least the partition count.");
    }
    return perPartitionRecordsPerSecond;
  }
}
