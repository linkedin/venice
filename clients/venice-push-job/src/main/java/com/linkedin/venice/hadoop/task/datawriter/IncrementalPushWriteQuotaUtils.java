package com.linkedin.venice.hadoop.task.datawriter;

import static com.linkedin.venice.vpj.VenicePushJobConstants.INCREMENTAL_PUSH_WRITE_QUOTA_RECORDS_PER_SECOND;

import com.linkedin.venice.exceptions.VeniceException;


/**
 * Utilities for enforcing the incremental-push write quota as one global job/store budget.
 *
 * <p>Partition-writer tasks run independently in distributed executors, so the global quota is enforced by a static
 * even split: every partition-writer task gets {@code globalRecordsPerSecond / partitionCount}. This keeps the
 * aggregate write rate across all partition writers at or below the configured global quota.
 */
public final class IncrementalPushWriteQuotaUtils {
  private IncrementalPushWriteQuotaUtils() {
  }

  /**
   * Validate that a configured positive global quota can be split into at least 1 record/sec per partition-writer task.
   * Non-positive quotas disable throttling and are considered valid.
   */
  public static void validateQuota(long globalRecordsPerSecond, int partitionCount) {
    if (globalRecordsPerSecond <= 0) {
      return;
    }
    if (partitionCount <= 0) {
      throw new VeniceException(
          "Cannot split the incremental-push write quota across a non-positive partition count " + partitionCount);
    }
    if (globalRecordsPerSecond / partitionCount <= 0) {
      throw new VeniceException(
          "Incremental-push write quota " + globalRecordsPerSecond + "/sec is smaller than the " + partitionCount
              + " partition writers sharing it; each task would get 0/sec. Raise "
              + INCREMENTAL_PUSH_WRITE_QUOTA_RECORDS_PER_SECOND + " to at least the partition count.");
    }
  }

  /** Return the local per-partition-writer quota for an already-validated positive global quota. */
  public static long getPerPartitionRecordsPerSecond(long globalRecordsPerSecond, int partitionCount) {
    return globalRecordsPerSecond / partitionCount;
  }
}
