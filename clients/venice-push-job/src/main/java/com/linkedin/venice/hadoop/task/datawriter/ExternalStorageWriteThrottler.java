package com.linkedin.venice.hadoop.task.datawriter;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.throttle.GuavaRateLimiter;
import com.linkedin.venice.throttle.VeniceRateLimiter;


/**
 * Per-(partition-writer task, region) rate limiter for the VPJ external-storage dual-write path. A single
 * configured <em>global</em> record rate and/or byte rate is the budget for writing to one external region;
 * because partition-writer tasks run in separate distributed executors with no shared memory, the budget is
 * enforced by static even split: every task gets a local throttler sized to {@code globalRate / partitionCount}
 * so the aggregate across all {@code partitionCount} tasks for that region does not exceed the global rate.
 *
 * <p>Each region gets its own {@code ExternalStorageWriteThrottler} instance (independent buckets): the
 * dual-write fan-out writes the same batch to every region sequentially, so a shared bucket would let two
 * regions drain one budget and halve each region's effective rate. Per-region instances keep each region at
 * its full per-region budget.
 *
 * <p>The two dimensions are independent: either can be configured alone (the other limiter is {@code null} and
 * that dimension is unthrottled). Both use a blocking {@link VeniceRateLimiter} — a push must slow down to fit
 * the budget, never drop records.
 */
public class ExternalStorageWriteThrottler {
  private final VeniceRateLimiter recordRateLimiter;
  private final VeniceRateLimiter byteRateLimiter;

  ExternalStorageWriteThrottler(VeniceRateLimiter recordRateLimiter, VeniceRateLimiter byteRateLimiter) {
    if (recordRateLimiter == null && byteRateLimiter == null) {
      throw new IllegalArgumentException(
          "ExternalStorageWriteThrottler requires at least one of the record/byte rate limiters to be non-null");
    }
    this.recordRateLimiter = recordRateLimiter;
    this.byteRateLimiter = byteRateLimiter;
  }

  /**
   * Block until both configured budgets admit a batch of {@code recordCount} records totalling
   * {@code byteCount} bytes. A dimension whose limiter is {@code null} is not enforced. Called once per region
   * per batch, immediately before that region's {@code batchPut}.
   */
  void throttle(int recordCount, long byteCount) {
    if (recordRateLimiter != null) {
      recordRateLimiter.acquirePermit(recordCount);
    }
    if (byteRateLimiter != null) {
      acquire(byteRateLimiter, byteCount);
    }
  }

  /**
   * Charge {@code permits} to {@code limiter}. {@link VeniceRateLimiter#acquirePermit(int)} takes an int, so a
   * batch whose byte count exceeds {@link Integer#MAX_VALUE} is charged in {@code Integer.MAX_VALUE} chunks
   * rather than silently clamped — the byte budget is honored exactly even for very large batches.
   */
  private static void acquire(VeniceRateLimiter limiter, long permits) {
    while (permits > Integer.MAX_VALUE) {
      limiter.acquirePermit(Integer.MAX_VALUE);
      permits -= Integer.MAX_VALUE;
    }
    if (permits > 0) {
      limiter.acquirePermit((int) permits);
    }
  }

  /**
   * Build a throttler giving this one partition-writer task an even {@code 1/partitionCount} slice of each
   * configured global rate, or {@code null} when neither dimension is configured (throttling off for this
   * region). A dimension with rate {@code <= 0} is disabled.
   */
  static ExternalStorageWriteThrottler create(
      long globalRecordRatePerSecond,
      long globalByteRatePerSecond,
      int partitionCount) {
    VeniceRateLimiter recordRateLimiter = perTaskRateLimiter(globalRecordRatePerSecond, partitionCount, "records");
    VeniceRateLimiter byteRateLimiter = perTaskRateLimiter(globalByteRatePerSecond, partitionCount, "bytes");
    if (recordRateLimiter == null && byteRateLimiter == null) {
      return null;
    }
    return new ExternalStorageWriteThrottler(recordRateLimiter, byteRateLimiter);
  }

  /**
   * Validate that every configured ({@code > 0}) dimension can be split into at least 1/sec per task for
   * {@code partitionCount} tasks, throwing {@link VeniceException} otherwise. Allocates nothing, so the VPJ
   * driver can call it to fail a misconfigured push <em>before</em> launching the data-writer job (and its
   * cluster resources). Partition count is only known after version creation, so the earliest the driver can
   * call this is right after the version is created, not before topic creation.
   */
  public static void validateQuota(long globalRecordRatePerSecond, long globalByteRatePerSecond, int partitionCount) {
    requireSplittable(globalRecordRatePerSecond, partitionCount, "records");
    requireSplittable(globalByteRatePerSecond, partitionCount, "bytes");
  }

  /**
   * The per-task slice of one global dimension, or {@code null} when that dimension is disabled
   * ({@code globalRatePerSecond <= 0}).
   */
  private static VeniceRateLimiter perTaskRateLimiter(long globalRatePerSecond, int partitionCount, String dimension) {
    if (globalRatePerSecond <= 0) {
      return null;
    }
    requireSplittable(globalRatePerSecond, partitionCount, dimension);
    return new GuavaRateLimiter(globalRatePerSecond / partitionCount);
  }

  /**
   * @throws VeniceException if a configured ({@code > 0}) global rate cannot give each of {@code partitionCount}
   *         tasks at least 1/sec — failing fast on a misconfiguration beats a 0/sec limiter that deadlocks the push.
   */
  private static void requireSplittable(long globalRatePerSecond, int partitionCount, String dimension) {
    if (globalRatePerSecond <= 0) {
      return;
    }
    if (partitionCount <= 0) {
      throw new VeniceException(
          "Cannot split the external-storage write " + dimension + " quota across a non-positive partition count "
              + partitionCount);
    }
    if (globalRatePerSecond / partitionCount <= 0) {
      throw new VeniceException(
          "External-storage write " + dimension + " quota " + globalRatePerSecond + "/sec is smaller than the "
              + partitionCount + " partition writers sharing it; each task would get 0/sec. Raise the quota to at "
              + "least the partition count.");
    }
  }

  /** True if this throttler enforces a byte-rate budget. Lets callers skip the byte sum when only records are limited. */
  boolean enforcesByteRate() {
    return byteRateLimiter != null;
  }

  // Visible for testing.
  VeniceRateLimiter getRecordRateLimiter() {
    return recordRateLimiter;
  }

  VeniceRateLimiter getByteRateLimiter() {
    return byteRateLimiter;
  }
}
