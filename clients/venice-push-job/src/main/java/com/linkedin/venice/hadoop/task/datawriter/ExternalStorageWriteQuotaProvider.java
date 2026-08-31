package com.linkedin.venice.hadoop.task.datawriter;

/**
 * Supplies the per-region external-storage write quota — a records/sec and a bytes/sec budget — for a push.
 * This isolates <em>where the quota comes from</em> from <em>how the throttler enforces it</em>
 * ({@link ExternalStorageWriteThrottler}), so the source can evolve (e.g. derived from a store's VU quota, or
 * looked up from a service) without touching the enforcement path. Today the only implementation reads static
 * VPJ config ({@link ConfigBackedExternalStorageWriteQuotaProvider}).
 *
 * <p>Both dimensions are independent and {@code <= 0} means that dimension is unthrottled.
 */
interface ExternalStorageWriteQuotaProvider {
  /** Per-region record-write quota in records/sec; {@code <= 0} disables record-rate throttling. */
  long getRecordRatePerSecond();

  /** Per-region byte-write quota in bytes/sec; {@code <= 0} disables byte-rate throttling. */
  long getByteRatePerSecond();
}
