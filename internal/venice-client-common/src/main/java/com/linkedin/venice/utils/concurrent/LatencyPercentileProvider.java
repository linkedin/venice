package com.linkedin.venice.utils.concurrent;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;


/**
 * Independent percentile provider for read-path latencies. Owns its backing state so
 * signal services can produce a throttling signal without depending on Tehuti's windowed
 * {@link io.tehuti.metrics.stats.Percentiles} computation — Tehuti {@code Percentiles} can
 * only be read via {@code MetricsRepository.getMetric()}, making the signal unavailable when
 * Tehuti is removed. This class provides the same p99 signal with no metrics-library dependency.
 *
 * <p>Each {@link LatencyType} keeps a fixed-size sample reservoir (ring buffer). Observations
 * overwrite the oldest sample once the buffer is full — this approximates a moving window for
 * high-throughput workloads where the buffer fills quickly relative to the throttler signal
 * refresh interval. Percentile reads snapshot the reservoir, sort, and index.
 *
 * <h3>Write path — {@link #observe}: O(1), lock-free</h3>
 * One {@link AtomicLong#getAndIncrement()} to claim the next slot + one
 * {@link AtomicLongArray#set} to store the value (encoded via
 * {@link Double#doubleToRawLongBits} — {@code AtomicLongArray} has no {@code double} variant
 * in the JDK). No locks, no CAS retry loop. Safe for concurrent writers at any request rate.
 *
 * <h3>Read path — {@link #getP99}: O(n log n), called only periodically</h3>
 * Snapshots all {@code capacity} slots (default {@value #DEFAULT_RESERVOIR_CAPACITY}), sorts
 * them, and returns the value at the 99th-percentile index. At the default capacity this takes
 * ~50–100 µs on modern hardware (fits in L2 cache). Crucially, {@code getP99} is only ever
 * called on a fixed periodic schedule (default every 30 s), so the sort cost is amortised
 * across millions of writes and is negligible in practice.
 *
 * <p>Concurrent writes during a read are tolerated — the signal is an approximation and a few
 * samples changing underfoot does not meaningfully skew a p99 over thousands of observations.
 */
public class LatencyPercentileProvider {
  /** Read-path latency categories observed by this provider. */
  public enum LatencyType {
    SINGLE_GET, MULTI_GET, READ_COMPUTE
  }

  /** Default reservoir capacity per {@link LatencyType} — large enough to stabilize p99 under load. */
  public static final int DEFAULT_RESERVOIR_CAPACITY = 4096;

  private final Reservoir[] reservoirs;

  public LatencyPercentileProvider() {
    this(DEFAULT_RESERVOIR_CAPACITY);
  }

  public LatencyPercentileProvider(int reservoirCapacity) {
    if (reservoirCapacity <= 0) {
      throw new IllegalArgumentException("reservoirCapacity must be > 0, got " + reservoirCapacity);
    }
    LatencyType[] types = LatencyType.values();
    this.reservoirs = new Reservoir[types.length];
    for (int i = 0; i < types.length; i++) {
      this.reservoirs[i] = new Reservoir(reservoirCapacity);
    }
  }

  public void observe(LatencyType type, double latencyMs) {
    reservoirs[type.ordinal()].observe(latencyMs);
  }

  /**
   * @return 99th percentile of observations in the reservoir for {@code type}, or 0 when no
   *         samples have been recorded yet. Returning 0 (rather than NaN) keeps the threshold
   *         comparison semantics of callers: signal stays inactive when the reservoir is empty.
   */
  public double getP99(LatencyType type) {
    return reservoirs[type.ordinal()].percentile(99.0);
  }

  private static final class Reservoir {
    private final int capacity;
    private final AtomicLongArray samples;
    private final AtomicLong nextIndex = new AtomicLong();

    Reservoir(int capacity) {
      this.capacity = capacity;
      this.samples = new AtomicLongArray(capacity);
    }

    void observe(double value) {
      long idx = nextIndex.getAndIncrement();
      samples.set((int) Math.floorMod(idx, capacity), Double.doubleToRawLongBits(value));
    }

    double percentile(double p) {
      long total = nextIndex.get();
      if (total <= 0) {
        return 0.0;
      }
      int size = (int) Math.min(total, (long) capacity);
      double[] copy = new double[size];
      for (int i = 0; i < size; i++) {
        copy[i] = Double.longBitsToDouble(samples.get(i));
      }
      Arrays.sort(copy);
      int idx = (int) Math.min(size - 1L, Math.max(0L, Math.round((p / 100.0) * (size - 1))));
      return copy[idx];
    }
  }
}
