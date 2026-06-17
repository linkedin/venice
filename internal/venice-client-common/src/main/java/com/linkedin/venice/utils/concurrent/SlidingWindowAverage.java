package com.linkedin.venice.utils.concurrent;

import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;


/**
 * Sliding-window arithmetic mean over two tumbling sub-buckets.
 *
 * <h3>Why two buckets?</h3>
 * A single tumbling bucket resets abruptly at every boundary — the average drops to NaN even
 * under steady load. Two buckets eliminate this cliff edge: at any instant you see between one
 * and two full buckets of data. {@code windowMs} is tunable; shorter = more responsive but
 * noisier, longer = smoother but lagging.
 *
 * <h3>Write path — {@link #record}: O(1), lock-free</h3>
 * One {@link LongAdder#increment()} and one {@link DoubleAdder#add(double)} into the active
 * bucket. Rotation fires at most once per {@code windowMs / 2} under a short
 * {@code synchronized} block — never on the per-record hot path.
 *
 * <h3>Read path — {@link #average}: O(cells), cached</h3>
 * A full recompute merges two pairs of {@link LongAdder}/{@link DoubleAdder} cells (O(cells),
 * typically ≤ 32 per adder on a busy JVM). Because {@code average()} is called on the
 * per-request routing hot path, the result is cached for {@value #CACHE_MS} ms via a single
 * volatile reference to an immutable {@link CachedAverage} object — one volatile load on the
 * fast path. Staleness is negligible relative to the sub-bucket duration ({@code windowMs / 2}).
 */
public class SlidingWindowAverage {
  static final long CACHE_MS = 100;

  private final long bucketMs;
  private final Time time;
  private final long cacheMs;
  private final Object rotateLock = new Object();
  private volatile Bucket current;
  private volatile Bucket previous;

  /* Single volatile reference — one load on the hot path, atomic update via reference swap.
   * NaN avg signals "no data": the cache is bypassed so new records are seen immediately. */
  private volatile CachedAverage cache = CachedAverage.EMPTY;

  private static final class CachedAverage {
    static final CachedAverage EMPTY = new CachedAverage(Double.NaN, 0);

    final double avg;
    final long refreshMs;

    CachedAverage(double avg, long refreshMs) {
      this.avg = avg;
      this.refreshMs = refreshMs;
    }

    boolean isValid(long now, long ttlMs) {
      return !Double.isNaN(avg) && now - refreshMs < ttlMs;
    }
  }

  private static final class Bucket {
    final long startMs;
    final LongAdder count = new LongAdder();
    final DoubleAdder sum = new DoubleAdder();

    Bucket(long startMs) {
      this.startMs = startMs;
    }
  }

  public SlidingWindowAverage(long windowMs) {
    this(windowMs, new SystemTime(), CACHE_MS);
  }

  public SlidingWindowAverage(long windowMs, Time time) {
    this(windowMs, time, CACHE_MS);
  }

  public SlidingWindowAverage(long windowMs, Time time, long cacheMs) {
    if (windowMs <= 0) {
      throw new IllegalArgumentException("windowMs must be > 0, got " + windowMs);
    }
    if (time == null) {
      throw new IllegalArgumentException("time must not be null");
    }
    this.bucketMs = Math.max(1, windowMs / 2);
    this.time = time;
    this.cacheMs = cacheMs;
    long now = time.getMilliseconds();
    this.current = new Bucket(now);
    this.previous = new Bucket(now - bucketMs);
  }

  public void record(double value) {
    Bucket b = maybeRotate();
    b.count.increment();
    b.sum.add(value);
  }

  /**
   * @return arithmetic mean across the current sliding window, or {@link Double#NaN} when no
   *         observations have been recorded. Result is cached for {@value #CACHE_MS} ms; NaN
   *         is never cached so new records arriving in an empty window are seen immediately.
   */
  public double average() {
    long now = time.getMilliseconds();
    CachedAverage c = cache; // single volatile load
    if (c.isValid(now, cacheMs)) {
      return c.avg;
    }
    maybeRotate();
    double count = current.count.sum() + previous.count.sum();
    double avg = count == 0 ? Double.NaN : (current.sum.sum() + previous.sum.sum()) / count;
    // Atomic reference swap — cache update is always consistent (readers see full CachedAverage).
    // NaN is not cached: leave cache as EMPTY so the next call recomputes and picks up new records.
    if (!Double.isNaN(avg)) {
      cache = new CachedAverage(avg, now);
    }
    return avg;
  }

  private Bucket maybeRotate() {
    long now = time.getMilliseconds();
    Bucket cur = current;
    if (now - cur.startMs < bucketMs) {
      return cur;
    }
    synchronized (rotateLock) {
      cur = current;
      long age = now - cur.startMs;
      if (age < bucketMs) {
        return cur;
      }
      if (age >= 2 * bucketMs) {
        previous = new Bucket(now - bucketMs);
        current = new Bucket(now);
      } else {
        previous = cur;
        current = new Bucket(now);
      }
      return current;
    }
  }
}
