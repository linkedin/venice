package com.linkedin.venice.stats.routing;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.DoubleSupplier;
import java.util.function.IntToDoubleFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Latency-adaptive weighted group selection, shared by the router and fast-client routing strategies. Given each
 * group's measured average latency it picks a group by a weighted-random draw whose skew adapts to the slowest group's
 * latency: even routing while every group is fast, steering share onto the faster groups as the slowest group slows.
 *
 * <pre>
 *   strength(g) = 1 / max(latency(g), MIN_LATENCY_MS)                     // faster group => stronger
 *   slowest     = max(latency)                  over measured groups (ms)
 *   skew        = 0                                          if slowest &lt;= evenUntilLatencyMs
 *                 1                                          if slowest &gt;= fullSkewAtLatencyMs
 *                 ((slowest - evenUntil) / (fullSkew - evenUntil))^m  otherwise
 *   share(g)    = (1 - skew) * (1 / G)  +  skew * strength(g) / sum(strength)
 *   share(g)    = max(share(g), PROBE_FLOOR_FRACTION * (1 / G))           // never fully starve a group
 * </pre>
 *
 * <p>Equal-latency groups keep equal strength, so all-slow-but-equal routing stays even; an unmeasured group
 * ({@code latency <= 0}) is neutral and excluded from the skew, and the probe floor keeps every group observable.
 *
 * <p>Latency is tracked per <em>key-count bucket</em> so request size does not conflate a slow host with a large
 * request: each request maps to a bucket by key count, reading the signal and recording its sample in that bucket.
 * Bucket boundaries and per-bucket full-skew thresholds are supplied pre-parsed; {@link #bucketKeyForKeyCount(int)}
 * exposes the mapping so callers record into the bucket the selector reads. Each bucket's stay-even threshold is a
 * fixed fraction of its full-skew threshold, and a bucket under {@code minSampleCount} samples for a group falls back
 * to even routing there.
 *
 * <p>Holds no per-request state; thread-safe given thread-safe providers and {@code randomSupplier}. The only mutable
 * state is a lock-free throttle timestamp for the optional periodic weight log, which does not affect selection.
 */
public class LatencyAdaptiveGroupSelector {
  /** Maps a group and a bucket (its lower key-count bound) to its avg latency in ms; non-positive = unmeasured. */
  @FunctionalInterface
  public interface GroupBucketLatencyProvider {
    double latencyMs(int groupId, int bucketKey);
  }

  /** Maps a group and a key-count bucket to its number of latency samples, used to apply the minimum-sample floor. */
  @FunctionalInterface
  public interface GroupBucketSampleCountProvider {
    long sampleCount(int groupId, int bucketKey);
  }

  public static final int MAX_ALLOWED_GROUP = 100;

  /** Default interval (ms) between periodic per-group weight log lines at INFO; {@code <= 0} disables. */
  public static final long DEFAULT_WEIGHT_LOG_INTERVAL_MS = 2000L;

  private static final Logger LOGGER = LogManager.getLogger(LatencyAdaptiveGroupSelector.class);

  /** Default single-bucket stay-even threshold (ms): skew stays 0 while the slowest group is at or below it. */
  public static final double DEFAULT_EVEN_UNTIL_LATENCY_MS = 10.0;

  /** Default full-skew threshold (ms): routing reaches its full split (skew {@code == 1}) once the slowest hits it. */
  public static final double DEFAULT_FULL_SKEW_AT_LATENCY_MS = 30.0;

  /** Default fraction of a bucket's full-skew threshold used as its stay-even threshold (the {@code 10 / 30} ratio). */
  public static final double DEFAULT_EVEN_UNTIL_FRACTION = 1.0 / 3.0;

  /** Default minimum per-(group, bucket) samples before a bucket's average latency is acted on. */
  public static final long DEFAULT_MIN_SAMPLE_COUNT = 10L;

  /** Default in-band ramp exponent {@code m}: {@code 1.0} is linear, {@code > 1} steepens toward the full-skew end. */
  public static final double DEFAULT_INTERPOLATION_EXPONENT = 1.0;

  /** Lower bound on a group's latency before inverting it into a strength, so near-zero latency stays bounded. */
  public static final double MIN_LATENCY_MS = 1.0;

  /** Minimum share every group retains, as a fraction of the even share {@code 1 / G}, so no group is fully starved. */
  public static final double PROBE_FLOOR_FRACTION = 0.05;

  private final GroupBucketLatencyProvider latencyProvider;
  private final GroupBucketSampleCountProvider sampleCountProvider;
  /** Bucket lower bound (inclusive) -> array index into the per-bucket threshold arrays, for {@code floorEntry} lookup. */
  private final TreeMap<Integer, Integer> bucketIndexByLowerBound;
  private final int[] bucketLowerBounds;
  private final double[] bucketEvenUntilMs;
  private final double[] bucketFullSkewMs;
  private final double evenUntilFraction;
  private final long minSampleCount;
  private final double interpolationExponent;
  private final DoubleSupplier randomSupplier;
  private final AtomicLong lastWeightLogTimeNs = new AtomicLong(System.nanoTime());
  private volatile long weightLogIntervalMs = DEFAULT_WEIGHT_LOG_INTERVAL_MS;
  private volatile String logContext = "";

  /** Single-bucket selector with default knobs and {@link ThreadLocalRandom}; see the five-argument constructor. */
  public LatencyAdaptiveGroupSelector(IntToDoubleFunction latencyProvider) {
    this(
        latencyProvider,
        DEFAULT_EVEN_UNTIL_LATENCY_MS,
        DEFAULT_FULL_SKEW_AT_LATENCY_MS,
        DEFAULT_INTERPOLATION_EXPONENT,
        () -> ThreadLocalRandom.current().nextDouble());
  }

  /**
   * Single-bucket (non size-aware) selector. All key counts map to one bucket with dead-band
   * {@code [evenUntilLatencyMs, fullSkewAtLatencyMs]} and no minimum-sample floor.
   *
   * @param latencyProvider       group id -> avg response time in ms; non-positive = not measured (neutral).
   * @param evenUntilLatencyMs    stay-even threshold in ms; {@code >= 0} and {@code < fullSkewAtLatencyMs}.
   * @param fullSkewAtLatencyMs   full-skew threshold in ms; {@code > evenUntilLatencyMs}.
   * @param interpolationExponent ramp exponent {@code m} between thresholds ({@code 1.0} = linear); {@code > 0}.
   * @param randomSupplier        uniform random double in [0, 1); injectable for deterministic tests.
   */
  public LatencyAdaptiveGroupSelector(
      IntToDoubleFunction latencyProvider,
      double evenUntilLatencyMs,
      double fullSkewAtLatencyMs,
      double interpolationExponent,
      DoubleSupplier randomSupplier) {
    if (!(evenUntilLatencyMs >= 0.0 && evenUntilLatencyMs < fullSkewAtLatencyMs)) {
      throw new VeniceException(
          "Require 0 <= evenUntilLatencyMs < fullSkewAtLatencyMs, but received evenUntilLatencyMs=" + evenUntilLatencyMs
              + ", fullSkewAtLatencyMs=" + fullSkewAtLatencyMs);
    }
    if (!(interpolationExponent > 0.0)) {
      throw new VeniceException(
          "Require interpolationExponent > 0 (a non-positive exponent would push skew outside [0, 1] and distort "
              + "the share past full skew), but received interpolationExponent=" + interpolationExponent);
    }
    // One bucket covering every key count; the sample-count provider is unused since the floor is disabled.
    this.latencyProvider = (groupId, bucketKey) -> latencyProvider.applyAsDouble(groupId);
    this.sampleCountProvider = (groupId, bucketKey) -> Long.MAX_VALUE;
    this.bucketLowerBounds = new int[] { 1 };
    this.bucketEvenUntilMs = new double[] { evenUntilLatencyMs };
    this.bucketFullSkewMs = new double[] { fullSkewAtLatencyMs };
    this.bucketIndexByLowerBound = new TreeMap<>();
    this.bucketIndexByLowerBound.put(1, 0);
    this.evenUntilFraction = evenUntilLatencyMs / fullSkewAtLatencyMs;
    this.minSampleCount = 0L;
    this.interpolationExponent = interpolationExponent;
    this.randomSupplier = randomSupplier;
  }

  /**
   * Size-aware (bucketed) selector. Latency is tracked and thresholds applied per key-count bucket.
   *
   * @param latencyProvider        {@code (groupId, bucketKey)} -> avg response time in ms; non-positive = not measured.
   * @param sampleCountProvider    {@code (groupId, bucketKey)} -> sample count; below {@code minSampleCount} = unmeasured.
   * @param fullSkewMsByLowerBound bucket lower bound -> full-skew threshold ms; non-empty, starts at key 1, positive.
   * @param evenUntilFraction      fraction of a bucket's full-skew threshold used as its stay-even threshold; {@code (0, 1)}.
   * @param interpolationExponent  ramp exponent {@code m} between thresholds ({@code 1.0} = linear); {@code > 0}.
   * @param minSampleCount         minimum per-(group, bucket) samples before the average is acted on; {@code >= 0}.
   * @param randomSupplier         uniform random double in [0, 1); injectable for deterministic tests.
   */
  public LatencyAdaptiveGroupSelector(
      GroupBucketLatencyProvider latencyProvider,
      GroupBucketSampleCountProvider sampleCountProvider,
      NavigableMap<Integer, Integer> fullSkewMsByLowerBound,
      double evenUntilFraction,
      double interpolationExponent,
      long minSampleCount,
      DoubleSupplier randomSupplier) {
    if (fullSkewMsByLowerBound == null || fullSkewMsByLowerBound.isEmpty()) {
      throw new VeniceException("Require a non-empty bucket threshold map");
    }
    if (fullSkewMsByLowerBound.firstKey() != 1) {
      throw new VeniceException(
          "Require the bucket threshold map to start at key count 1, but received lowest bound "
              + fullSkewMsByLowerBound.firstKey());
    }
    if (!(evenUntilFraction > 0.0 && evenUntilFraction < 1.0)) {
      throw new VeniceException(
          "Require 0 < evenUntilFraction < 1, but received evenUntilFraction=" + evenUntilFraction);
    }
    if (!(interpolationExponent > 0.0)) {
      throw new VeniceException(
          "Require interpolationExponent > 0 (a non-positive exponent would push skew outside [0, 1] and distort "
              + "the share past full skew), but received interpolationExponent=" + interpolationExponent);
    }
    if (minSampleCount < 0L) {
      throw new VeniceException("Require minSampleCount >= 0, but received minSampleCount=" + minSampleCount);
    }
    int bucketCount = fullSkewMsByLowerBound.size();
    this.bucketLowerBounds = new int[bucketCount];
    this.bucketEvenUntilMs = new double[bucketCount];
    this.bucketFullSkewMs = new double[bucketCount];
    this.bucketIndexByLowerBound = new TreeMap<>();
    int index = 0;
    for (Map.Entry<Integer, Integer> entry: fullSkewMsByLowerBound.entrySet()) {
      int lowerBound = entry.getKey();
      double fullSkew = entry.getValue();
      if (!(fullSkew > 0.0)) {
        throw new VeniceException(
            "Require a positive full-skew threshold per bucket, but bucket starting at " + lowerBound + " has "
                + fullSkew);
      }
      this.bucketLowerBounds[index] = lowerBound;
      this.bucketFullSkewMs[index] = fullSkew;
      this.bucketEvenUntilMs[index] = fullSkew * evenUntilFraction;
      this.bucketIndexByLowerBound.put(lowerBound, index);
      ++index;
    }
    this.latencyProvider = latencyProvider;
    this.sampleCountProvider = sampleCountProvider;
    this.evenUntilFraction = evenUntilFraction;
    this.minSampleCount = minSampleCount;
    this.interpolationExponent = interpolationExponent;
    this.randomSupplier = randomSupplier;
  }

  /**
   * The inclusive lower key-count bound identifying the bucket a {@code keyCount}-key request falls into. Callers record
   * a request's sample under this same key so the selector reads a size-matched signal; below range clamps to bucket 0.
   */
  public int bucketKeyForKeyCount(int keyCount) {
    Map.Entry<Integer, Integer> entry = bucketIndexByLowerBound.floorEntry(keyCount);
    return entry == null ? bucketLowerBounds[0] : entry.getKey();
  }

  /** Single-bucket selection across all groups; {@link #selectGroup(int, int, int, int)} with no exclusion. */
  public int selectGroup(int groupCount, int startGroupId) {
    return selectGroup(groupCount, startGroupId, -1, bucketLowerBounds[0]);
  }

  /** Single-bucket weighted-random selection, optionally excluding one group. Retained for the non size-aware path. */
  public int selectGroup(int groupCount, int startGroupId, int excludedGroupId) {
    return selectGroup(groupCount, startGroupId, excludedGroupId, bucketLowerBounds[0]);
  }

  /**
   * Weighted-random reservoir selection across the groups for a request of {@code keyCount} keys, optionally excluding
   * one group. The key count selects the bucket whose latency signal and thresholds drive the draw; each candidate is
   * adopted with probability {@code share(g) / cumulativeShare}, so the final selection probability is proportional to
   * {@code share(g)} in a single pass. The scan starts at {@code startGroupId} only to avoid biasing toward group 0.
   *
   * @param groupCount      number of groups; must be in {@code [1, MAX_ALLOWED_GROUP]}.
   * @param startGroupId    group id the scan starts from (typically {@code requestId % groupCount}).
   * @param excludedGroupId group to exclude from selection and skew (e.g. the group already tried); {@code -1} excludes none.
   * @param keyCount        number of keys in the request, used to pick the key-count bucket.
   */
  public int selectGroup(int groupCount, int startGroupId, int excludedGroupId, int keyCount) {
    return selectGroup(groupCount, startGroupId, excludedGroupId, keyCount, -1.0);
  }

  /**
   * As {@link #selectGroup(int, int, int, int)} with this request's full-skew threshold ({@code <= 0} keeps the
   * bucket's; stay-even scales by the same fraction), so callers can follow a runtime threshold such as a retry policy.
   */
  public int selectGroup(int groupCount, int startGroupId, int excludedGroupId, int keyCount, double fullSkewMs) {
    if (groupCount > MAX_ALLOWED_GROUP || groupCount <= 0) {
      throw new VeniceException(
          "The valid group num must fall into this range: [1, " + MAX_ALLOWED_GROUP + "], but received: " + groupCount);
    }
    int bucketIndex = bucketIndexForKeyCount(keyCount);
    IntToDoubleFunction latencyView = latencyView(bucketIndex, groupCount);
    double evenUntil = fullSkewMs > 0.0 ? fullSkewMs * evenUntilFraction : bucketEvenUntilMs[bucketIndex];
    double fullSkew = fullSkewMs > 0.0 ? fullSkewMs : bucketFullSkewMs[bucketIndex];
    double skew = latencySkew(latencyView, groupCount, excludedGroupId, evenUntil, fullSkew);
    double evenShare = 1.0 / groupCount;
    double floor = PROBE_FLOOR_FRACTION * evenShare;
    double neutralStrength = neutralStrength(latencyView, groupCount, excludedGroupId);
    double totalStrength = totalStrength(latencyView, groupCount, neutralStrength, excludedGroupId);

    double cumulativeShare = 0.0;
    int selectedGroup = -1;
    for (int i = 0; i < groupCount; ++i) {
      int currentGroup = (i + startGroupId) % groupCount;
      if (currentGroup == excludedGroupId) {
        continue;
      }
      double share = shareForGroup(latencyView, currentGroup, evenShare, floor, skew, neutralStrength, totalStrength);
      if (share <= 0.0) {
        continue;
      }
      cumulativeShare += share;
      if (randomSupplier.getAsDouble() * cumulativeShare < share) {
        selectedGroup = currentGroup;
      }
    }
    maybeLogWeights(groupCount, bucketIndex, latencyView, fullSkewMs);
    if (selectedGroup >= 0) {
      return selectedGroup;
    }
    // Every candidate collapsed to zero (should not happen given the probe floor) or the only group was excluded: route
    // somewhere rather than drop the request.
    return startGroupId == excludedGroupId ? (startGroupId + 1) % groupCount : startGroupId;
  }

  /** Sets how often (ms) per-group weights are logged at INFO ({@code <= 0} disables); default {@link #DEFAULT_WEIGHT_LOG_INTERVAL_MS}. */
  public void setWeightLogIntervalMs(long weightLogIntervalMs) {
    this.weightLogIntervalMs = weightLogIntervalMs;
  }

  /** Sets a short identifier (e.g. {@code "router"}) included in the periodic weight log line for attribution. */
  public void setLogContext(String logContext) {
    this.logContext = logContext == null ? "" : logContext;
  }

  /** The array index of the bucket a {@code keyCount}-key request falls into; clamps to the first bucket below range. */
  private int bucketIndexForKeyCount(int keyCount) {
    Map.Entry<Integer, Integer> entry = bucketIndexByLowerBound.floorEntry(keyCount);
    return entry == null ? 0 : entry.getValue();
  }

  /**
   * Snapshots each group's latency for one bucket once, so skew, total strength and shares agree even while other threads
   * record; a group under {@code minSampleCount} samples reads as unmeasured ({@code -1}).
   */
  private IntToDoubleFunction latencyView(int bucketIndex, int groupCount) {
    int bucketKey = bucketLowerBounds[bucketIndex];
    double[] latencies = new double[groupCount];
    for (int g = 0; g < groupCount; ++g) {
      boolean unmeasured = minSampleCount > 0L && sampleCountProvider.sampleCount(g, bucketKey) < minSampleCount;
      latencies[g] = unmeasured ? -1.0 : latencyProvider.latencyMs(g, bucketKey);
    }
    return groupId -> latencies[groupId];
  }

  /** Renders the routing picture for the {@code keyCount}-key request's bucket as one log line. Pure. */
  public String formatWeights(int groupCount, int keyCount) {
    int bucketIndex = bucketIndexForKeyCount(keyCount);
    return renderWeights(groupCount, bucketIndex, latencyView(bucketIndex, groupCount), -1.0);
  }

  /** Renders bucket range, skew, thresholds, per-group latency and realized weight (no exclusion) as one line. Pure. */
  private String renderWeights(int groupCount, int bucketIndex, IntToDoubleFunction latencyView, double fullSkewMs) {
    double evenUntil = fullSkewMs > 0.0 ? fullSkewMs * evenUntilFraction : bucketEvenUntilMs[bucketIndex];
    double fullSkew = fullSkewMs > 0.0 ? fullSkewMs : bucketFullSkewMs[bucketIndex];
    double skew = latencySkew(latencyView, groupCount, -1, evenUntil, fullSkew);
    double evenShare = 1.0 / groupCount;
    double floor = PROBE_FLOOR_FRACTION * evenShare;
    double neutralStrength = neutralStrength(latencyView, groupCount, -1);
    double totalStrength = totalStrength(latencyView, groupCount, neutralStrength, -1);
    double[] shares = new double[groupCount];
    double shareSum = 0.0;
    for (int g = 0; g < groupCount; ++g) {
      shares[g] = shareForGroup(latencyView, g, evenShare, floor, skew, neutralStrength, totalStrength);
      shareSum += shares[g];
    }
    StringBuilder sb = new StringBuilder(80 + groupCount * 24);
    sb.append("LatencyAdaptiveGroupSelector weights");
    if (!logContext.isEmpty()) {
      sb.append(" [").append(logContext).append(']');
    }
    sb.append(
        String.format(
            ": bucket=%s skew=%.2f evenUntilMs=%.1f fullSkewMs=%.1f groups=%d [",
            bucketRangeLabel(bucketIndex),
            skew,
            evenUntil,
            fullSkew,
            groupCount));
    for (int g = 0; g < groupCount; ++g) {
      double latency = latencyView.applyAsDouble(g);
      double weightPct = shareSum > 0 ? shares[g] / shareSum * 100.0 : evenShare * 100.0;
      if (g > 0) {
        sb.append(", ");
      }
      sb.append('g').append(g).append(' ');
      sb.append(latency <= 0.0 ? "lat=n/a" : String.format("lat=%.1fms", latency));
      sb.append(String.format(" w=%.1f%%", weightPct));
    }
    sb.append(']');
    return sb.toString();
  }

  /** A human-readable {@code "<lower>-<upper>"} key-count range label for a bucket; the terminal bucket renders as open. */
  private String bucketRangeLabel(int bucketIndex) {
    int lower = bucketLowerBounds[bucketIndex];
    if (bucketIndex == bucketLowerBounds.length - 1) {
      return lower + "-+";
    }
    return lower + "-" + (bucketLowerBounds[bucketIndex + 1] - 1);
  }

  /**
   * Logs the current per-group weights for the active bucket at INFO at most once per {@link #weightLogIntervalMs}. A
   * lock-free compare-and-set gate lets one thread emit per window and keeps the hot selection path contention free.
   */
  private void maybeLogWeights(int groupCount, int bucketIndex, IntToDoubleFunction latencyView, double fullSkewMs) {
    long intervalMs = weightLogIntervalMs;
    if (intervalMs <= 0 || !LOGGER.isInfoEnabled()) {
      return;
    }
    long now = System.nanoTime();
    long last = lastWeightLogTimeNs.get();
    if (now - last < TimeUnit.MILLISECONDS.toNanos(intervalMs)) {
      return;
    }
    if (!lastWeightLogTimeNs.compareAndSet(last, now)) {
      // Another thread already claimed this window; skip to avoid duplicate lines.
      return;
    }
    LOGGER.info(renderWeights(groupCount, bucketIndex, latencyView, fullSkewMs));
  }

  /**
   * The skew factor in {@code [0, 1]} derived from the slowest measured, non-excluded group's latency (ms);
   * not-yet-measured groups are ignored, and with none measured the skew is 0 (even routing).
   */
  private double latencySkew(
      IntToDoubleFunction latencyView,
      int groupCount,
      int excludedGroupId,
      double evenUntilLatencyMs,
      double fullSkewAtLatencyMs) {
    double slowestLatencyMs = 0.0;
    for (int g = 0; g < groupCount; ++g) {
      if (g == excludedGroupId) {
        continue;
      }
      double latency = latencyView.applyAsDouble(g);
      if (latency <= 0.0) {
        continue;
      }
      slowestLatencyMs = Math.max(slowestLatencyMs, Math.max(latency, MIN_LATENCY_MS));
    }
    if (slowestLatencyMs <= evenUntilLatencyMs) {
      return 0.0;
    }
    if (slowestLatencyMs >= fullSkewAtLatencyMs) {
      return 1.0;
    }
    double position = (slowestLatencyMs - evenUntilLatencyMs) / (fullSkewAtLatencyMs - evenUntilLatencyMs);
    return interpolationExponent == 1.0 ? position : Math.pow(position, interpolationExponent);
  }

  /**
   * The target share for a group:
   * {@code max( (1 - skew) * evenShare + skew * strength(g) / totalStrength, floor )}; the floor keeps a slow group
   * observable rather than fully starved.
   */
  private double shareForGroup(
      IntToDoubleFunction latencyView,
      int groupId,
      double evenShare,
      double floor,
      double skew,
      double neutralStrength,
      double totalStrength) {
    double strengthShare =
        totalStrength > 0 ? strength(latencyView, groupId, neutralStrength) / totalStrength : evenShare;
    double share = (1.0 - skew) * evenShare + skew * strengthShare;
    return Math.max(share, floor);
  }

  /** A group's inferred strength: reciprocal of its measured latency (faster => stronger); unmeasured reads as neutral. */
  private double strength(IntToDoubleFunction latencyView, int groupId, double neutralStrength) {
    double latency = latencyView.applyAsDouble(groupId);
    if (latency <= 0.0) {
      return neutralStrength;
    }
    return 1.0 / Math.max(latency, MIN_LATENCY_MS);
  }

  /** The mean strength of the already-measured, non-excluded groups, used as the strength of not-yet-measured groups. */
  private double neutralStrength(IntToDoubleFunction latencyView, int groupCount, int excludedGroupId) {
    double sum = 0.0;
    int measured = 0;
    for (int g = 0; g < groupCount; ++g) {
      if (g == excludedGroupId) {
        continue;
      }
      double latency = latencyView.applyAsDouble(g);
      if (latency > 0.0) {
        sum += 1.0 / Math.max(latency, MIN_LATENCY_MS);
        ++measured;
      }
    }
    // No group measured yet: any positive constant works since every group then gets the same neutral strength.
    return measured > 0 ? sum / measured : 1.0;
  }

  private double totalStrength(
      IntToDoubleFunction latencyView,
      int groupCount,
      double neutralStrength,
      int excludedGroupId) {
    double total = 0.0;
    for (int g = 0; g < groupCount; ++g) {
      if (g == excludedGroupId) {
        continue;
      }
      total += strength(latencyView, g, neutralStrength);
    }
    return total;
  }
}
