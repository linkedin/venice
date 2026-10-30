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
 * latency: while every group is fast it routes evenly, and as the slowest group's latency climbs it steers share onto
 * the faster groups. The two thresholds are absolute latencies in milliseconds so they can be set against a
 * response-time SLA. Measured latency is the only signal - it determines both which group is fast and how much to skew:
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
 * <p>Because share still weighs by {@code strength(g)}, groups that are all slow but equal in latency get equal
 * strength and routing stays even even at full skew - the skew only steers when a genuinely faster group exists. A
 * group not yet measured ({@code latency <= 0}) is treated as neutral (average strength) and excluded from the skew
 * calculation. The probe floor keeps every group observable so the latency signal stays live and the loop
 * self-corrects.
 *
 * <h2>Key-count buckets</h2>
 *
 * <p>Whole-request latency scales with the number of keys in a multi-key request, so a single latency average that
 * mixes request sizes would conflate a slow host with a merely large request. To compare like-for-like, latency is
 * tracked per <em>key-count bucket</em>: each request is mapped to a bucket by its key count, and both the signal it
 * reads and the sample it records are scoped to that bucket. The bucket boundaries and their per-bucket full-skew
 * thresholds are supplied pre-parsed (see {@link #LatencyAdaptiveGroupSelector(GroupBucketLatencyProvider,
 * GroupBucketSampleCountProvider, NavigableMap, double, double, long, DoubleSupplier)}); the same key-count-to-bucket
 * mapping is exposed via {@link #bucketKeyForKeyCount(int)} so callers record into the identical bucket the selector
 * reads from. The per-bucket stay-even threshold is derived from the full-skew threshold by a fixed fraction so one
 * configured value fully defines a bucket's dead-band.
 *
 * <p>A bucket with fewer than {@code minSampleCount} samples for a group is treated as not-yet-measured for that group,
 * so a thin or rarely-exercised bucket falls back to even routing rather than reacting to a noisy average.
 *
 * <p>Holds no per-request state; thread-safe as long as the supplied providers and {@code randomSupplier} are. Callers
 * own any in-flight accounting. The only mutable state is a throttle timestamp for the optional periodic weight log,
 * which is updated with a lock-free compare-and-set and does not affect selection.
 */
public class LatencyAdaptiveGroupSelector {
  /**
   * Maps a group and a key-count bucket (identified by the bucket's inclusive lower key-count bound) to that group's
   * measured average response time in ms; non-positive means not yet measured.
   */
  @FunctionalInterface
  public interface GroupBucketLatencyProvider {
    double latencyMs(int groupId, int bucketKey);
  }

  /**
   * Maps a group and a key-count bucket to the number of latency samples recorded for it, used to apply the
   * minimum-sample floor below which a bucket is treated as not yet measured for that group.
   */
  @FunctionalInterface
  public interface GroupBucketSampleCountProvider {
    long sampleCount(int groupId, int bucketKey);
  }

  public static final int MAX_ALLOWED_GROUP = 100;

  /**
   * Default interval, in milliseconds, between periodic per-group weight log lines (at INFO). Set it to {@code <= 0}
   * via {@link #setWeightLogIntervalMs(long)} to disable the periodic log.
   */
  public static final long DEFAULT_WEIGHT_LOG_INTERVAL_MS = 2000L;

  private static final Logger LOGGER = LogManager.getLogger(LatencyAdaptiveGroupSelector.class);

  /**
   * Default stay-even threshold, as an absolute latency in milliseconds: routing stays fully even (skew {@code == 0})
   * while the slowest measured group's average latency is at or below this. Set it against the read-path SLA. Used by
   * the single-bucket (non size-aware) constructors; bucketed construction derives each bucket's stay-even threshold
   * from its full-skew threshold via {@link #DEFAULT_EVEN_UNTIL_FRACTION}.
   */
  public static final double DEFAULT_EVEN_UNTIL_LATENCY_MS = 10.0;

  /**
   * Default full-skew threshold, as an absolute latency in milliseconds: routing reaches its full latency-proportional
   * split (skew {@code == 1}) once the slowest measured group's average latency is at or above this. Between
   * {@link #DEFAULT_EVEN_UNTIL_LATENCY_MS} and this the skew ramps 0 to 1.
   */
  public static final double DEFAULT_FULL_SKEW_AT_LATENCY_MS = 30.0;

  /**
   * Default fraction of a bucket's full-skew threshold used as that bucket's stay-even threshold, so one configured
   * full-skew value fully defines the bucket's dead-band. Matches the {@code 10 / 30} ratio of the single-bucket
   * defaults.
   */
  public static final double DEFAULT_EVEN_UNTIL_FRACTION = 1.0 / 3.0;

  /**
   * Default minimum number of samples a group must have in a bucket before its average latency is acted on; below it the
   * bucket is treated as not yet measured for that group and routing stays even there.
   */
  public static final long DEFAULT_MIN_SAMPLE_COUNT = 10L;

  /**
   * Default in-band ramp exponent {@code m}, shaping the skew ramp between the two thresholds: {@code 1.0} is linear and
   * {@code > 1} steepens toward the full-skew threshold. Affects the transition band only.
   */
  public static final double DEFAULT_INTERPOLATION_EXPONENT = 1.0;

  /**
   * Lower bound on a group's latency before inverting it into a strength, so a near-zero latency cannot yield an
   * unbounded strength.
   */
  public static final double MIN_LATENCY_MS = 1.0;

  /**
   * Minimum share every group retains, as a fraction of the even share {@code 1 / G}, so a slow group is never fully
   * starved and its latency stays observable.
   */
  public static final double PROBE_FLOOR_FRACTION = 0.05;

  private final GroupBucketLatencyProvider latencyProvider;
  private final GroupBucketSampleCountProvider sampleCountProvider;
  /** Bucket lower bound (inclusive) -> array index into the per-bucket threshold arrays, for {@code floorEntry} lookup. */
  private final TreeMap<Integer, Integer> bucketIndexByLowerBound;
  private final int[] bucketLowerBounds;
  private final double[] bucketEvenUntilMs;
  private final double[] bucketFullSkewMs;
  private final long minSampleCount;
  private final double interpolationExponent;
  private final DoubleSupplier randomSupplier;
  private final AtomicLong lastWeightLogTimeNs = new AtomicLong(System.nanoTime());
  private volatile long weightLogIntervalMs = DEFAULT_WEIGHT_LOG_INTERVAL_MS;
  private volatile String logContext = "";

  /**
   * Single-bucket (non size-aware) selector with the default knobs and {@link ThreadLocalRandom} for the weighted draw.
   * All key counts map to one bucket, reproducing the pre-bucketing behaviour.
   *
   * @param latencyProvider maps a group id to its measured average response time in ms; non-positive means not yet
   *                        measured.
   */
  public LatencyAdaptiveGroupSelector(IntToDoubleFunction latencyProvider) {
    this(
        latencyProvider,
        DEFAULT_EVEN_UNTIL_LATENCY_MS,
        DEFAULT_FULL_SKEW_AT_LATENCY_MS,
        DEFAULT_INTERPOLATION_EXPONENT,
        () -> ThreadLocalRandom.current().nextDouble());
  }

  /**
   * Single-bucket (non size-aware) selector. All key counts map to one bucket whose dead-band is {@code
   * [evenUntilLatencyMs, fullSkewAtLatencyMs]}, and no minimum-sample floor is applied.
   *
   * @param latencyProvider        group id -> measured average response time in ms; non-positive means not yet measured
   *                              (treated as neutral and excluded from the skew calculation).
   * @param evenUntilLatencyMs     stay-even threshold in ms; while the slowest measured group is at or below it routing
   *                              stays even. Must be {@code >= 0} and {@code < fullSkewAtLatencyMs}.
   * @param fullSkewAtLatencyMs    full-skew threshold in ms; routing reaches its full latency-proportional split once
   *                              the slowest measured group is at or above it. Must be {@code > evenUntilLatencyMs}.
   * @param interpolationExponent  ramp exponent {@code m} between the thresholds ({@code 1.0} = linear). Must be
   *                              {@code > 0}.
   * @param randomSupplier         uniform random double in [0, 1); injectable for deterministic tests.
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
    this.minSampleCount = 0L;
    this.interpolationExponent = interpolationExponent;
    this.randomSupplier = randomSupplier;
  }

  /**
   * Size-aware (bucketed) selector. Latency is tracked and thresholds applied per key-count bucket.
   *
   * @param latencyProvider       {@code (groupId, bucketKey)} -> measured average response time in ms; non-positive
   *                              means not yet measured.
   * @param sampleCountProvider   {@code (groupId, bucketKey)} -> number of samples; a group with fewer than
   *                              {@code minSampleCount} in a bucket is treated as not yet measured there.
   * @param fullSkewMsByLowerBound inclusive bucket lower key-count bound -> that bucket's full-skew threshold in ms.
   *                              Must be non-empty, start at key count {@code 1}, and have positive thresholds (as
   *                              produced by the shared retry-range parser).
   * @param evenUntilFraction     fraction of each bucket's full-skew threshold used as its stay-even threshold; must be
   *                              in {@code (0, 1)}.
   * @param interpolationExponent ramp exponent {@code m} between the thresholds ({@code 1.0} = linear). Must be
   *                              {@code > 0}.
   * @param minSampleCount        minimum per-(group, bucket) samples before the average is acted on; must be
   *                              {@code >= 0}.
   * @param randomSupplier        uniform random double in [0, 1); injectable for deterministic tests.
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
    this.minSampleCount = minSampleCount;
    this.interpolationExponent = interpolationExponent;
    this.randomSupplier = randomSupplier;
  }

  /**
   * The inclusive lower key-count bound identifying the bucket a request of {@code keyCount} keys falls into. Callers
   * record a request's latency sample under this same key so the selector reads back a size-matched signal. Key counts
   * below the lowest bucket clamp to the first bucket.
   */
  public int bucketKeyForKeyCount(int keyCount) {
    Map.Entry<Integer, Integer> entry = bucketIndexByLowerBound.floorEntry(keyCount);
    return entry == null ? bucketLowerBounds[0] : entry.getKey();
  }

  /**
   * Single-bucket weighted-random selection across all groups. Equivalent to {@link #selectGroup(int, int, int, int)}
   * with no excluded group; retained for the non size-aware path.
   */
  public int selectGroup(int groupCount, int startGroupId) {
    return selectGroup(groupCount, startGroupId, -1, bucketLowerBounds[0]);
  }

  /**
   * Single-bucket weighted-random selection, optionally excluding one group. Retained for the non size-aware path.
   */
  public int selectGroup(int groupCount, int startGroupId, int excludedGroupId) {
    return selectGroup(groupCount, startGroupId, excludedGroupId, bucketLowerBounds[0]);
  }

  /**
   * Weighted-random reservoir selection across the groups for a request of {@code keyCount} keys, optionally excluding
   * one group. The request's key count selects the bucket whose latency signal and thresholds drive the draw; each
   * candidate is adopted with probability {@code share(g) / cumulativeShare}, so the final selection probability is
   * proportional to {@code share(g)} in a single pass. The scan starts at {@code startGroupId} only to avoid biasing
   * toward group 0.
   *
   * @param groupCount      number of groups; must be in {@code [1, MAX_ALLOWED_GROUP]}.
   * @param startGroupId    group id the scan starts from (typically {@code requestId % groupCount}).
   * @param excludedGroupId group to exclude from selection and from the skew calculation (e.g. the group already tried,
   *                        so a retry lands elsewhere); {@code -1} excludes none.
   * @param keyCount        number of keys in the request, used to pick the key-count bucket.
   */
  public int selectGroup(int groupCount, int startGroupId, int excludedGroupId, int keyCount) {
    if (groupCount > MAX_ALLOWED_GROUP || groupCount <= 0) {
      throw new VeniceException(
          "The valid group num must fall into this range: [1, " + MAX_ALLOWED_GROUP + "], but received: " + groupCount);
    }
    int bucketIndex = bucketIndexForKeyCount(keyCount);
    IntToDoubleFunction latencyView = latencyView(bucketIndex);
    double evenUntil = bucketEvenUntilMs[bucketIndex];
    double fullSkew = bucketFullSkewMs[bucketIndex];
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
    maybeLogWeights(groupCount, bucketIndex, latencyView);
    if (selectedGroup >= 0) {
      return selectedGroup;
    }
    // Every candidate collapsed to zero (should not happen given the probe floor) or the only group was excluded: fall
    // back to a group that still gets the request routed somewhere rather than dropped.
    return startGroupId == excludedGroupId ? (startGroupId + 1) % groupCount : startGroupId;
  }

  /**
   * Sets how often, in milliseconds, the per-group weights are logged at INFO. A value {@code <= 0} disables the
   * periodic log. Defaults to {@link #DEFAULT_WEIGHT_LOG_INTERVAL_MS}.
   */
  public void setWeightLogIntervalMs(long weightLogIntervalMs) {
    this.weightLogIntervalMs = weightLogIntervalMs;
  }

  /**
   * Sets a short identifier (e.g. {@code "router"} or {@code "fast-client:<store>"}) included in the periodic weight log
   * line so lines from different selectors are attributable.
   */
  public void setLogContext(String logContext) {
    this.logContext = logContext == null ? "" : logContext;
  }

  /**
   * The array index of the bucket a request of {@code keyCount} keys falls into; clamps to the first bucket for key
   * counts below the lowest bound.
   */
  private int bucketIndexForKeyCount(int keyCount) {
    Map.Entry<Integer, Integer> entry = bucketIndexByLowerBound.floorEntry(keyCount);
    return entry == null ? 0 : entry.getValue();
  }

  /**
   * A per-group latency view for one bucket that applies the minimum-sample floor: a group with fewer than
   * {@code minSampleCount} samples in the bucket reads as not yet measured ({@code -1}).
   */
  private IntToDoubleFunction latencyView(int bucketIndex) {
    int bucketKey = bucketLowerBounds[bucketIndex];
    return groupId -> {
      if (minSampleCount > 0L && sampleCountProvider.sampleCount(groupId, bucketKey) < minSampleCount) {
        return -1.0;
      }
      return latencyProvider.latencyMs(groupId, bucketKey);
    };
  }

  /**
   * Renders the routing picture for the bucket a request of {@code keyCount} keys falls into - its key-count range,
   * skew, thresholds, and each group's measured latency and realized selection weight - as one log-friendly line. Pure:
   * reads the latency view but mutates nothing.
   */
  public String formatWeights(int groupCount, int keyCount) {
    int bucketIndex = bucketIndexForKeyCount(keyCount);
    return renderWeights(groupCount, bucketIndex, latencyView(bucketIndex));
  }

  /**
   * Renders the current routing picture for the given bucket - its key-count range, skew, thresholds, and each group's
   * measured latency and realized selection weight - as one log-friendly line. Recomputes the same shares
   * {@link #selectGroup} would use (no group excluded) and normalizes them to the actual draw probabilities, so the
   * reported weights sum to ~100%. Pure: reads the latency view but mutates nothing.
   */
  private String renderWeights(int groupCount, int bucketIndex, IntToDoubleFunction latencyView) {
    double evenUntil = bucketEvenUntilMs[bucketIndex];
    double fullSkew = bucketFullSkewMs[bucketIndex];
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
   * Logs the current per-group weights for the active bucket at INFO at most once per {@link #weightLogIntervalMs}. The
   * time gate is a lock-free compare-and-set so only one thread emits a line per window and the hot selection path stays
   * contention free.
   */
  private void maybeLogWeights(int groupCount, int bucketIndex, IntToDoubleFunction latencyView) {
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
    LOGGER.info(renderWeights(groupCount, bucketIndex, latencyView));
  }

  /**
   * The skew factor in {@code [0, 1]} derived from the slowest measured, non-excluded group's latency (ms). Groups not
   * yet measured are ignored here; with no measured group the skew is 0 (even routing).
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
   * {@code max( (1 - skew) * evenShare + skew * strength(g) / totalStrength, floor )}. The floor keeps a slow group
   * from being fully starved so its latency stays observable.
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

  /**
   * A group's inferred strength: the reciprocal of its measured latency (faster => stronger). A group that has not been
   * measured yet (non-positive latency) is treated as neutral so it is neither flooded nor starved before there is
   * data.
   */
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
    // No group measured yet: any positive constant works since every group then gets the same neutral strength, which
    // reduces the strength term to an even split.
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
