package com.linkedin.venice.router.api.routing.helix;

import com.linkedin.alpini.base.concurrency.TimeoutProcessor;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.stats.routing.HelixGroupStats;
import com.linkedin.venice.stats.routing.LatencyAdaptiveGroupSelector;
import com.linkedin.venice.utils.Pair;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.DoubleSupplier;
import java.util.function.IntToDoubleFunction;


/**
 * Latency-adaptive Helix group selection strategy: spreads load across groups by a continuous, latency-driven weight
 * instead of the winner-take-all tie-break used by {@link HelixGroupLeastLoadedStrategy}. The share/skew math is shared
 * with the fast client in {@link LatencyAdaptiveGroupSelector}; this class only adds the router's
 * {@link HelixGroupSelectionStrategy} plumbing on top.
 *
 * <p>{@link HelixGroupLeastLoadedStrategy} first picks the group(s) with the fewest in-flight requests, then breaks
 * ties by lowest latency. At low per-router in-flight the counters are almost always tied at ~0, so the tie-break fires
 * on nearly every request and over-concentrates traffic on whichever group is momentarily fastest. This strategy keeps
 * latency as the signal for which group is fast but turns it into a per-group target share and a weighted-random draw,
 * routing evenly while the groups are close and shedding traffic off a group only as its latency pulls ahead. See
 * {@link LatencyAdaptiveGroupSelector} for the formula and knobs.
 *
 * <p>The counter-leak protection via {@link TimeoutProcessor} and the synchronized in-flight accounting are preserved
 * from {@link HelixGroupLeastLoadedStrategy}; the counters are kept for leak protection and observability
 * ({@link HelixGroupStats#recordGroupPendingRequest}) and do not influence the target share.
 */
public class HelixGroupLatencyAdaptiveStrategy implements HelixGroupSelectionStrategy {
  public static final int MAX_ALLOWED_GROUP = LatencyAdaptiveGroupSelector.MAX_ALLOWED_GROUP;

  /** @see LatencyAdaptiveGroupSelector#DEFAULT_EVEN_UNTIL_LATENCY_MS */
  public static final double DEFAULT_EVEN_UNTIL_LATENCY_MS = LatencyAdaptiveGroupSelector.DEFAULT_EVEN_UNTIL_LATENCY_MS;

  /** @see LatencyAdaptiveGroupSelector#DEFAULT_FULL_SKEW_AT_LATENCY_MS */
  public static final double DEFAULT_FULL_SKEW_AT_LATENCY_MS =
      LatencyAdaptiveGroupSelector.DEFAULT_FULL_SKEW_AT_LATENCY_MS;

  /** @see LatencyAdaptiveGroupSelector#DEFAULT_INTERPOLATION_EXPONENT */
  public static final double DEFAULT_INTERPOLATION_EXPONENT =
      LatencyAdaptiveGroupSelector.DEFAULT_INTERPOLATION_EXPONENT;

  /** @see LatencyAdaptiveGroupSelector#DEFAULT_EVEN_UNTIL_FRACTION */
  public static final double DEFAULT_EVEN_UNTIL_FRACTION = LatencyAdaptiveGroupSelector.DEFAULT_EVEN_UNTIL_FRACTION;

  /** @see LatencyAdaptiveGroupSelector#DEFAULT_MIN_SAMPLE_COUNT */
  public static final long DEFAULT_MIN_SAMPLE_COUNT = LatencyAdaptiveGroupSelector.DEFAULT_MIN_SAMPLE_COUNT;

  /** @see LatencyAdaptiveGroupSelector#MIN_LATENCY_MS */
  public static final double MIN_LATENCY_MS = LatencyAdaptiveGroupSelector.MIN_LATENCY_MS;

  /** @see LatencyAdaptiveGroupSelector#PROBE_FLOOR_FRACTION */
  public static final double PROBE_FLOOR_FRACTION = LatencyAdaptiveGroupSelector.PROBE_FLOOR_FRACTION;

  private final int[] counters = new int[MAX_ALLOWED_GROUP];
  /** Key count used for the size-agnostic entry points; maps to the first (and only) bucket in non size-aware mode. */
  private static final int SINGLE_KEY_COUNT = 1;
  /** Sentinel passed to the selector meaning no group is excluded from selection. */
  private static final int NO_EXCLUDED_GROUP = -1;
  private final TimeoutProcessor timeoutProcessor;
  private final long timeoutInMS;
  private final Map<Long, Pair<Integer, TimeoutProcessor.TimeoutFuture>> requestTimeoutFutureMap = new HashMap<>();
  private final HelixGroupStats helixGroupStats;
  private final LatencyAdaptiveGroupSelector selector;
  /**
   * When {@code true} the strategy tracks and skews latency per key-count bucket, reading and recording the dedicated
   * per-(group, bucket) routing metric; when {@code false} it uses the single absolute-ms band and the shared
   * response-waiting-time metric (also consumed by least-loaded routing).
   */
  private final boolean sizeAware;

  public HelixGroupLatencyAdaptiveStrategy(
      TimeoutProcessor timeoutProcessor,
      long timeoutInMS,
      HelixGroupStats helixGroupStats) {
    this(
        timeoutProcessor,
        timeoutInMS,
        helixGroupStats,
        helixGroupStats::getGroupResponseWaitingTimeAvg,
        DEFAULT_EVEN_UNTIL_LATENCY_MS,
        DEFAULT_FULL_SKEW_AT_LATENCY_MS,
        DEFAULT_INTERPOLATION_EXPONENT,
        () -> ThreadLocalRandom.current().nextDouble());
  }

  /**
   * Knobs and latency source are forwarded to {@link LatencyAdaptiveGroupSelector}; see it for their meaning and the
   * validated invariants ({@code 0 <= evenUntilLatencyMs < fullSkewAtLatencyMs}, {@code interpolationExponent > 0}).
   */
  public HelixGroupLatencyAdaptiveStrategy(
      TimeoutProcessor timeoutProcessor,
      long timeoutInMS,
      HelixGroupStats helixGroupStats,
      IntToDoubleFunction latencyProvider,
      double evenUntilLatencyMs,
      double fullSkewAtLatencyMs,
      double interpolationExponent,
      DoubleSupplier randomSupplier) {
    this.timeoutProcessor = timeoutProcessor;
    this.timeoutInMS = timeoutInMS;
    this.helixGroupStats = helixGroupStats;
    this.sizeAware = false;
    this.selector = new LatencyAdaptiveGroupSelector(
        latencyProvider,
        evenUntilLatencyMs,
        fullSkewAtLatencyMs,
        interpolationExponent,
        randomSupplier);
    this.selector.setLogContext("router");
  }

  /**
   * Size-aware (bucketed) construction: latency is tracked and skewed per key-count bucket, reading and recording the
   * dedicated per-(group, bucket) routing metric so a group that is fast for small batches but slow for large ones is
   * routed accordingly. Thresholds and knobs are forwarded to the bucketed
   * {@link LatencyAdaptiveGroupSelector#LatencyAdaptiveGroupSelector(LatencyAdaptiveGroupSelector.GroupBucketLatencyProvider,
   * LatencyAdaptiveGroupSelector.GroupBucketSampleCountProvider, java.util.NavigableMap, double, double, long,
   * DoubleSupplier)} constructor; see it for the validated invariants.
   */
  public HelixGroupLatencyAdaptiveStrategy(
      TimeoutProcessor timeoutProcessor,
      long timeoutInMS,
      HelixGroupStats helixGroupStats,
      NavigableMap<Integer, Integer> fullSkewMsByLowerBound,
      double evenUntilFraction,
      double interpolationExponent,
      long minSampleCount,
      DoubleSupplier randomSupplier) {
    this.timeoutProcessor = timeoutProcessor;
    this.timeoutInMS = timeoutInMS;
    this.helixGroupStats = helixGroupStats;
    this.sizeAware = true;
    this.selector = new LatencyAdaptiveGroupSelector(
        helixGroupStats::getRoutingLatencyAvg,
        helixGroupStats::getRoutingLatencySampleCount,
        fullSkewMsByLowerBound,
        evenUntilFraction,
        interpolationExponent,
        minSampleCount,
        randomSupplier);
    this.selector.setLogContext("router");
  }

  @Override
  public int selectGroup(long requestId, int groupCount) {
    return selectGroup(requestId, groupCount, SINGLE_KEY_COUNT);
  }

  @Override
  public int selectGroup(long requestId, int groupCount, int keyCount) {
    if (groupCount > MAX_ALLOWED_GROUP || groupCount <= 0) {
      throw new VeniceException(
          "The valid group num must fail into this range: [1, " + MAX_ALLOWED_GROUP + "], but received: " + groupCount);
    }
    int startGroupId = (int) (requestId % groupCount);
    int selectedGroup;
    synchronized (this) {
      if (requestTimeoutFutureMap.containsKey(requestId)) {
        throw new VeniceException(
            "One request should at most select one group, but request with request id: " + requestId
                + " has invoked this function more than once");
      }
      selectedGroup = pickWeightedGroup(groupCount, startGroupId, keyCount);
      final int finalSelectedGroup = selectedGroup;
      /**
       * Setting up timeout future for this request since it is possible in some situation, {@link #finishRequest}
       * may not be invoked, and without timeout, the group counter will be leaking.
       */
      requestTimeoutFutureMap.put(
          requestId,
          new Pair<>(
              selectedGroup,
              timeoutProcessor.schedule(
                  () -> timeoutRequest(requestId, finalSelectedGroup, keyCount),
                  timeoutInMS,
                  TimeUnit.MILLISECONDS)));
      ++counters[selectedGroup];
    }
    helixGroupStats.recordGroupPendingRequest(selectedGroup, counters[selectedGroup]);
    return selectedGroup;
  }

  /**
   * Delegates to the shared {@link LatencyAdaptiveGroupSelector}; the router excludes no group. In size-aware mode the
   * selector skews by the bucket {@code keyCount} falls into; otherwise {@code keyCount} is ignored.
   */
  private int pickWeightedGroup(int groupCount, int startGroupId, int keyCount) {
    return sizeAware
        ? selector.selectGroup(groupCount, startGroupId, NO_EXCLUDED_GROUP, keyCount)
        : selector.selectGroup(groupCount, startGroupId);
  }

  /**
   * Records a latency sample for a group into the right metric: in size-aware mode the dedicated per-(group, bucket)
   * routing metric the selector reads back, keyed by the bucket {@code keyCount} falls into; otherwise the shared
   * response-waiting-time metric (also consumed by least-loaded routing).
   */
  private void recordLatencySample(int groupId, int keyCount, double latency) {
    if (sizeAware) {
      helixGroupStats.recordRoutingLatency(groupId, selector.bucketKeyForKeyCount(keyCount), latency);
    } else {
      helixGroupStats.recordGroupResponseWaitingTime(groupId, latency);
    }
  }

  private void timeoutRequest(long requestId, int groupId, int keyCount) {
    /**
     * Internal leak safeguard: the request never finished in time. Penalize the group with the timeout latency so
     * traffic sheds away from a stalled group, but only if this call actually owns the request (dedup against a
     * concurrent {@link #finishRequest}, which would otherwise double-count the same request).
     */
    if (releaseRequest(requestId, groupId, false)) {
      recordLatencySample(groupId, keyCount, timeoutInMS);
    }
  }

  /**
   * Releases a request's bookkeeping (pending-request counter plus timeout future) exactly once. Returns {@code true}
   * if this call owned the request, so the caller may record a single latency sample; returns {@code false} if the
   * request was already released by the timeout or a prior finish, preventing the same request's latency from being
   * counted twice.
   */
  private boolean releaseRequest(long requestId, int groupId, boolean cancelTimeoutFuture) {
    if (groupId >= MAX_ALLOWED_GROUP || groupId < 0) {
      throw new VeniceException(
          "The allowed group id must fail into this range: [0, " + (MAX_ALLOWED_GROUP - 1) + "], but received: "
              + groupId);
    }
    synchronized (this) {
      Pair<Integer, TimeoutProcessor.TimeoutFuture> timeoutFuturePair = requestTimeoutFutureMap.get(requestId);
      if (timeoutFuturePair == null) {
        /**
         * Request has already timed out or already finished.
         */
        return false;
      }
      if (groupId != timeoutFuturePair.getFirst()) {
        throw new VeniceException(
            "Group id for request with id: " + requestId + " should be: " + timeoutFuturePair.getFirst()
                + ", but received: " + groupId);
      }
      if (--counters[groupId] < 0) {
        counters[groupId] = 0;
        throw new VeniceException(
            "The counter for group: " + groupId + " became negative, something wrong happened, will reset it to be 0.");
      }
      if (cancelTimeoutFuture) {
        // Cancel the timeout future
        timeoutFuturePair.getSecond().cancel();
      }
      requestTimeoutFutureMap.remove(requestId);
    }
    return true;
  }

  @Override
  public void finishRequest(long requestId, int groupId, double latency) {
    finishRequest(requestId, groupId, SINGLE_KEY_COUNT, latency, true);
  }

  @Override
  public void finishRequest(long requestId, int groupId, int keyCount, double latency) {
    finishRequest(requestId, groupId, keyCount, latency, true);
  }

  @Override
  public void finishRequest(long requestId, int groupId, int keyCount, double latency, boolean success) {
    /**
     * Always release the group bookkeeping so the pending-request counter cannot leak, but only feed the latency signal
     * for successful requests: a fast error (fail-fast) would otherwise make a group look artificially fast and attract
     * more traffic. Record at most once via {@link #releaseRequest} ownership.
     */
    if (releaseRequest(requestId, groupId, true) && success) {
      recordLatencySample(groupId, keyCount, latency);
    }
  }
}
