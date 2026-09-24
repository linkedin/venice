package com.linkedin.venice.router.api.routing.helix;

import com.linkedin.alpini.base.concurrency.TimeoutProcessor;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.stats.routing.HelixGroupStats;
import com.linkedin.venice.stats.routing.LatencyAdaptiveGroupSelector;
import com.linkedin.venice.utils.Pair;
import java.util.HashMap;
import java.util.Map;
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

  /** @see LatencyAdaptiveGroupSelector#DEFAULT_EVEN_UNTIL_LATENCY_RATIO */
  public static final double DEFAULT_EVEN_UNTIL_LATENCY_RATIO =
      LatencyAdaptiveGroupSelector.DEFAULT_EVEN_UNTIL_LATENCY_RATIO;

  /** @see LatencyAdaptiveGroupSelector#DEFAULT_FULL_SKEW_AT_LATENCY_RATIO */
  public static final double DEFAULT_FULL_SKEW_AT_LATENCY_RATIO =
      LatencyAdaptiveGroupSelector.DEFAULT_FULL_SKEW_AT_LATENCY_RATIO;

  /** @see LatencyAdaptiveGroupSelector#DEFAULT_INTERPOLATION_EXPONENT */
  public static final double DEFAULT_INTERPOLATION_EXPONENT =
      LatencyAdaptiveGroupSelector.DEFAULT_INTERPOLATION_EXPONENT;

  /** @see LatencyAdaptiveGroupSelector#MIN_LATENCY_MS */
  public static final double MIN_LATENCY_MS = LatencyAdaptiveGroupSelector.MIN_LATENCY_MS;

  /** @see LatencyAdaptiveGroupSelector#PROBE_FLOOR_FRACTION */
  public static final double PROBE_FLOOR_FRACTION = LatencyAdaptiveGroupSelector.PROBE_FLOOR_FRACTION;

  private final int[] counters = new int[MAX_ALLOWED_GROUP];
  private final TimeoutProcessor timeoutProcessor;
  private final long timeoutInMS;
  private final Map<Long, Pair<Integer, TimeoutProcessor.TimeoutFuture>> requestTimeoutFutureMap = new HashMap<>();
  private final HelixGroupStats helixGroupStats;
  private final LatencyAdaptiveGroupSelector selector;

  public HelixGroupLatencyAdaptiveStrategy(
      TimeoutProcessor timeoutProcessor,
      long timeoutInMS,
      HelixGroupStats helixGroupStats) {
    this(
        timeoutProcessor,
        timeoutInMS,
        helixGroupStats,
        helixGroupStats::getGroupResponseWaitingTimeAvg,
        DEFAULT_EVEN_UNTIL_LATENCY_RATIO,
        DEFAULT_FULL_SKEW_AT_LATENCY_RATIO,
        DEFAULT_INTERPOLATION_EXPONENT,
        () -> ThreadLocalRandom.current().nextDouble());
  }

  /**
   * Knobs and latency source are forwarded to {@link LatencyAdaptiveGroupSelector}; see it for their meaning and the
   * validated invariants ({@code 1 <= evenUntilLatencyRatio < fullSkewAtLatencyRatio}, {@code interpolationExponent > 0}).
   */
  public HelixGroupLatencyAdaptiveStrategy(
      TimeoutProcessor timeoutProcessor,
      long timeoutInMS,
      HelixGroupStats helixGroupStats,
      IntToDoubleFunction latencyProvider,
      double evenUntilLatencyRatio,
      double fullSkewAtLatencyRatio,
      double interpolationExponent,
      DoubleSupplier randomSupplier) {
    this.timeoutProcessor = timeoutProcessor;
    this.timeoutInMS = timeoutInMS;
    this.helixGroupStats = helixGroupStats;
    this.selector = new LatencyAdaptiveGroupSelector(
        latencyProvider,
        evenUntilLatencyRatio,
        fullSkewAtLatencyRatio,
        interpolationExponent,
        randomSupplier);
  }

  @Override
  public int selectGroup(long requestId, int groupCount) {
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
      selectedGroup = pickWeightedGroup(groupCount, startGroupId);
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
                  () -> timeoutRequest(requestId, finalSelectedGroup, false),
                  timeoutInMS,
                  TimeUnit.MILLISECONDS)));
      ++counters[selectedGroup];
    }
    helixGroupStats.recordGroupPendingRequest(selectedGroup, counters[selectedGroup]);
    return selectedGroup;
  }

  /** Delegates to the shared {@link LatencyAdaptiveGroupSelector}; the router excludes no group. */
  private int pickWeightedGroup(int groupCount, int startGroupId) {
    return selector.selectGroup(groupCount, startGroupId);
  }

  private void timeoutRequest(long requestId, int groupId, boolean cancelTimeoutFuture) {
    if (groupId >= MAX_ALLOWED_GROUP || groupId < 0) {
      throw new VeniceException(
          "The allowed group id must fail into this range: [0, " + (MAX_ALLOWED_GROUP - 1) + "], but received: "
              + groupId);
    }
    if (!cancelTimeoutFuture) {
      // Timeout request
      helixGroupStats.recordGroupResponseWaitingTime(groupId, timeoutInMS);
    }
    synchronized (this) {
      Pair<Integer, TimeoutProcessor.TimeoutFuture> timeoutFuturePair = requestTimeoutFutureMap.get(requestId);
      if (timeoutFuturePair == null) {
        /**
         * Request has already timed out or already finished.
         */
        return;
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
  }

  @Override
  public void finishRequest(long requestId, int groupId, double latency) {
    timeoutRequest(requestId, groupId, true);
    helixGroupStats.recordGroupResponseWaitingTime(groupId, latency);
  }
}
