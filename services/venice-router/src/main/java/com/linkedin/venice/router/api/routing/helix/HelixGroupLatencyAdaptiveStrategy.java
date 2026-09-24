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
 * A latency-aware Helix group selection strategy that spreads load across groups by a continuous weight
 * instead of the deterministic winner-take-all tie-break used by {@link HelixGroupLeastLoadedStrategy}.
 *
 * <p>The legacy least-loaded strategy is lexicographic: it first picks the group(s) with the fewest in-flight
 * requests, then, among ties, deterministically picks the single lowest-latency group. At low per-router
 * in-flight the counters are almost always tied at (or near) zero, so the latency tie-break fires on nearly
 * every request and funnels a disproportionate share of traffic to whichever group is momentarily fastest,
 * even when its latency edge is sub-millisecond. That over-concentration drives one group's nodes toward
 * their read-quota ceiling (429s) while other groups sit idle with headroom.
 *
 * <p>This strategy keeps latency as the signal for <em>which</em> group is fast, but changes <em>how</em> that
 * signal is used. Instead of a winner-take-all tie-break it computes a per-group target <em>share</em> and does
 * a weighted-random draw. The amount of skew is driven by how far apart the groups' latencies are, so the
 * strategy behaves as a best-effort latency equaliser: while every group is fast it routes evenly, and as one
 * group's latency pulls ahead of the others it sheds traffic off that group and onto the faster ones, using
 * request share as the lever to pull the groups back toward a common latency.
 *
 * <p>Think of each group's latency as a function of the request rate it receives: flat while the group has
 * ample headroom, bending to linear, then climbing steeply as it approaches saturation. Different groups bend
 * at different rates (a weaker group's curve rises sooner). The strategy reads only the latency each group is
 * currently producing — its position on that curve — and steers share away from the groups that have climbed
 * highest, which lowers their latency and raises the fast groups' latency until the curves meet.
 *
 * <pre>
 *   strength(g) = 1 / max(latency(g), MIN_LATENCY_MS)                     // fast (low-latency) group => stronger
 *   ratio       = max(latency) / min(latency)   over measured groups      // 1.0 == perfectly even latency
 *   skew        = 0                                          if ratio &lt;= evenUntilLatencyRatio  // stay-even knob
 *                 1                                          if ratio &gt;= fullSkewAtLatencyRatio  // full-skew knob
 *                 ((ratio - evenUntil) / (fullSkew - evenUntil))^m  otherwise                     // ramp between
 *   share(g)    = (1 - skew) * (1 / G)  +  skew * strength(g) / sum(strength)
 *   share(g)    = max(share(g), PROBE_FLOOR_FRACTION * (1 / G))           // never fully starve a group
 * </pre>
 *
 * <ul>
 *   <li><b>latency(g)</b> — the group's measured average response time
 *       ({@link HelixGroupStats#getGroupResponseWaitingTimeAvg}). This is the <em>only</em> signal the strategy
 *       needs — both <em>which</em> group is fast (lower latency => higher strength) and <em>how much</em> to
 *       skew (the spread across groups). There is no configured per-group capacity, read-quota allocation, or
 *       aggregate utilisation input: the strength of a group is inferred purely from what it currently delivers.
 *       A group that has not served anything yet (latency {@code <= 0}) is treated as neutral (average strength)
 *       and is excluded from the spread, so it is neither flooded nor starved before it has been measured.</li>
 *   <li><b>ratio (latency spread)</b> — the ratio of the slowest to the fastest measured group. It is
 *       {@code 1.0} when the groups are perfectly balanced and grows as they diverge. Because it is a
 *       <em>relative</em> measure it needs no absolute latency target and travels across stores with very
 *       different baselines; and when every group is uniformly slow (all balanced, ratio near {@code 1}) it
 *       correctly reports "nothing to correct" and leaves routing even, since skewing could not lower the
 *       common latency.</li>
 *   <li><b>1 / G (even split)</b> — the balanced target. Spreading evenly while the groups are close keeps every
 *       group's read-quota consumption low, avoids the over-concentration that pushes a single group to its 429
 *       ceiling, and keeps every group probed so its latency measurement stays fresh.</li>
 *   <li><b>evenUntilLatencyRatio (stay-even knob)</b> — the latency spread up to which routing stays fully even.
 *       While the slowest group is within this factor of the fastest ({@code ratio <= evenUntilLatencyRatio})
 *       the spread is treated as noise and {@code skew == 0}. Lowering it makes the strategy react to smaller
 *       imbalances; raising it tolerates a wider spread before it starts steering.</li>
 *   <li><b>fullSkewAtLatencyRatio (full-skew knob)</b> — the latency spread at (and above) which routing reaches
 *       its full latency-proportional split ({@code skew == 1}). Between the two knobs the skew ramps from 0 to
 *       1 as the spread widens.</li>
 *   <li><b>m (in-band ramp exponent)</b> — shapes the ramp <em>between</em> the two knobs. {@code m == 1} is a
 *       straight linear ramp; {@code m > 1} keeps the ramp gentle just past {@code evenUntilLatencyRatio} and
 *       steepens it near {@code fullSkewAtLatencyRatio}. It only affects the transition band.</li>
 *   <li><b>PROBE_FLOOR_FRACTION</b> — a floor that guarantees every group keeps a small share even at full skew,
 *       so the slower groups are never fully starved and the router keeps observing their latency. This is what
 *       closes the loop: routing more traffic to a fast group raises its latency and less to a slow group lowers
 *       its latency, so the groups converge toward equal latency rather than one being driven past its
 *       ceiling.</li>
 * </ul>
 *
 * <p>The equalisation is <em>best effort</em>: a pure {@code 1/latency} weighting relieves an imbalance rather
 * than perfectly erasing it, so with a large capacity gap the slower groups settle above the fastest but well
 * below where even routing would have left them. Trading a small, within-SLO increase in average / p99 latency
 * for the avoidance of read-quota breaches (429s) is the explicit design goal.
 *
 * <p>The counter-leak protection via {@link TimeoutProcessor} and the synchronized in-flight accounting are
 * preserved from {@link HelixGroupLeastLoadedStrategy}; the in-flight counters are kept for leak protection and
 * observability ({@link HelixGroupStats#recordGroupPendingRequest}) and do not influence the target share.
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
   * @param latencyProvider        maps a group id to its measured average response time in milliseconds; a
   *                              non-positive value means the group has not been measured yet and is treated as
   *                              neutral (average strength) and excluded from the spread.
   * @param evenUntilLatencyRatio  stay-even threshold as a latency spread ratio (slowest / fastest): routing
   *                              stays fully even while the spread is at or below this factor. Must be
   *                              {@code >= 1} and strictly less than {@code fullSkewAtLatencyRatio}.
   * @param fullSkewAtLatencyRatio full-skew threshold as a latency spread ratio: routing reaches its full
   *                              latency-proportional split at or above this factor. Must be strictly greater
   *                              than {@code evenUntilLatencyRatio}.
   * @param interpolationExponent  the {@code m} exponent shaping the skew ramp between the two thresholds
   *                              ({@code 1.0} = linear). Must be strictly greater than {@code 0}.
   * @param randomSupplier         supplies a uniform random double in [0, 1); injectable so tests can be
   *                              deterministic.
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

  /**
   * Delegates to the shared {@link LatencyAdaptiveGroupSelector}, which performs a weighted-random reservoir draw
   * whose skew adapts to the current latency spread across groups. The router applies no group exclusion, so it never
   * excludes a group ({@code excludedGroupId == -1}).
   */
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
