package com.linkedin.venice.router.api.routing.helix;

import com.linkedin.alpini.base.concurrency.TimeoutProcessor;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.stats.routing.HelixGroupStats;
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
public class HelixGroupWeightedLeastLoadedStrategy implements HelixGroupSelectionStrategy {
  public static final int MAX_ALLOWED_GROUP = 100;

  /**
   * Default stay-even threshold, expressed as a latency spread ratio (slowest / fastest measured group): while
   * the spread is at or below this factor the groups are considered balanced and routing stays fully even
   * (skew {@code == 0}). {@code 1.2} means "stay even until the slowest group is more than 20% slower than the
   * fastest".
   */
  public static final double DEFAULT_EVEN_UNTIL_LATENCY_RATIO = 1.2;

  /**
   * Default full-skew threshold, expressed as a latency spread ratio: at (and above) this factor the routing
   * reaches its full latency-proportional split (skew {@code == 1}). Between
   * {@link #DEFAULT_EVEN_UNTIL_LATENCY_RATIO} and this the skew ramps from 0 to 1 as the spread widens.
   * {@code 2.0} means "reach full skew once the slowest group is at least twice the fastest".
   */
  public static final double DEFAULT_FULL_SKEW_AT_LATENCY_RATIO = 2.0;

  /**
   * Default in-band ramp exponent {@code m}: shapes the skew ramp between the stay-even and full-skew thresholds.
   * {@code 1.0} is a linear ramp; values &gt; 1 keep the ramp gentle just past the stay-even threshold and
   * steepen it near the full-skew threshold. It only affects the transition band.
   */
  public static final double DEFAULT_INTERPOLATION_EXPONENT = 1.0;

  /**
   * Lower bound applied to a group's measured latency before inverting it into a strength, so a group reporting
   * a near-zero latency cannot be assigned an unbounded strength (and thus flood-routed).
   */
  public static final double MIN_LATENCY_MS = 1.0;

  /**
   * The minimum share every group retains, expressed as a fraction of the even share {@code 1 / G}. It keeps a
   * slow group from being fully starved at high utilization so the router keeps observing its latency and the
   * latency signal stays live and self-correcting.
   */
  public static final double PROBE_FLOOR_FRACTION = 0.05;

  private final int[] counters = new int[MAX_ALLOWED_GROUP];
  private final TimeoutProcessor timeoutProcessor;
  private final long timeoutInMS;
  private final Map<Long, Pair<Integer, TimeoutProcessor.TimeoutFuture>> requestTimeoutFutureMap = new HashMap<>();
  private final HelixGroupStats helixGroupStats;
  private final IntToDoubleFunction latencyProvider;
  private final double evenUntilLatencyRatio;
  private final double fullSkewAtLatencyRatio;
  private final double interpolationExponent;
  private final DoubleSupplier randomSupplier;

  public HelixGroupWeightedLeastLoadedStrategy(
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
   *                              ({@code 1.0} = linear).
   * @param randomSupplier         supplies a uniform random double in [0, 1); injectable so tests can be
   *                              deterministic.
   */
  public HelixGroupWeightedLeastLoadedStrategy(
      TimeoutProcessor timeoutProcessor,
      long timeoutInMS,
      HelixGroupStats helixGroupStats,
      IntToDoubleFunction latencyProvider,
      double evenUntilLatencyRatio,
      double fullSkewAtLatencyRatio,
      double interpolationExponent,
      DoubleSupplier randomSupplier) {
    if (!(evenUntilLatencyRatio >= 1.0 && evenUntilLatencyRatio < fullSkewAtLatencyRatio)) {
      throw new VeniceException(
          "Require 1 <= evenUntilLatencyRatio < fullSkewAtLatencyRatio, but received evenUntilLatencyRatio="
              + evenUntilLatencyRatio + ", fullSkewAtLatencyRatio=" + fullSkewAtLatencyRatio);
    }
    this.timeoutProcessor = timeoutProcessor;
    this.timeoutInMS = timeoutInMS;
    this.helixGroupStats = helixGroupStats;
    this.latencyProvider = latencyProvider;
    this.evenUntilLatencyRatio = evenUntilLatencyRatio;
    this.fullSkewAtLatencyRatio = fullSkewAtLatencyRatio;
    this.interpolationExponent = interpolationExponent;
    this.randomSupplier = randomSupplier;
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
   * Weighted-random reservoir selection across all groups. Each group is adopted with probability
   * {@code share(g) / cumulativeShare}, yielding a final selection probability proportional to {@code share(g)}
   * in a single pass. The scan starts at {@code startGroupId} purely to avoid biasing toward group 0; it does
   * not affect the resulting distribution.
   */
  private int pickWeightedGroup(int groupCount, int startGroupId) {
    double skew = latencySkew(groupCount);
    double evenShare = 1.0 / groupCount;
    double floor = PROBE_FLOOR_FRACTION * evenShare;
    // Pre-pass: total inferred strength and the neutral strength used for not-yet-measured groups.
    double neutralStrength = neutralStrength(groupCount);
    double totalStrength = totalStrength(groupCount, neutralStrength);

    double cumulativeShare = 0.0;
    int selectedGroup = -1;
    for (int i = 0; i < groupCount; ++i) {
      int currentGroup = (i + startGroupId) % groupCount;
      double share = shareForGroup(currentGroup, evenShare, floor, skew, neutralStrength, totalStrength);
      if (share <= 0.0) {
        continue;
      }
      cumulativeShare += share;
      if (randomSupplier.getAsDouble() * cumulativeShare < share) {
        selectedGroup = currentGroup;
      }
    }
    // Every share collapsed to zero (should not happen given the probe floor): fall back to the scan start so
    // the request is still routed somewhere rather than dropped.
    return selectedGroup < 0 ? startGroupId : selectedGroup;
  }

  /**
   * The target share for a group:
   * {@code max( (1 - skew) * evenShare + skew * strength(g) / totalStrength, floor )}. The floor keeps a slow
   * group from being fully starved so its latency stays observable.
   */
  private double shareForGroup(
      int groupId,
      double evenShare,
      double floor,
      double skew,
      double neutralStrength,
      double totalStrength) {
    double strengthShare = totalStrength > 0 ? strength(groupId, neutralStrength) / totalStrength : evenShare;
    double share = (1.0 - skew) * evenShare + skew * strengthShare;
    return Math.max(share, floor);
  }

  /**
   * A group's inferred strength: the reciprocal of its measured latency (faster => stronger). A group that has
   * not been measured yet (non-positive latency) is treated as neutral so it is neither flooded nor starved
   * before there is data.
   */
  private double strength(int groupId, double neutralStrength) {
    double latency = latencyProvider.applyAsDouble(groupId);
    if (latency <= 0.0) {
      return neutralStrength;
    }
    return 1.0 / Math.max(latency, MIN_LATENCY_MS);
  }

  /** The mean strength of the already-measured groups, used as the strength of not-yet-measured groups. */
  private double neutralStrength(int groupCount) {
    double sum = 0.0;
    int measured = 0;
    for (int g = 0; g < groupCount; ++g) {
      double latency = latencyProvider.applyAsDouble(g);
      if (latency > 0.0) {
        sum += 1.0 / Math.max(latency, MIN_LATENCY_MS);
        ++measured;
      }
    }
    // No group measured yet: any positive constant works since every group then gets the same neutral strength,
    // which reduces the strength term to an even split.
    return measured > 0 ? sum / measured : 1.0;
  }

  private double totalStrength(int groupCount, double neutralStrength) {
    double total = 0.0;
    for (int g = 0; g < groupCount; ++g) {
      total += strength(g, neutralStrength);
    }
    return total;
  }

  /**
   * The skew factor in {@code [0, 1]} derived from the current latency spread across measured groups:
   * {@code 0} while the slowest group is within {@code evenUntilLatencyRatio} of the fastest (treat the spread
   * as noise, stay even), {@code 1} once the spread reaches {@code fullSkewAtLatencyRatio} (full
   * latency-proportional split), and a ramp shaped by the in-band exponent {@code m} in between. Groups that
   * have not been measured yet ({@code latency <= 0}) are excluded from the spread; fewer than two measured
   * groups means there is no spread to act on, so routing stays even.
   */
  private double latencySkew(int groupCount) {
    double minLatency = Double.MAX_VALUE;
    double maxLatency = 0.0;
    int measured = 0;
    for (int g = 0; g < groupCount; ++g) {
      double latency = latencyProvider.applyAsDouble(g);
      if (latency <= 0.0) {
        continue;
      }
      double clamped = Math.max(latency, MIN_LATENCY_MS);
      minLatency = Math.min(minLatency, clamped);
      maxLatency = Math.max(maxLatency, clamped);
      ++measured;
    }
    if (measured < 2) {
      return 0.0;
    }
    double ratio = maxLatency / minLatency;
    if (ratio <= evenUntilLatencyRatio) {
      return 0.0;
    }
    if (ratio >= fullSkewAtLatencyRatio) {
      return 1.0;
    }
    double position = (ratio - evenUntilLatencyRatio) / (fullSkewAtLatencyRatio - evenUntilLatencyRatio);
    return interpolationExponent == 1.0 ? position : Math.pow(position, interpolationExponent);
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
