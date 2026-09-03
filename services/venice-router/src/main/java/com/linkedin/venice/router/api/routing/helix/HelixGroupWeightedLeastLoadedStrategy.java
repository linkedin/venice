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
 * a weighted-random draw. Each group's share interpolates between an even split and a
 * latency-proportional split as aggregate utilization rises:
 *
 * <pre>
 *   strength(g) = 1 / max(latency(g), MIN_LATENCY_MS)                // fast (low-latency) group => higher strength
 *   u           = clamp01( aggregate read-quota utilization )         // in [0, 1]
 *   skew        = 0                                        if u &lt;= evenUntilUtilization   // stay-even knob
 *                 1                                        if u &gt;= fullSkewAtUtilization   // full-skew knob
 *                 ((u - evenUntil) / (fullSkew - evenUntil))^m   otherwise                   // ramp between knobs
 *   share(g)    = (1 - skew) * (1 / G)  +  skew * strength(g) / sum(strength)
 *   share(g)    = max(share(g), PROBE_FLOOR_FRACTION * (1 / G))       // never fully starve a group
 * </pre>
 *
 * <ul>
 *   <li><b>latency(g)</b> — the group's measured average response time
 *       ({@link HelixGroupStats#getGroupResponseWaitingTimeAvg}). This is the <em>only</em> per-group signal the
 *       strategy needs: the faster group is simply the one whose measured latency is lower. There is no
 *       configured per-group capacity or read-quota allocation — the "strength" of a group is inferred from what
 *       it actually delivers. A group that has not served anything yet (latency {@code <= 0}) is treated as
 *       neutral (average strength) so it is neither flooded nor starved before it has been measured.</li>
 *   <li><b>u</b> — aggregate read-quota utilization: total admitted read capacity across the cluster divided by
 *       the total read quota. It is an <em>aggregate</em> signal (independent of how traffic is split across
 *       groups), so it decides only <em>how much</em> to skew, never <em>which</em> group to skew toward. When
 *       there is ample headroom ({@code u} small) routing is essentially even; as the cluster fills
 *       ({@code u} toward 1) routing shifts toward the faster groups. Until a real utilization signal is wired
 *       in, {@link #NO_UTILIZATION_SIGNAL} reports {@code 0} and the strategy stays even at every load.</li>
 *   <li><b>1 / G (even split)</b> — the low-utilization target. Spreading evenly while there is headroom keeps
 *       every group's read-quota consumption low, avoids the over-concentration that pushes a single group to
 *       its 429 ceiling, and keeps every group probed so its latency measurement stays fresh.</li>
 *   <li><b>evenUntilUtilization (stay-even knob)</b> — the utilization threshold up to which routing stays fully
 *       even regardless of the latency spread. Below it {@code skew == 0}, so every group gets exactly
 *       {@code 1 / G}. Raising it lets the cluster run even for longer before it reacts to latency; lowering it
 *       makes the strategy start protecting the faster groups earlier.</li>
 *   <li><b>fullSkewAtUtilization (full-skew knob)</b> — the utilization threshold at (and above) which routing
 *       reaches its full latency-proportional split ({@code skew == 1}). Between the two knobs the skew ramps
 *       from 0 to 1. Lowering it makes the cluster reach maximum protection before it is fully saturated;
 *       leaving it at 1 reserves full skew for the saturation point.</li>
 *   <li><b>m (in-band ramp exponent)</b> — shapes the ramp <em>between</em> the two knobs. {@code m == 1} is a
 *       straight linear ramp; {@code m > 1} keeps the ramp gentle just past {@code evenUntilUtilization} and
 *       steepens it near {@code fullSkewAtUtilization}. It only affects the transition band, not the flat
 *       even / full-skew regions.</li>
 *   <li><b>PROBE_FLOOR_FRACTION</b> — a floor that guarantees every group keeps a small share even at full
 *       utilization, so the slower groups are never fully starved and the router keeps observing their latency.
 *       This is what makes the latency signal self-correcting: routing more traffic to a fast group raises its
 *       latency and lowers its inferred strength, so the groups converge toward equal latency rather than one
 *       group being driven past its ceiling.</li>
 * </ul>
 *
 * <p>Trading a small, within-SLO increase in average / p99 latency for the avoidance of read-quota breaches
 * (429s) is the explicit design goal.
 *
 * <p>The counter-leak protection via {@link TimeoutProcessor} and the synchronized in-flight accounting are
 * preserved from {@link HelixGroupLeastLoadedStrategy}; the in-flight counters are kept for leak protection and
 * observability ({@link HelixGroupStats#recordGroupPendingRequest}) and do not influence the target share.
 */
public class HelixGroupWeightedLeastLoadedStrategy implements HelixGroupSelectionStrategy {
  public static final int MAX_ALLOWED_GROUP = 100;

  /**
   * Fallback utilization signal used when no aggregate read-quota utilization is wired in yet: utilization is
   * reported as {@code 0}, so {@code u^m} is {@code 0} and the strategy spreads evenly at every load. This makes
   * the safe default behaviour identical to plain even routing until a real utilization signal is provided.
   */
  public static final DoubleSupplier NO_UTILIZATION_SIGNAL = () -> 0.0;

  /**
   * Default stay-even threshold: routing stays fully even (skew {@code == 0}) while aggregate utilization is at
   * or below this fraction, so the cluster ignores the latency spread while it still has ample headroom.
   */
  public static final double DEFAULT_EVEN_UNTIL_UTILIZATION = 0.7;

  /**
   * Default full-skew threshold: at (and above) this aggregate utilization the routing reaches its full
   * latency-proportional split (skew {@code == 1}). Between {@link #DEFAULT_EVEN_UNTIL_UTILIZATION} and this the
   * skew ramps from 0 to 1.
   */
  public static final double DEFAULT_FULL_SKEW_AT_UTILIZATION = 1.0;

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
  private final DoubleSupplier utilizationSupplier;
  private final double evenUntilUtilization;
  private final double fullSkewAtUtilization;
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
        NO_UTILIZATION_SIGNAL,
        DEFAULT_EVEN_UNTIL_UTILIZATION,
        DEFAULT_FULL_SKEW_AT_UTILIZATION,
        DEFAULT_INTERPOLATION_EXPONENT,
        () -> ThreadLocalRandom.current().nextDouble());
  }

  /**
   * @param latencyProvider       maps a group id to its measured average response time in milliseconds; a
   *                              non-positive value means the group has not been measured yet and is treated as
   *                              neutral (average strength).
   * @param utilizationSupplier    supplies the aggregate read-quota utilization in {@code [0, 1]}; values are
   *                              clamped. This decides only how much to skew, not which group to skew toward.
   * @param evenUntilUtilization   stay-even threshold: routing stays fully even while utilization is at or below
   *                              this fraction. Must be in {@code [0, 1)} and strictly less than
   *                              {@code fullSkewAtUtilization}.
   * @param fullSkewAtUtilization  full-skew threshold: routing reaches its full latency-proportional split at or
   *                              above this fraction. Must be in {@code (0, 1]} and strictly greater than
   *                              {@code evenUntilUtilization}.
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
      DoubleSupplier utilizationSupplier,
      double evenUntilUtilization,
      double fullSkewAtUtilization,
      double interpolationExponent,
      DoubleSupplier randomSupplier) {
    if (!(evenUntilUtilization >= 0.0 && evenUntilUtilization < fullSkewAtUtilization
        && fullSkewAtUtilization <= 1.0)) {
      throw new VeniceException(
          "Require 0 <= evenUntilUtilization < fullSkewAtUtilization <= 1, but received evenUntilUtilization="
              + evenUntilUtilization + ", fullSkewAtUtilization=" + fullSkewAtUtilization);
    }
    this.timeoutProcessor = timeoutProcessor;
    this.timeoutInMS = timeoutInMS;
    this.helixGroupStats = helixGroupStats;
    this.latencyProvider = latencyProvider;
    this.utilizationSupplier = utilizationSupplier;
    this.evenUntilUtilization = evenUntilUtilization;
    this.fullSkewAtUtilization = fullSkewAtUtilization;
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
    double skew = skewFactor();
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
   * The skew factor in {@code [0, 1]}: {@code 0} at or below {@code evenUntilUtilization} (stay fully even),
   * {@code 1} at or above {@code fullSkewAtUtilization} (full latency-proportional split), and a ramp shaped by
   * the in-band exponent {@code m} in between.
   */
  private double skewFactor() {
    double utilization = utilizationSupplier.getAsDouble();
    if (utilization <= evenUntilUtilization) {
      return 0.0;
    }
    if (utilization >= fullSkewAtUtilization) {
      return 1.0;
    }
    double position = (utilization - evenUntilUtilization) / (fullSkewAtUtilization - evenUntilUtilization);
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
