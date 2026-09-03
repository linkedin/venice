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
 *   strength(g) = 1 / max(latency(g), MIN_LATENCY_MS)          // fast (low-latency) group => higher strength
 *   u           = clamp01( aggregate read-quota utilization )   // in [0, 1]
 *   share(g)    = (1 - u^m) * (1 / G)  +  u^m * strength(g) / sum(strength)
 *   share(g)    = max(share(g), PROBE_FLOOR_FRACTION * (1 / G)) // never fully starve a group
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
 *   <li><b>m (interpolation exponent)</b> — controls how late the shift from even to latency-proportional
 *       happens. Because {@code m > 1}, {@code u^m} stays near zero for most of the utilization range and only
 *       climbs sharply as {@code u} approaches 1, so the cluster runs even for most of its life and only skews
 *       toward the faster groups when it is genuinely close to saturation.</li>
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
   * Default interpolation exponent {@code m}. Values &gt; 1 keep {@code u^m} near zero for most of the
   * utilization range and make it climb sharply only as utilization approaches 1, so the cluster stays even for
   * most of its life and skews toward the faster groups only when close to saturation.
   */
  public static final double DEFAULT_INTERPOLATION_EXPONENT = 3.0;

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
        DEFAULT_INTERPOLATION_EXPONENT,
        () -> ThreadLocalRandom.current().nextDouble());
  }

  /**
   * @param latencyProvider       maps a group id to its measured average response time in milliseconds; a
   *                              non-positive value means the group has not been measured yet and is treated as
   *                              neutral (average strength).
   * @param utilizationSupplier   supplies the aggregate read-quota utilization in {@code [0, 1]}; values are
   *                              clamped. This decides only how much to skew, not which group to skew toward.
   * @param interpolationExponent the {@code m} exponent controlling how late routing shifts from an even split
   *                              to a latency-proportional split as utilization rises.
   * @param randomSupplier        supplies a uniform random double in [0, 1); injectable so tests can be
   *                              deterministic.
   */
  public HelixGroupWeightedLeastLoadedStrategy(
      TimeoutProcessor timeoutProcessor,
      long timeoutInMS,
      HelixGroupStats helixGroupStats,
      IntToDoubleFunction latencyProvider,
      DoubleSupplier utilizationSupplier,
      double interpolationExponent,
      DoubleSupplier randomSupplier) {
    this.timeoutProcessor = timeoutProcessor;
    this.timeoutInMS = timeoutInMS;
    this.helixGroupStats = helixGroupStats;
    this.latencyProvider = latencyProvider;
    this.utilizationSupplier = utilizationSupplier;
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
    double utilizationPower = utilizationPower();
    double evenShare = 1.0 / groupCount;
    double floor = PROBE_FLOOR_FRACTION * evenShare;
    // Pre-pass: total inferred strength and the neutral strength used for not-yet-measured groups.
    double neutralStrength = neutralStrength(groupCount);
    double totalStrength = totalStrength(groupCount, neutralStrength);

    double cumulativeShare = 0.0;
    int selectedGroup = -1;
    for (int i = 0; i < groupCount; ++i) {
      int currentGroup = (i + startGroupId) % groupCount;
      double share = shareForGroup(currentGroup, evenShare, floor, utilizationPower, neutralStrength, totalStrength);
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
   * {@code max( (1 - u^m) * evenShare + u^m * strength(g) / totalStrength, floor )}. The floor keeps a slow
   * group from being fully starved so its latency stays observable.
   */
  private double shareForGroup(
      int groupId,
      double evenShare,
      double floor,
      double utilizationPower,
      double neutralStrength,
      double totalStrength) {
    double strengthShare = totalStrength > 0 ? strength(groupId, neutralStrength) / totalStrength : evenShare;
    double share = (1.0 - utilizationPower) * evenShare + utilizationPower * strengthShare;
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

  /** {@code u^m}, where {@code u} is the clamped aggregate utilization and {@code m} is the interpolation exponent. */
  private double utilizationPower() {
    double utilization = utilizationSupplier.getAsDouble();
    if (utilization <= 0.0) {
      return 0.0;
    }
    if (utilization >= 1.0) {
      return 1.0;
    }
    return interpolationExponent == 1.0 ? utilization : Math.pow(utilization, interpolationExponent);
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
