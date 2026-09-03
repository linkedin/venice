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
 * A capacity-aware Helix group selection strategy that spreads load across groups by a continuous weight
 * instead of the deterministic winner-take-all tie-break used by {@link HelixGroupLeastLoadedStrategy}.
 *
 * <p>The legacy least-loaded strategy is lexicographic: it first picks the group(s) with the fewest in-flight
 * requests, then, among ties, deterministically picks the single lowest-latency group. At low per-router
 * in-flight the counters are almost always tied at (or near) zero, so the latency tie-break fires on nearly
 * every request and funnels a disproportionate share of traffic to whichever group is momentarily fastest,
 * even when its latency edge is sub-millisecond. That over-concentration drives one group's nodes toward
 * their read-quota ceiling (429s) while other groups sit idle with headroom.
 *
 * <p>This strategy replaces the lexicographic (counter, latency) rule with a single per-group target
 * <em>share</em> and a weighted-random draw. The share of each group interpolates between an even split and a
 * capacity-proportional split as aggregate utilization rises:
 *
 * <pre>
 *   u        = clamp01( sum(used(g)) / sum(capacity(g)) )     // aggregate utilization in [0, 1]
 *   share(g) = (1 - u^m) * (1 / G)  +  u^m * (capacity(g) / sum(capacity))
 * </pre>
 *
 * <ul>
 *   <li><b>u</b> — aggregate utilization: total consumed read capacity across all groups divided by total
 *       allocated read capacity. It is the single knob that decides how even vs. how skewed routing should be.
 *       When there is ample headroom ({@code u} small) routing is essentially even; as the cluster fills
 *       ({@code u} toward 1) routing shifts toward the capacity-proportional split.</li>
 *   <li><b>capacity(g)</b> — the group's allocated read capacity (its read-quota allocation). A "stronger"
 *       group has a larger capacity and therefore absorbs a larger proportional share as utilization climbs.
 *       When no capacity signal is wired in, {@link #UNIFORM_CAPACITY} treats every group as equal, so the
 *       capacity-proportional term also reduces to {@code 1 / G} and the strategy stays even at every load.</li>
 *   <li><b>1 / G (even split)</b> — the low-utilization target. Spreading evenly while there is headroom keeps
 *       every group's read-quota consumption low and avoids the over-concentration that pushes a single group
 *       to its 429 ceiling.</li>
 *   <li><b>m (interpolation exponent)</b> — controls how late the shift from even to capacity-proportional
 *       happens. Because {@code m > 1}, {@code u^m} stays near zero for most of the utilization range and only
 *       climbs sharply as {@code u} approaches 1, so the cluster runs even for most of its life and only skews
 *       toward the stronger groups when it is genuinely close to saturation.</li>
 * </ul>
 *
 * <p>Latency is deliberately <em>not</em> a routing input here: under this model latency is a
 * <em>consequence</em> of load, and the aggregate-utilization signal already captures how close the cluster is
 * to its read-quota ceiling. Trading a small, within-SLO increase in average / p99 latency for the avoidance of
 * read-quota breaches (429s) is the explicit design goal.
 *
 * <p>The counter-leak protection via {@link TimeoutProcessor} and the synchronized in-flight accounting are
 * preserved from {@link HelixGroupLeastLoadedStrategy}; the in-flight counters are kept for leak protection and
 * observability ({@link HelixGroupStats#recordGroupPendingRequest}) and do not influence the target share.
 */
public class HelixGroupWeightedLeastLoadedStrategy implements HelixGroupSelectionStrategy {
  public static final int MAX_ALLOWED_GROUP = 100;

  /**
   * Fallback capacity provider used when no read-quota allocation signal is wired in yet: every group is
   * treated as having equal capacity, so the capacity-proportional term also reduces to an even split and the
   * strategy spreads evenly at every utilization level.
   */
  public static final IntToDoubleFunction UNIFORM_CAPACITY = groupId -> 1.0;

  /**
   * Fallback usage provider used when no consumed-read-capacity signal is wired in yet: every group reports
   * zero usage, so aggregate utilization is 0 and routing is even.
   */
  public static final IntToDoubleFunction ZERO_USAGE = groupId -> 0.0;

  /**
   * Default interpolation exponent {@code m}. Values &gt; 1 keep {@code u^m} near zero for most of the
   * utilization range and make it climb sharply only as utilization approaches 1, so the cluster stays even for
   * most of its life and skews toward the stronger groups only when close to saturation.
   */
  public static final double DEFAULT_INTERPOLATION_EXPONENT = 3.0;

  private final int[] counters = new int[MAX_ALLOWED_GROUP];
  private final TimeoutProcessor timeoutProcessor;
  private final long timeoutInMS;
  private final Map<Long, Pair<Integer, TimeoutProcessor.TimeoutFuture>> requestTimeoutFutureMap = new HashMap<>();
  private final HelixGroupStats helixGroupStats;
  private final IntToDoubleFunction capacityProvider;
  private final IntToDoubleFunction usageProvider;
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
        UNIFORM_CAPACITY,
        ZERO_USAGE,
        DEFAULT_INTERPOLATION_EXPONENT,
        () -> ThreadLocalRandom.current().nextDouble());
  }

  /**
   * @param capacityProvider      maps a group id to its allocated read capacity (read-quota allocation);
   *                              non-positive values are treated as zero.
   * @param usageProvider         maps a group id to its consumed read capacity; non-positive values are treated
   *                              as zero.
   * @param interpolationExponent the {@code m} exponent controlling how late routing shifts from an even split
   *                              to a capacity-proportional split as utilization rises.
   * @param randomSupplier        supplies a uniform random double in [0, 1); injectable so tests can be
   *                              deterministic.
   */
  public HelixGroupWeightedLeastLoadedStrategy(
      TimeoutProcessor timeoutProcessor,
      long timeoutInMS,
      HelixGroupStats helixGroupStats,
      IntToDoubleFunction capacityProvider,
      IntToDoubleFunction usageProvider,
      double interpolationExponent,
      DoubleSupplier randomSupplier) {
    this.timeoutProcessor = timeoutProcessor;
    this.timeoutInMS = timeoutInMS;
    this.helixGroupStats = helixGroupStats;
    this.capacityProvider = capacityProvider;
    this.usageProvider = usageProvider;
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
   * {@code share(g) / cumulativeShare}, yielding a final selection probability of {@code share(g)} (the shares
   * sum to 1) in a single pass without allocation. The scan starts at {@code startGroupId} purely to avoid
   * biasing toward group 0; it does not affect the resulting distribution.
   */
  private int pickWeightedGroup(int groupCount, int startGroupId) {
    double utilizationPower = utilizationPower(groupCount);
    double totalCapacity = totalCapacity(groupCount);
    double evenShare = 1.0 / groupCount;
    double cumulativeShare = 0.0;
    int selectedGroup = -1;
    for (int i = 0; i < groupCount; ++i) {
      int currentGroup = (i + startGroupId) % groupCount;
      double share = shareForGroup(currentGroup, evenShare, utilizationPower, totalCapacity);
      if (share <= 0.0) {
        continue;
      }
      cumulativeShare += share;
      if (randomSupplier.getAsDouble() * cumulativeShare < share) {
        selectedGroup = currentGroup;
      }
    }
    // Every share collapsed to zero (should not happen since shares sum to 1): fall back to the scan start so
    // the request is still routed somewhere rather than dropped.
    return selectedGroup < 0 ? startGroupId : selectedGroup;
  }

  /**
   * The target share for a group: {@code (1 - u^m) * evenShare + u^m * capacity(g) / totalCapacity}. When there
   * is no capacity signal ({@code totalCapacity <= 0}) the capacity term degenerates to the even split, so the
   * share is simply {@code evenShare}.
   */
  private double shareForGroup(int groupId, double evenShare, double utilizationPower, double totalCapacity) {
    double capacityShare =
        totalCapacity > 0 ? nonNegative(capacityProvider.applyAsDouble(groupId)) / totalCapacity : evenShare;
    return (1.0 - utilizationPower) * evenShare + utilizationPower * capacityShare;
  }

  /** {@code u^m}, where {@code u} is aggregate utilization in [0, 1] and {@code m} is the interpolation exponent. */
  private double utilizationPower(int groupCount) {
    double totalCapacity = totalCapacity(groupCount);
    if (totalCapacity <= 0) {
      return 0.0;
    }
    double totalUsage = 0.0;
    for (int g = 0; g < groupCount; ++g) {
      totalUsage += nonNegative(usageProvider.applyAsDouble(g));
    }
    double utilization = totalUsage / totalCapacity;
    if (utilization <= 0.0) {
      return 0.0;
    }
    if (utilization >= 1.0) {
      return 1.0;
    }
    return interpolationExponent == 1.0 ? utilization : Math.pow(utilization, interpolationExponent);
  }

  private double totalCapacity(int groupCount) {
    double total = 0.0;
    for (int g = 0; g < groupCount; ++g) {
      total += nonNegative(capacityProvider.applyAsDouble(g));
    }
    return total;
  }

  private static double nonNegative(double value) {
    return value > 0.0 ? value : 0.0;
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
