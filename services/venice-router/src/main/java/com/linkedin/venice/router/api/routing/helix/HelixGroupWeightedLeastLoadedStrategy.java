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
 * <p>This strategy replaces the lexicographic (counter, latency) rule with a single per-group weight and a
 * weighted-random draw across all groups:
 *
 * <pre>
 *   weight(g) = headroom(g)^beta / (latency(g) * (inFlight(g) + 1))
 * </pre>
 *
 * <ul>
 *   <li><b>latency(g)</b> — lower latency yields a proportionally higher weight, so faster groups still earn
 *       more traffic (the Little's-Law share), but proportionally rather than winner-take-all.</li>
 *   <li><b>inFlight(g)</b> — folds the in-flight counter into the weight so a busier group is de-prioritized
 *       continuously instead of via a hard primary key.</li>
 *   <li><b>headroom(g)</b> — a value in [0, 1] describing the group's remaining read-quota headroom (1.0 =
 *       full headroom, 0.0 = at the 429 mark). As a group approaches its quota its weight decays toward zero
 *       and traffic sheds to groups with headroom. {@code beta} controls how aggressively the shed ramps.
 *       Crucially, because headroom multiplies the weight directly, a quota-saturated group is de-prioritized
 *       even when its in-flight counter looks low (saturated groups reject fast, so rejected requests never
 *       accumulate as pending and would otherwise make the group look attractive to a pure least-loaded rule).</li>
 * </ul>
 *
 * <p>The counter-leak protection via {@link TimeoutProcessor} and the synchronized accounting are preserved
 * from {@link HelixGroupLeastLoadedStrategy}.
 */
public class HelixGroupWeightedLeastLoadedStrategy implements HelixGroupSelectionStrategy {
  public static final int MAX_ALLOWED_GROUP = 100;

  /**
   * Fallback headroom provider used when no read-quota signal is wired in yet: every group is treated as
   * having full headroom, so the strategy reduces to latency- and in-flight-weighted balancing.
   */
  public static final IntToDoubleFunction FULL_HEADROOM = groupId -> 1.0;

  /** Default exponent applied to the headroom factor. */
  public static final double DEFAULT_HEADROOM_EXPONENT = 1.0;

  private final int[] counters = new int[MAX_ALLOWED_GROUP];
  private final TimeoutProcessor timeoutProcessor;
  private final long timeoutInMS;
  private final Map<Long, Pair<Integer, TimeoutProcessor.TimeoutFuture>> requestTimeoutFutureMap = new HashMap<>();
  private final HelixGroupStats helixGroupStats;
  private final IntToDoubleFunction groupHeadroomProvider;
  private final double headroomExponent;
  private final DoubleSupplier randomSupplier;

  public HelixGroupWeightedLeastLoadedStrategy(
      TimeoutProcessor timeoutProcessor,
      long timeoutInMS,
      HelixGroupStats helixGroupStats) {
    this(
        timeoutProcessor,
        timeoutInMS,
        helixGroupStats,
        FULL_HEADROOM,
        DEFAULT_HEADROOM_EXPONENT,
        () -> ThreadLocalRandom.current().nextDouble());
  }

  /**
   * @param groupHeadroomProvider maps a group id to its remaining read-quota headroom in [0, 1]; values are
   *                              clamped into that range.
   * @param headroomExponent      the {@code beta} exponent applied to the headroom factor.
   * @param randomSupplier        supplies a uniform random double in [0, 1); injectable so tests can be
   *                              deterministic.
   */
  public HelixGroupWeightedLeastLoadedStrategy(
      TimeoutProcessor timeoutProcessor,
      long timeoutInMS,
      HelixGroupStats helixGroupStats,
      IntToDoubleFunction groupHeadroomProvider,
      double headroomExponent,
      DoubleSupplier randomSupplier) {
    this.timeoutProcessor = timeoutProcessor;
    this.timeoutInMS = timeoutInMS;
    this.helixGroupStats = helixGroupStats;
    this.groupHeadroomProvider = groupHeadroomProvider;
    this.headroomExponent = headroomExponent;
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
   * {@code weight(g) / cumulativeWeight}, yielding a final selection probability of
   * {@code weight(g) / sum(weight)} in a single pass without allocation. The scan starts at
   * {@code startGroupId} purely to avoid biasing toward group 0; it does not affect the resulting
   * distribution.
   */
  private int pickWeightedGroup(int groupCount, int startGroupId) {
    // A group whose average latency is not yet known (unused) reports a non-positive latency; fall back to
    // the mean of the known latencies so a cold group competes on par instead of dominating (a negative
    // latency would otherwise win a naive comparison) or starving (never being explored).
    double neutralLatency = meanKnownLatency(groupCount);
    double cumulativeWeight = 0.0;
    int selectedGroup = -1;
    for (int i = 0; i < groupCount; ++i) {
      int currentGroup = (i + startGroupId) % groupCount;
      double weight = weightForGroup(currentGroup, neutralLatency);
      if (weight <= 0.0) {
        continue;
      }
      cumulativeWeight += weight;
      if (randomSupplier.getAsDouble() * cumulativeWeight < weight) {
        selectedGroup = currentGroup;
      }
    }
    // Every weight collapsed to zero (e.g. all groups reported zero headroom): fall back to the scan start so
    // the request is still routed somewhere rather than dropped.
    return selectedGroup < 0 ? startGroupId : selectedGroup;
  }

  private double weightForGroup(int groupId, double neutralLatency) {
    double avgLatency = helixGroupStats.getGroupResponseWaitingTimeAvg(groupId);
    double effectiveLatency = avgLatency > 0 ? avgLatency : neutralLatency;
    double headroom = clampHeadroom(groupHeadroomProvider.applyAsDouble(groupId));
    double headroomFactor = headroomExponent == 1.0 ? headroom : Math.pow(headroom, headroomExponent);
    // inFlight + 1 keeps the denominator positive and lets an idle group (0 in-flight) still be bounded.
    return headroomFactor / (effectiveLatency * (counters[groupId] + 1));
  }

  private double meanKnownLatency(int groupCount) {
    double sum = 0.0;
    int known = 0;
    for (int g = 0; g < groupCount; ++g) {
      double avgLatency = helixGroupStats.getGroupResponseWaitingTimeAvg(g);
      if (avgLatency > 0) {
        sum += avgLatency;
        ++known;
      }
    }
    return known == 0 ? 1.0 : sum / known;
  }

  private static double clampHeadroom(double headroom) {
    if (headroom < 0.0) {
      return 0.0;
    }
    return Math.min(headroom, 1.0);
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
