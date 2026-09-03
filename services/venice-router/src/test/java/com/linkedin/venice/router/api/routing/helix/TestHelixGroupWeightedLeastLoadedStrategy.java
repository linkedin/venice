package com.linkedin.venice.router.api.routing.helix;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.alpini.base.concurrency.TimeoutProcessor;
import com.linkedin.venice.stats.routing.HelixGroupStats;
import java.util.Arrays;
import java.util.Random;
import java.util.function.IntToDoubleFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Integration-style tests for {@link HelixGroupWeightedLeastLoadedStrategy} that drive a stream of mock
 * requests through the strategy and snapshot how many queries were routed to each group as the per-group
 * latency, in-flight load, read-quota headroom, and per-request latency budget change.
 *
 * <p>The per-group latency the strategy reads is controlled through a mocked
 * {@link HelixGroupStats#getGroupResponseWaitingTimeAvg(int)} so each scenario can move the latency vector
 * deterministically. The per-request latency budget is the long-tail retry threshold the router already
 * resolves per key range (see
 * {@link com.linkedin.venice.router.api.path.VenicePath#getLongTailRetryThresholdMs()}); here it is passed
 * directly to {@link HelixGroupWeightedLeastLoadedStrategy#selectGroup(long, int, int)}. Randomness in the
 * weighted draw is made deterministic by injecting a seeded {@link Random}, so the snapshots are reproducible.
 */
public class TestHelixGroupWeightedLeastLoadedStrategy {
  private static final Logger LOGGER = LogManager.getLogger(TestHelixGroupWeightedLeastLoadedStrategy.class);
  private static final long TIMEOUT_MS = 10000;
  private static final long SEED = 42;
  private static final int NO_BUDGET = HelixGroupSelectionStrategy.NO_LATENCY_BUDGET;

  /** A mocked HelixGroupStats whose per-group average latency is backed by a mutable array. */
  private static HelixGroupStats statsWithLatencies(double[] latencies) {
    HelixGroupStats stats = mock(HelixGroupStats.class);
    when(stats.getGroupResponseWaitingTimeAvg(anyInt()))
        .thenAnswer(invocation -> latencies[(int) invocation.getArgument(0)]);
    return stats;
  }

  /**
   * A mocked HelixGroupStats that returns the per-group base latency plus fresh Gaussian jitter on every read,
   * modelling the per-request latency variation a real router observes. The jitter is drawn from the supplied
   * seeded {@link Random} so the stream is deterministic. Draws are clamped to a small positive floor so an
   * unlucky sample can never produce a non-positive latency.
   */
  private static HelixGroupStats statsWithJitteredLatencies(double[] baseLatencies, Random jitter, double stdDevMs) {
    HelixGroupStats stats = mock(HelixGroupStats.class);
    when(stats.getGroupResponseWaitingTimeAvg(anyInt())).thenAnswer(invocation -> {
      int group = invocation.getArgument(0);
      return Math.max(0.1, baseLatencies[group] + jitter.nextGaussian() * stdDevMs);
    });
    return stats;
  }

  private static TimeoutProcessor mockTimeoutProcessor() {
    TimeoutProcessor timeoutProcessor = mock(TimeoutProcessor.class);
    doReturn(mock(TimeoutProcessor.TimeoutFuture.class)).when(timeoutProcessor).schedule(any(), anyLong(), any());
    return timeoutProcessor;
  }

  /**
   * Route {@code requestCount} requests through the strategy with the given per-request latency budget,
   * finishing each one immediately (so in-flight stays ~0 and the routed share is driven purely by the
   * latency-budget/headroom weight). Returns the per-group routed counts.
   */
  private static int[] routeAndFinish(
      HelixGroupSelectionStrategy strategy,
      double[] latencies,
      int groupCount,
      long startRequestId,
      int requestCount,
      int latencyBudgetMs) {
    int[] routed = new int[groupCount];
    for (int i = 0; i < requestCount; i++) {
      long requestId = startRequestId + i;
      int group = strategy.selectGroup(requestId, groupCount, latencyBudgetMs);
      routed[group]++;
      strategy.finishRequest(requestId, group, latencies[group]);
    }
    return routed;
  }

  private static HelixGroupWeightedLeastLoadedStrategy weightedStrategy(HelixGroupStats stats, Random random) {
    return new HelixGroupWeightedLeastLoadedStrategy(
        mockTimeoutProcessor(),
        TIMEOUT_MS,
        stats,
        HelixGroupWeightedLeastLoadedStrategy.FULL_HEADROOM,
        HelixGroupWeightedLeastLoadedStrategy.DEFAULT_HEADROOM_EXPONENT,
        HelixGroupWeightedLeastLoadedStrategy.DEFAULT_LATENCY_URGENCY_EXPONENT,
        random::nextDouble);
  }

  private static int argMax(int[] values) {
    int idx = 0;
    for (int i = 1; i < values.length; i++) {
      if (values[i] > values[idx]) {
        idx = i;
      }
    }
    return idx;
  }

  private static int argMin(double[] values) {
    int idx = 0;
    for (int i = 1; i < values.length; i++) {
      if (values[i] < values[idx]) {
        idx = i;
      }
    }
    return idx;
  }

  /**
   * The headline behaviour: the strategy only skews when a group's latency approaches its SLA budget (the
   * long-tail retry threshold), not for a small latency edge while everyone is healthy.
   *
   * <p>Five groups start equal and healthy against a 100ms budget. Group 0 stays fast (20ms) while groups 1-4
   * ramp their latency up toward the budget. We snapshot the routed distribution at each step and assert:
   *
   * <ul>
   *   <li>While the other groups are still comfortably under the budget, routing stays roughly even -- the
   *       small latency edge does <em>not</em> concentrate traffic on group 0 (this is what preserves
   *       read-quota headroom and avoids the 429s the legacy tie-break caused).</li>
   *   <li>Only as groups 1-4 approach the budget does their share collapse and traffic shed onto the fast
   *       group; group 0's share grows monotonically and ends as a clear majority.</li>
   *   <li>The four ramping groups, always equal to each other, keep equal shares at every step.</li>
   * </ul>
   */
  @Test
  public void testOnlySkewsWhenGroupsApproachRetryBudget() {
    int groupCount = 5;
    int budgetMs = 100;
    double[] latencies = new double[groupCount];
    Arrays.fill(latencies, 20.0);

    Random random = new Random(SEED);
    HelixGroupWeightedLeastLoadedStrategy strategy = weightedStrategy(statsWithLatencies(latencies), random);

    int requestsPerSnapshot = 10000;
    long nextRequestId = 0;
    double previousFastShare = -1.0;
    int snapshotIndex = 0;
    // Group 0 stays at 20ms; the other four ramp from healthy (20ms) up to near the 100ms budget.
    double[] otherLatencySteps = { 20.0, 40.0, 60.0, 80.0, 95.0 };

    LOGGER.info(
        "Latency-budget shed ({}ms budget). Snapshot | group latencies (ms) | routed per group | fast g0 share",
        budgetMs);

    for (double otherLatencyMs: otherLatencySteps) {
      for (int g = 1; g < groupCount; g++) {
        latencies[g] = otherLatencyMs;
      }

      int[] routed = routeAndFinish(strategy, latencies, groupCount, nextRequestId, requestsPerSnapshot, budgetMs);
      nextRequestId += requestsPerSnapshot;
      double fastShare = routed[0] / (double) requestsPerSnapshot;

      LOGGER.info(
          String.format(
              "   %2d    | %-24s | %-32s | %5.1f%%",
              snapshotIndex,
              Arrays.toString(latencies),
              Arrays.toString(routed),
              100 * fastShare));

      // The four ramping groups are always identical, so their shares must stay within sampling noise.
      int minOther = routed[1];
      int maxOther = routed[1];
      for (int g = 2; g < groupCount; g++) {
        minOther = Math.min(minOther, routed[g]);
        maxOther = Math.max(maxOther, routed[g]);
      }
      Assert.assertTrue(
          (maxOther - minOther) < 0.06 * requestsPerSnapshot,
          "Equal-latency groups should get equal shares; routed=" + Arrays.toString(routed));

      if (otherLatencyMs <= 60.0) {
        // Others are still well under the 100ms budget: the fast group must NOT hoard traffic just for being
        // slightly quicker -- routing stays close to an even 20% split.
        Assert.assertTrue(
            fastShare < 0.26,
            "While all groups are healthy the fast group should not over-concentrate; share=" + fastShare
                + " at otherLatency=" + otherLatencyMs);
      }
      if (snapshotIndex > 0) {
        Assert.assertTrue(
            fastShare >= previousFastShare - 0.01,
            "Fast group share should grow (not shrink) as the other groups approach the budget; was "
                + previousFastShare + " now " + fastShare);
      }
      previousFastShare = fastShare;
      snapshotIndex++;
    }

    // Once the other groups are near the budget (95ms of 100ms), traffic has clearly shed onto the fast group.
    Assert.assertTrue(
        previousFastShare > 0.45,
        "As the other groups near their SLA budget the fast group should hold a clear majority; share="
            + previousFastShare);
  }

  /**
   * Oscillation scenario: the other groups' latency follows a sine wave that swings from healthy up to near the
   * SLA budget and back. Routing should "breathe" with it -- close to even at the troughs (minimal skew, quota
   * preserved), skewing onto the steady fast group at the peaks (so requests still complete within the latency
   * budget), and relaxing back to even once the wave subsides. This proves the gate is not sticky: the skew
   * appears only while latency is high and disappears when it recovers, with no hysteresis between the two
   * troughs.
   *
   * <p>Latency is driven deterministically (no jitter) so the wave, and the routed response to it, are clean to
   * read; the jittered, production-latency reproduction lives in
   * {@link #testBaselineOldStrategyOverConcentratesVersusNewStrategy}.
   */
  @Test
  public void testRoutingBreathesWithSinusoidalLatency() {
    int groupCount = 5;
    int budgetMs = 100;
    double troughMs = 20.0; // all groups healthy -> even routing
    double peakMs = 95.0; // other groups near the budget -> shed to the fast group
    double mid = (peakMs + troughMs) / 2;
    double amplitude = (peakMs - troughMs) / 2;

    double[] latencies = new double[groupCount];
    Arrays.fill(latencies, troughMs);
    Random random = new Random(SEED);
    HelixGroupWeightedLeastLoadedStrategy strategy = weightedStrategy(statsWithLatencies(latencies), random);

    int requestsPerSnapshot = 10000;
    int steps = 16; // one full period
    long nextRequestId = 0;
    double[] fastShares = new double[steps + 1];

    LOGGER.info(
        "Sinusoidal latency ({}ms budget, group 0 steady at {}ms). Step | others latency (ms) | routed | fast g0 share",
        budgetMs,
        (int) troughMs);

    for (int step = 0; step <= steps; step++) {
      // Start at the trough (sin = -1), rise to the peak (sin = +1) at the half-period, and return to the trough.
      double phase = -Math.PI / 2 + (2 * Math.PI * step) / steps;
      double othersLatency = mid + amplitude * Math.sin(phase);
      for (int g = 1; g < groupCount; g++) {
        latencies[g] = othersLatency;
      }

      int[] routed = routeAndFinish(strategy, latencies, groupCount, nextRequestId, requestsPerSnapshot, budgetMs);
      nextRequestId += requestsPerSnapshot;
      double fastShare = routed[0] / (double) requestsPerSnapshot;
      fastShares[step] = fastShare;

      LOGGER.info(
          String.format(
              "  %2d  | %3d | %-32s | %5.1f%%",
              step,
              Math.round(othersLatency),
              Arrays.toString(routed),
              100 * fastShare));
    }

    int peakStep = steps / 2; // sin = +1 here -> maximum latency on the other groups
    // Troughs at both ends: routing is close to even (minimal skew).
    Assert.assertTrue(
        fastShares[0] < 0.26,
        "Fast group should be near even at the starting trough; share=" + fastShares[0]);
    Assert.assertTrue(
        fastShares[steps] < 0.26,
        "Routing should return to even once the wave subsides; share=" + fastShares[steps]);
    // Peak: traffic skews onto the steady fast group so requests stay within the budget.
    Assert.assertTrue(
        fastShares[peakStep] > 0.45,
        "Fast group should absorb the peak of the wave; share=" + fastShares[peakStep]);
    // The skew tracks the wave rather than latching: the peak is far above both troughs.
    Assert.assertTrue(
        fastShares[peakStep] > fastShares[0] + 0.2 && fastShares[peakStep] > fastShares[steps] + 0.2,
        "Peak skew should clearly exceed the trough skew");
    // ...and the two troughs are essentially identical (fully relaxed, no hysteresis).
    Assert.assertTrue(
        Math.abs(fastShares[0] - fastShares[steps]) < 0.05,
        "The wave should return routing to its original evenness; start=" + fastShares[0] + " end="
            + fastShares[steps]);
  }

  /**
   * The weight folds in the in-flight counter, so a large backlog of un-finished (in-flight) requests on a
   * group must steer subsequent traffic away from it toward groups with spare capacity, independent of latency.
   */
  @Test
  public void testInFlightLoadSteersAwayFromBackloggedGroup() {
    int groupCount = 3;
    double[] latencies = new double[] { 20.0, 20.0, 20.0 };
    Random random = new Random(SEED);
    HelixGroupWeightedLeastLoadedStrategy strategy = weightedStrategy(statsWithLatencies(latencies), random);

    // Build a deterministic in-flight backlog on group 0 by pushing groups 1 and 2 past a tight budget (so
    // their latency gate collapses and they are shed) while group 0 stays healthy and absorbs a run of
    // un-finished selects. Then restore equal, budget-free latency for the measurement batch.
    int tightBudgetMs = 30;
    latencies[0] = 8.0;
    latencies[1] = 45.0;
    latencies[2] = 45.0;
    long requestId = 0;
    for (int i = 0; i < 60; i++) {
      strategy.selectGroup(requestId++, groupCount, tightBudgetMs);
    }
    Arrays.fill(latencies, 20.0);

    // Group 0 now carries a large persistent in-flight backlog while groups 1 and 2 are idle. With equal
    // latency and no budget in play, the new traffic should overwhelmingly avoid the backlogged group.
    int[] routed = routeAndFinish(strategy, latencies, groupCount, requestId, 9000, NO_BUDGET);
    Assert.assertTrue(
        routed[1] + routed[2] > routed[0],
        "Backlogged group should shed new traffic to less-loaded groups; routed=" + Arrays.toString(routed));
    Assert.assertTrue(
        routed[0] < 0.2 * 9000,
        "Heavily backlogged group should get a small fraction of new traffic; routed=" + Arrays.toString(routed));
  }

  /**
   * The read-quota headroom factor sheds traffic away from a group approaching its quota ceiling even though
   * its in-flight counter looks low (a quota-saturated group rejects fast, so it never accumulates pending
   * load). This is the read-quota-aware behavior the routing needs to avoid 429s while other groups still have
   * headroom. Latency is held equal and the budget disabled so the quota term is exercised in isolation.
   */
  @Test
  public void testQuotaHeadroomShedsNearLimitGroup() {
    int groupCount = 3;
    double[] latencies = new double[] { 20.0, 20.0, 20.0 };
    // Group 0 is essentially out of read-quota headroom; the others have full headroom.
    double[] headroom = new double[] { 0.02, 1.0, 1.0 };
    IntToDoubleFunction headroomProvider = groupId -> headroom[groupId];

    Random random = new Random(SEED);
    HelixGroupWeightedLeastLoadedStrategy strategy = new HelixGroupWeightedLeastLoadedStrategy(
        mockTimeoutProcessor(),
        TIMEOUT_MS,
        statsWithLatencies(latencies),
        headroomProvider,
        2.0,
        HelixGroupWeightedLeastLoadedStrategy.DEFAULT_LATENCY_URGENCY_EXPONENT,
        random::nextDouble);

    int[] routed = routeAndFinish(strategy, latencies, groupCount, 0, 9000, NO_BUDGET);

    // Group 0 is near its quota ceiling and should receive far less traffic than the groups with headroom.
    Assert.assertTrue(
        routed[0] < routed[1] && routed[0] < routed[2],
        "Group near its read-quota ceiling should be shed; routed=" + Arrays.toString(routed));
    Assert.assertTrue(
        routed[0] < 0.1 * 9000,
        "A group at ~2% headroom should get a small fraction of traffic; routed=" + Arrays.toString(routed));
  }

  /**
   * The read-quota story from the originating issue: routing should get <em>more even as offered load grows
   * toward the aggregate read quota</em>, so every group is used and none breaches its per-group ceiling (the
   * 429s), even though a latency edge would happily concentrate traffic on the fast group when there is spare
   * quota.
   *
   * <p>The scenario gives group 0 a latency edge (20ms vs 85ms against a 100ms budget) so the latency gate
   * alone would over-weight it, and models a per-group read quota via a dynamic headroom provider:
   * {@code headroom(g) = 1 - consumed(g)/quota(g)}, where {@code consumed} is incremented as the batch routes,
   * so a filling group self-corrects mid-batch (a saturated group drops to zero headroom and stops being
   * picked). Sweeping the offered load from a small fraction of the aggregate quota up to the full aggregate:
   *
   * <ul>
   *   <li>At low load the latency edge dominates -- traffic skews onto the fast group (uneven), which is fine
   *       because there is ample quota headroom and no group is anywhere near its ceiling.</li>
   *   <li>As the load approaches the aggregate quota the fast group fills first, its headroom collapses, and
   *       the quota factor forces the remaining load onto the other groups -- the distribution flattens toward
   *       even, every group is used, and no group is driven past its quota (so no 429s).</li>
   * </ul>
   */
  @Test
  public void testRoutingEvensOutAsRpsApproachesQuota() {
    int groupCount = 5;
    int perGroupQuota = 10000; // reads served per quota window, per group
    int aggregateQuota = perGroupQuota * groupCount; // 50000
    int budgetMs = 100;
    double fastLatencyMs = 20.0; // group 0 -- the latency edge that would skew routing absent quota pressure
    double slowLatencyMs = 85.0; // groups 1-4
    double evenShare = 1.0 / groupCount;

    double[] latencies = new double[groupCount];
    latencies[0] = fastLatencyMs;
    for (int g = 1; g < groupCount; g++) {
      latencies[g] = slowLatencyMs;
    }

    // Per-group consumed reads in the current window; the headroom provider reads this live so a filling group
    // is down-weighted mid-batch. Reset for each offered-load level.
    int[] consumed = new int[groupCount];
    IntToDoubleFunction headroomProvider = groupId -> Math.max(0.0, 1.0 - consumed[groupId] / (double) perGroupQuota);

    // Offered load as a fraction of the aggregate quota: from lightly loaded up to fully saturated.
    int[] offeredLoads = { 5000, 20000, 35000, 45000, aggregateQuota };

    LOGGER.info(
        "RPS vs read quota (aggregate quota {}, group 0 fast @ {}ms, others @ {}ms, {}ms budget).",
        aggregateQuota,
        (int) fastLatencyMs,
        (int) slowLatencyMs,
        budgetMs);
    LOGGER.info("  offered load | utilization | fast g0 share | max group utilization | consumed per group");

    long nextRequestId = 0;
    double previousFastShare = Double.MAX_VALUE;
    double firstFastShare = -1;
    double lastFastShare = -1;

    for (int offeredLoad: offeredLoads) {
      Arrays.fill(consumed, 0);
      Random random = new Random(SEED);
      HelixGroupWeightedLeastLoadedStrategy strategy = new HelixGroupWeightedLeastLoadedStrategy(
          mockTimeoutProcessor(),
          TIMEOUT_MS,
          statsWithLatencies(latencies),
          headroomProvider,
          2.0,
          HelixGroupWeightedLeastLoadedStrategy.DEFAULT_LATENCY_URGENCY_EXPONENT,
          random::nextDouble);

      for (int i = 0; i < offeredLoad; i++) {
        long requestId = nextRequestId++;
        int group = strategy.selectGroup(requestId, groupCount, budgetMs);
        consumed[group]++;
        strategy.finishRequest(requestId, group, latencies[group]);
      }

      int maxConsumed = consumed[0];
      int minConsumed = consumed[0];
      for (int g = 1; g < groupCount; g++) {
        maxConsumed = Math.max(maxConsumed, consumed[g]);
        minConsumed = Math.min(minConsumed, consumed[g]);
      }
      double fastShare = consumed[0] / (double) offeredLoad;
      double utilization = offeredLoad / (double) aggregateQuota;
      double maxGroupUtilization = maxConsumed / (double) perGroupQuota;

      LOGGER.info(
          String.format(
              "  %11d  | %10.0f%% | %12.1f%% | %20.2f  | %s",
              offeredLoad,
              100 * utilization,
              100 * fastShare,
              maxGroupUtilization,
              Arrays.toString(consumed)));

      // No group may ever be driven past its read quota -- that is the 429 this whole change exists to prevent.
      Assert.assertTrue(
          maxGroupUtilization <= 1.0 + 1e-9,
          "No group may exceed its read quota; consumed=" + Arrays.toString(consumed));
      // Evenness must not get worse as load grows (allowing a small sampling epsilon).
      Assert.assertTrue(
          fastShare <= previousFastShare + 0.015,
          "Routing should get more even (fast-group share must not grow) as load approaches quota; was "
              + previousFastShare + " now " + fastShare);

      if (firstFastShare < 0) {
        firstFastShare = fastShare;
      }
      lastFastShare = fastShare;
      previousFastShare = fastShare;
    }

    // At low load the latency edge is allowed to skew traffic onto the fast group.
    Assert.assertTrue(
        firstFastShare > evenShare + 0.05,
        "At low load the fast group should be over-represented (uneven is fine); share=" + firstFastShare);
    // At full quota the distribution is essentially even -- every group is used to its ceiling, none breaches.
    Assert.assertTrue(
        lastFastShare < evenShare + 0.02,
        "At full quota routing should be essentially even so all groups are used; fast share=" + lastFastShare);
    Assert.assertTrue(
        firstFastShare - lastFastShare > 0.08,
        "Routing should measurably even out as load approaches quota; low=" + firstFastShare + " full="
            + lastFastShare);
  }

  /**
   * Baseline reproduction of the group-routing skew this strategy fixes, plus the fix, on identical input.
   *
   * <p>Both the existing {@link HelixGroupLeastLoadedStrategy} and the new
   * {@link HelixGroupWeightedLeastLoadedStrategy} are driven through the same harness using a <em>real
   * per-group latency vector observed in production</em> (only a 1.18x spread between the fastest and slowest
   * group, and every group comfortably under a typical 100ms retry budget).
   *
   * <p>The per-group latency each strategy reads carries fresh Gaussian jitter on every request, modelling the
   * real per-request latency variation a router sees (a static vector would make one group deterministically
   * fastest forever and collapse the old tie-break to a degenerate 100% winner-take-all that is never observed
   * in production). The old strategy breaks its (constant, low-in-flight) ties with the momentarily lowest
   * latency, so it over-concentrates traffic on whichever group is momentarily fastest -- reproducing the ~40%
   * production skew from a sub-millisecond edge -- and squeezes read-quota headroom on the others (the 429s).
   * The new strategy's latency gate stays ~1 for every group while all are healthy, so those groups are
   * interchangeable and load spreads evenly, holding each group near its fair share and preserving quota.
   */
  @Test
  public void testBaselineOldStrategyOverConcentratesVersusNewStrategy() {
    int groupCount = 5;
    // Real per-group average latency (ms) observed in production; group 4 is fastest.
    double[] measured = new double[] { 22.88, 22.13, 23.84, 20.94, 20.25 };
    double jitterStdDevMs = 3.0;
    int budgetMs = 100; // typical long-tail retry threshold; all groups are healthy relative to it.
    int fastGroup = argMin(measured);
    double evenShare = 1.0 / groupCount;
    int requestCount = 50000;

    // --- Baseline: existing least-loaded strategy (ignores the budget) on the jittered latency vector. ---
    HelixGroupStats oldStats = statsWithJitteredLatencies(measured, new Random(SEED), jitterStdDevMs);
    HelixGroupLeastLoadedStrategy oldStrategy =
        new HelixGroupLeastLoadedStrategy(mockTimeoutProcessor(), TIMEOUT_MS, oldStats);
    int[] oldRouted = routeAndFinish(oldStrategy, measured, groupCount, 0, requestCount, budgetMs);

    // --- Fix: new weighted strategy on the identical (independently seeded) jittered latency vector. ---
    HelixGroupStats newStats = statsWithJitteredLatencies(measured, new Random(SEED + 1), jitterStdDevMs);
    HelixGroupWeightedLeastLoadedStrategy newStrategy = weightedStrategy(newStats, new Random(SEED));
    int[] newRouted = routeAndFinish(newStrategy, measured, groupCount, 0, requestCount, budgetMs);

    double oldFastShare = oldRouted[fastGroup] / (double) requestCount;
    double newFastShare = newRouted[fastGroup] / (double) requestCount;

    LOGGER.info(
        "Baseline reproduction on production-observed latency vector {} (1.18x spread, {}ms jitter, {}ms budget):",
        Arrays.toString(measured),
        jitterStdDevMs,
        budgetMs);
    LOGGER.info(
        "  OLD (least-loaded)  routed={}  fast group {} share={}%",
        Arrays.toString(oldRouted),
        fastGroup,
        String.format("%.1f", 100 * oldFastShare));
    LOGGER.info(
        "  NEW (weighted)      routed={}  fast group {} share={}%",
        Arrays.toString(newRouted),
        fastGroup,
        String.format("%.1f", 100 * newFastShare));

    // The baseline exhibits the undesirable over-concentration: despite only a 1.18x latency spread and every
    // group being healthy, the old strategy pushes the fastest group's share far above its ~20% fair share.
    Assert.assertEquals(argMax(oldRouted), fastGroup, "Baseline should concentrate on the fastest group");
    Assert.assertTrue(
        oldFastShare > evenShare * 1.5,
        "Baseline should over-concentrate on the fastest group (>1.5x fair share); share=" + oldFastShare);

    // The fix removes that over-concentration: with every group healthy relative to the budget the latency
    // gate is ~1 for all, so load spreads evenly -- the fastest group stays near its fair share, well below
    // the baseline, and no group is starved.
    Assert.assertTrue(
        newFastShare < oldFastShare - 0.1,
        "New strategy must materially reduce the over-concentration; old=" + oldFastShare + " new=" + newFastShare);
    Assert.assertTrue(
        newFastShare < evenShare * 1.25,
        "New strategy should keep the fastest group near an even share while healthy; share=" + newFastShare);
    for (int g = 0; g < groupCount; g++) {
      Assert.assertTrue(
          newRouted[g] > 0.10 * requestCount,
          "No group should be starved under the new strategy; routed=" + Arrays.toString(newRouted));
    }
  }
}
