package com.linkedin.venice.router.api.routing.helix;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.alpini.base.concurrency.TimeoutProcessor;
import com.linkedin.venice.exceptions.VeniceException;
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
 * requests through the strategy and snapshot how many queries were routed to each group as the per-group read
 * capacity (read-quota allocation) and the aggregate utilization change.
 *
 * <p>The strategy interpolates each group's routed share between an even split and a capacity-proportional
 * split as aggregate utilization {@code u = sum(used) / sum(capacity)} rises:
 * {@code share(g) = (1 - u^m) / G + u^m * capacity(g) / sum(capacity)}. Capacity and usage are injected through
 * simple array-backed providers so each scenario can move {@code u} deterministically, and randomness in the
 * weighted draw is made deterministic by injecting a seeded {@link Random}, so the snapshots are reproducible.
 */
public class TestHelixGroupWeightedLeastLoadedStrategy {
  private static final Logger LOGGER = LogManager.getLogger(TestHelixGroupWeightedLeastLoadedStrategy.class);
  private static final long TIMEOUT_MS = 10000;
  private static final long SEED = 42;

  private static TimeoutProcessor mockTimeoutProcessor() {
    TimeoutProcessor timeoutProcessor = mock(TimeoutProcessor.class);
    doReturn(mock(TimeoutProcessor.TimeoutFuture.class)).when(timeoutProcessor).schedule(any(), anyLong(), any());
    return timeoutProcessor;
  }

  /** A mocked HelixGroupStats whose per-group average latency is backed by a mutable array (legacy baseline). */
  private static HelixGroupStats statsWithLatencies(double[] latencies) {
    HelixGroupStats stats = mock(HelixGroupStats.class);
    when(stats.getGroupResponseWaitingTimeAvg(anyInt()))
        .thenAnswer(invocation -> latencies[(int) invocation.getArgument(0)]);
    return stats;
  }

  /**
   * A mocked HelixGroupStats that returns the per-group base latency plus fresh Gaussian jitter on every read,
   * modelling the per-request latency variation a real router observes (used only by the legacy-baseline
   * comparison, since the new strategy does not read latency). Draws are clamped to a small positive floor.
   */
  private static HelixGroupStats statsWithJitteredLatencies(double[] baseLatencies, Random jitter, double stdDevMs) {
    HelixGroupStats stats = mock(HelixGroupStats.class);
    when(stats.getGroupResponseWaitingTimeAvg(anyInt())).thenAnswer(invocation -> {
      int group = invocation.getArgument(0);
      return Math.max(0.1, baseLatencies[group] + jitter.nextGaussian() * stdDevMs);
    });
    return stats;
  }

  /**
   * Build a weighted strategy with array-backed capacity/usage providers and an injected seeded random. The
   * arrays are read live, so a scenario can mutate utilization between batches and the strategy will observe it.
   */
  private static HelixGroupWeightedLeastLoadedStrategy weightedStrategy(
      double[] capacity,
      double[] usage,
      double interpolationExponent,
      Random random) {
    IntToDoubleFunction capacityProvider = groupId -> capacity[groupId];
    IntToDoubleFunction usageProvider = groupId -> usage[groupId];
    return new HelixGroupWeightedLeastLoadedStrategy(
        mockTimeoutProcessor(),
        TIMEOUT_MS,
        mock(HelixGroupStats.class),
        capacityProvider,
        usageProvider,
        interpolationExponent,
        random::nextDouble);
  }

  /** Route {@code requestCount} requests, finishing each immediately so in-flight stays ~0. */
  private static int[] routeAndFinish(
      HelixGroupSelectionStrategy strategy,
      int groupCount,
      long startRequestId,
      int requestCount) {
    int[] routed = new int[groupCount];
    for (int i = 0; i < requestCount; i++) {
      long requestId = startRequestId + i;
      int group = strategy.selectGroup(requestId, groupCount);
      routed[group]++;
      strategy.finishRequest(requestId, group, 1.0);
    }
    return routed;
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
   * Headline behaviour: with plenty of aggregate headroom the strategy routes evenly even though the groups
   * have very different read-capacity allocations. Spreading evenly while there is headroom keeps every group's
   * read-quota consumption low and avoids the over-concentration that drives a single group to its 429 ceiling.
   */
  @Test
  public void testEvenWhenUtilizationLowDespiteCapacityDifference() {
    int groupCount = 3;
    // Group 2 is "stronger" (2x the read-capacity allocation of the others), but utilization is low.
    double[] capacity = { 10000, 10000, 20000 };
    double[] usage = { 2000, 2000, 4000 }; // aggregate u = 8000/40000 = 0.20
    double evenShare = 1.0 / groupCount;

    int[] routed = routeAndFinish(
        weightedStrategy(
            capacity,
            usage,
            HelixGroupWeightedLeastLoadedStrategy.DEFAULT_INTERPOLATION_EXPONENT,
            new Random(SEED)),
        groupCount,
        0,
        60000);

    LOGGER.info(
        "Low utilization (u=0.20) with capacity {} -> routed {} (should be ~even {})",
        Arrays.toString(capacity),
        Arrays.toString(routed),
        String.format("%.1f%%", 100 * evenShare));

    for (int g = 0; g < groupCount; g++) {
      double share = routed[g] / 60000.0;
      Assert.assertTrue(
          Math.abs(share - evenShare) < 0.02,
          "At low utilization every group should get ~even share despite capacity gap; group " + g + " share=" + share);
    }
  }

  /**
   * The complement of the previous test: as aggregate utilization approaches 1, routing converges to the
   * capacity-proportional split, so the stronger (higher read-quota) group absorbs proportionally more traffic.
   * This is what keeps every group at (rather than past) its ceiling when the cluster is genuinely full.
   */
  @Test
  public void testSkewsToStrongGroupNearSaturation() {
    int groupCount = 3;
    double[] capacity = { 10000, 10000, 20000 }; // capacity shares 0.25 / 0.25 / 0.50
    double[] usage = { 9800, 9800, 19600 }; // aggregate u = 39200/40000 = 0.98
    double totalCapacity = 40000.0;

    int[] routed = routeAndFinish(
        weightedStrategy(
            capacity,
            usage,
            HelixGroupWeightedLeastLoadedStrategy.DEFAULT_INTERPOLATION_EXPONENT,
            new Random(SEED)),
        groupCount,
        0,
        60000);

    LOGGER.info(
        "High utilization (u=0.98) with capacity {} -> routed {} (should approach capacity shares 25/25/50)",
        Arrays.toString(capacity),
        Arrays.toString(routed));

    for (int g = 0; g < groupCount; g++) {
      double share = routed[g] / 60000.0;
      double capacityShare = capacity[g] / totalCapacity;
      Assert.assertTrue(
          Math.abs(share - capacityShare) < 0.03,
          "Near saturation each group's share should approach its capacity share; group " + g + " share=" + share
              + " capacityShare=" + capacityShare);
    }
    Assert.assertEquals(argMax(routed), 2, "The stronger group should absorb the most traffic near saturation");
  }

  /**
   * The design reproduction: replay Ali Poursamadi's five-stage capacity model. Three groups share a fixed
   * aggregate read quota; group 2 is the stronger member (~2x the read-capacity allocation of each weak
   * member). As the offered load climbs from a fraction of the aggregate quota up to the full quota, routing
   * must move from an even split (no group anywhere near its ceiling) to a capacity-proportional split (the
   * stronger member carrying its larger share), reproducing the modelled distribution at every stage.
   *
   * <p>Expected distributions from the model (per stage): even ~33/33/34 while there is headroom, sliding to
   * ~26/25/49 at full utilization. The stronger group's share must increase monotonically across the stages.
   */
  @Test
  public void testReproducesFiveStageCapacityModel() {
    int groupCount = 3;
    // Per-group read-capacity allocation (the "strength" of each member). Group 2 is ~2x the weak members.
    double[] capacity = { 10920, 10500, 20580 };
    double totalCapacity = 10920 + 10500 + 20580; // 42000
    // Offered aggregate load at each stage (sum of per-group consumed read capacity in the window).
    int[] stageLoads = { 10000, 20000, 30000, 38000, 42000 };
    int requestsPerStage = 40000;
    double m = HelixGroupWeightedLeastLoadedStrategy.DEFAULT_INTERPOLATION_EXPONENT;

    double[] usage = new double[groupCount];
    double previousStrongShare = -1.0;

    LOGGER.info(
        "Five-stage capacity model (capacity {}, total quota {}). stage | u | routed | shares%% | strong g2 share",
        Arrays.toString(capacity),
        (int) totalCapacity);

    for (int stage = 0; stage < stageLoads.length; stage++) {
      // Only the aggregate matters for utilization; split the stage load arbitrarily across groups.
      double per = stageLoads[stage] / (double) groupCount;
      Arrays.fill(usage, per);
      double u = stageLoads[stage] / totalCapacity;

      int[] routed = routeAndFinish(
          weightedStrategy(capacity, usage, m, new Random(SEED)),
          groupCount,
          (long) stage * requestsPerStage,
          requestsPerStage);

      double[] shares = new double[groupCount];
      for (int g = 0; g < groupCount; g++) {
        shares[g] = routed[g] / (double) requestsPerStage;
      }
      double strongShare = shares[2];

      LOGGER.info(
          String.format(
              "  %2d   | %.3f | %-22s | [%.1f, %.1f, %.1f] | %.1f%%",
              stage,
              u,
              Arrays.toString(routed),
              100 * shares[0],
              100 * shares[1],
              100 * shares[2],
              100 * strongShare));

      // Compare against the analytic model share(g) = (1 - u^m)/G + u^m * capacity(g)/total.
      double uPow = Math.pow(u, m);
      for (int g = 0; g < groupCount; g++) {
        double expected = (1.0 - uPow) / groupCount + uPow * capacity[g] / totalCapacity;
        Assert.assertTrue(
            Math.abs(shares[g] - expected) < 0.02,
            "Stage " + stage + " group " + g + " share=" + shares[g] + " should match model " + expected);
      }
      // The stronger group's share must not shrink as load grows.
      if (previousStrongShare >= 0) {
        Assert.assertTrue(
            strongShare >= previousStrongShare - 0.005,
            "Stronger group share should grow (not shrink) as load rises; was " + previousStrongShare + " now "
                + strongShare);
      }
      previousStrongShare = strongShare;
    }

    // Bookend checks anchored to the model: even at the first stage, clearly skewed to the strong member at full quota.
    double[] usageLow = { 3333, 3333, 3334 };
    int[] low =
        routeAndFinish(weightedStrategy(capacity, usageLow, m, new Random(SEED)), groupCount, 0, requestsPerStage);
    Assert.assertTrue(
        Math.abs(low[2] / (double) requestsPerStage - 1.0 / groupCount) < 0.02,
        "First stage should be ~even for the strong group; routed=" + Arrays.toString(low));

    double[] usageFull = { 14000, 14000, 14000 };
    int[] full =
        routeAndFinish(weightedStrategy(capacity, usageFull, m, new Random(SEED)), groupCount, 0, requestsPerStage);
    double fullStrongShare = full[2] / (double) requestsPerStage;
    Assert.assertTrue(
        Math.abs(fullStrongShare - capacity[2] / totalCapacity) < 0.02,
        "At full quota the strong group should carry its capacity share (~49%); share=" + fullStrongShare);
  }

  /**
   * When no capacity signal is wired in ({@link HelixGroupWeightedLeastLoadedStrategy#UNIFORM_CAPACITY}), every
   * group is treated as equal, so the capacity-proportional term also reduces to an even split and routing
   * stays even at <em>every</em> utilization level -- including a high-usage batch. This is the safe default
   * before per-group read-quota allocation is plumbed through.
   */
  @Test
  public void testUniformCapacityStaysEvenAtAllLoads() {
    int groupCount = 4;
    double evenShare = 1.0 / groupCount;
    // Uniform capacity, but heavy (and uneven) usage -> aggregate u is high, yet routing must stay even.
    double[] capacity = { 1, 1, 1, 1 };
    double[] usage = { 900, 950, 990, 800 };

    int[] routed = routeAndFinish(
        weightedStrategy(
            capacity,
            usage,
            HelixGroupWeightedLeastLoadedStrategy.DEFAULT_INTERPOLATION_EXPONENT,
            new Random(SEED)),
        groupCount,
        0,
        60000);

    LOGGER.info("Uniform capacity, high usage -> routed {} (should be ~even)", Arrays.toString(routed));
    for (int g = 0; g < groupCount; g++) {
      double share = routed[g] / 60000.0;
      Assert.assertTrue(
          Math.abs(share - evenShare) < 0.02,
          "Uniform capacity must stay even at all loads; group " + g + " share=" + share);
    }
  }

  /**
   * Degenerate input: when every group reports zero capacity the total capacity is zero and utilization is
   * undefined. The strategy must not divide by zero or drop the request -- it falls back to an even split and
   * still routes every request to a valid group.
   */
  @Test
  public void testZeroCapacityFallsBackToEven() {
    int groupCount = 3;
    double[] capacity = { 0, 0, 0 };
    double[] usage = { 100, 100, 100 };
    int requestCount = 30000;

    int[] routed = routeAndFinish(
        weightedStrategy(
            capacity,
            usage,
            HelixGroupWeightedLeastLoadedStrategy.DEFAULT_INTERPOLATION_EXPONENT,
            new Random(SEED)),
        groupCount,
        0,
        requestCount);

    int total = 0;
    for (int g = 0; g < groupCount; g++) {
      total += routed[g];
      Assert.assertTrue(routed[g] > 0, "Every group should still receive traffic; routed=" + Arrays.toString(routed));
    }
    Assert
        .assertEquals(total, requestCount, "Every request must be routed somewhere; routed=" + Arrays.toString(routed));
    for (int g = 0; g < groupCount; g++) {
      Assert.assertTrue(
          Math.abs(routed[g] / (double) requestCount - 1.0 / groupCount) < 0.02,
          "Zero-capacity fallback should be even; group " + g + " routed=" + routed[g]);
    }
  }

  /** Boundary: a single group is always selected, and every request is accounted for. */
  @Test
  public void testSingleGroupAlwaysSelected() {
    double[] capacity = { 5 };
    double[] usage = { 4 };
    int[] routed = routeAndFinish(
        weightedStrategy(
            capacity,
            usage,
            HelixGroupWeightedLeastLoadedStrategy.DEFAULT_INTERPOLATION_EXPONENT,
            new Random(SEED)),
        1,
        0,
        1000);
    Assert.assertEquals(routed[0], 1000, "The sole group must receive every request");
  }

  /** Failure path: an out-of-range group count is rejected. */
  @Test
  public void testInvalidGroupCountThrows() {
    HelixGroupWeightedLeastLoadedStrategy strategy = weightedStrategy(
        new double[] { 1 },
        new double[] { 0 },
        HelixGroupWeightedLeastLoadedStrategy.DEFAULT_INTERPOLATION_EXPONENT,
        new Random(SEED));
    Assert.assertThrows(VeniceException.class, () -> strategy.selectGroup(0, 0));
    Assert.assertThrows(
        VeniceException.class,
        () -> strategy.selectGroup(0, HelixGroupWeightedLeastLoadedStrategy.MAX_ALLOWED_GROUP + 1));
  }

  /** Failure path: selecting a group twice for the same request id is a programming error and must be rejected. */
  @Test
  public void testDuplicateRequestIdThrows() {
    HelixGroupWeightedLeastLoadedStrategy strategy = weightedStrategy(
        new double[] { 1, 1, 1 },
        new double[] { 0, 0, 0 },
        HelixGroupWeightedLeastLoadedStrategy.DEFAULT_INTERPOLATION_EXPONENT,
        new Random(SEED));
    strategy.selectGroup(7, 3);
    Assert.assertThrows(VeniceException.class, () -> strategy.selectGroup(7, 3));
  }

  /**
   * Baseline reproduction of the group-routing skew this strategy fixes, plus the fix, on identical input.
   *
   * <p>The existing {@link HelixGroupLeastLoadedStrategy} is driven with a <em>real per-group latency vector
   * observed in production</em> (only a 1.18x spread between the fastest and slowest group). Its lexicographic
   * (in-flight, latency) tie-break -- with the constant near-zero in-flight a single router sees -- breaks ties
   * on the momentarily lowest latency and over-concentrates traffic on whichever group is momentarily fastest,
   * reproducing the ~40% production skew from a sub-millisecond edge and squeezing read-quota headroom on the
   * others (the 429s).
   *
   * <p>The new strategy does not route on latency at all: with ample aggregate headroom (low utilization) it
   * spreads evenly across all groups, holding each near its fair share and preserving quota. The two strategies
   * are compared on the same harness to show the fix removes the over-concentration.
   */
  @Test
  public void testBaselineOldStrategyOverConcentratesVersusNewStrategy() {
    int groupCount = 5;
    // Real per-group average latency (ms) observed in production; group 4 is fastest.
    double[] measured = { 22.88, 22.13, 23.84, 20.94, 20.25 };
    double jitterStdDevMs = 3.0;
    int fastGroup = argMin(measured);
    double evenShare = 1.0 / groupCount;
    int requestCount = 50000;

    // --- Baseline: existing least-loaded strategy on the jittered production latency vector. ---
    HelixGroupStats oldStats = statsWithJitteredLatencies(measured, new Random(SEED), jitterStdDevMs);
    HelixGroupLeastLoadedStrategy oldStrategy =
        new HelixGroupLeastLoadedStrategy(mockTimeoutProcessor(), TIMEOUT_MS, oldStats);
    int[] oldRouted = routeAndFinish(oldStrategy, groupCount, 0, requestCount);

    // --- Fix: new weighted strategy with uniform capacity and ample headroom (low utilization) -> even. ---
    double[] capacity = { 1000, 1000, 1000, 1000, 1000 };
    double[] usage = { 100, 100, 100, 100, 100 }; // aggregate u = 500/5000 = 0.10 (ample headroom)
    HelixGroupWeightedLeastLoadedStrategy newStrategy = weightedStrategy(
        capacity,
        usage,
        HelixGroupWeightedLeastLoadedStrategy.DEFAULT_INTERPOLATION_EXPONENT,
        new Random(SEED));
    int[] newRouted = routeAndFinish(newStrategy, groupCount, 0, requestCount);

    double oldFastShare = oldRouted[fastGroup] / (double) requestCount;
    double newFastShare = newRouted[fastGroup] / (double) requestCount;

    LOGGER.info(
        "Baseline reproduction on production-observed latency vector {} (1.18x spread, {}ms jitter):",
        Arrays.toString(measured),
        jitterStdDevMs);
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

    // The fix removes that over-concentration: with ample headroom the new strategy spreads evenly, so the
    // (formerly hottest) group stays near its fair share and no group is starved.
    Assert.assertTrue(
        newFastShare < oldFastShare - 0.1,
        "New strategy must materially reduce the over-concentration; old=" + oldFastShare + " new=" + newFastShare);
    Assert.assertTrue(
        Math.abs(newFastShare - evenShare) < 0.02,
        "New strategy should keep every group near an even share while there is headroom; share=" + newFastShare);
    for (int g = 0; g < groupCount; g++) {
      Assert.assertTrue(
          newRouted[g] > 0.10 * requestCount,
          "No group should be starved under the new strategy; routed=" + Arrays.toString(newRouted));
    }
  }
}
