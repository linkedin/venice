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
 * requests through the strategy and take periodic snapshots of how many queries were routed to each group as
 * the per-group latency, in-flight load, and read-quota headroom change over time.
 *
 * <p>The per-group latency that the strategy reads is controlled directly through a mocked
 * {@link HelixGroupStats#getGroupResponseWaitingTimeAvg(int)} so each scenario can move the latency vector
 * deterministically. Randomness in the weighted draw is made deterministic by injecting a seeded
 * {@link Random}, so the snapshots are reproducible.
 */
public class TestHelixGroupWeightedLeastLoadedStrategy {
  private static final Logger LOGGER = LogManager.getLogger(TestHelixGroupWeightedLeastLoadedStrategy.class);
  private static final long TIMEOUT_MS = 10000;
  private static final long SEED = 42;

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
   * Route {@code requestCount} requests through the strategy, finishing each one immediately (so in-flight
   * stays ~0 and the routed share is driven purely by the latency/headroom weight). Returns the per-group
   * routed counts.
   */
  /**
   * Route {@code requestCount} requests through the strategy, finishing each one immediately (so in-flight
   * stays ~0 and the routed share is driven purely by the latency/headroom weight). Returns the per-group
   * routed counts.
   */
  private static int[] routeAndFinish(
      HelixGroupSelectionStrategy strategy,
      double[] latencies,
      int groupCount,
      long startRequestId,
      int requestCount) {
    int[] routed = new int[groupCount];
    for (int i = 0; i < requestCount; i++) {
      long requestId = startRequestId + i;
      int group = strategy.selectGroup(requestId, groupCount);
      routed[group]++;
      strategy.finishRequest(requestId, group, latencies[group]);
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

  /**
   * Baseline reproduction of the group-routing skew this strategy fixes, plus the fix, on identical input.
   *
   * <p>Both the existing {@link HelixGroupLeastLoadedStrategy} and the new
   * {@link HelixGroupWeightedLeastLoadedStrategy} are driven through the same harness using a <em>real
   * per-group latency vector observed in production</em> (only a 1.18x spread between the fastest and slowest
   * group). Little's Law says a 1.18x latency spread should give the fastest group only a ~22% share of a
   * 5-group cluster.
   *
   * <p>The per-group latency the strategies read carries fresh Gaussian jitter on every request, modelling the
   * real per-request latency variation a router sees (a static latency vector is unrealistic: it would make one
   * group deterministically fastest forever and collapse the old tiebreak to a degenerate 100% winner-take-all
   * that is never observed in production). With realistic jitter the <em>momentarily</em> fastest group changes
   * request to request, and the group with the lowest base latency simply wins that race most often.
   *
   * <p>This test runs in the tiebreak-isolated regime (each request finishes before the next is selected, so
   * the in-flight counters are always tied and the tiebreak alone decides every request) -- the low per-router
   * in-flight regime the skew lives in, where the least-loaded counters are almost always tied and the tiebreak
   * fires on nearly every request. The old strategy resolves every tie with the momentarily lowest latency, so
   * it over-concentrates traffic on the fastest group far beyond that group's ~22% Little's-Law share
   * (reproducing the ~40% production skew) and squeezes read-quota headroom on the others (the mechanism behind
   * the 429s). The new strategy replaces the tiebreak with a continuous {@code 1/latency} weight, so the fastest
   * group settles near its ~22% Little's-Law share and every group keeps a fair, latency-proportional slice.
   */
  @Test
  public void testBaselineOldStrategyOverConcentratesVersusNewStrategy() {
    int groupCount = 5;
    // Real per-group average latency (ms) observed in production; group 4 is fastest, group 2 slowest.
    double[] measured = new double[] { 22.88, 22.13, 23.84, 20.94, 20.25 };
    // Per-request latency jitter (ms). ~3ms 1-sigma is on the order of the inter-group spread, so the
    // momentarily fastest group varies request to request as it does in production.
    double jitterStdDevMs = 3.0;
    int fastGroup = argMin(measured);
    double evenShare = 1.0 / groupCount;
    int requestCount = 50000;

    // --- Baseline: existing least-loaded strategy on the jittered latency vector. ---
    HelixGroupStats oldStats = statsWithJitteredLatencies(measured, new Random(SEED), jitterStdDevMs);
    HelixGroupLeastLoadedStrategy oldStrategy =
        new HelixGroupLeastLoadedStrategy(mockTimeoutProcessor(), TIMEOUT_MS, oldStats);
    int[] oldRouted = routeAndFinish(oldStrategy, measured, groupCount, 0, requestCount);

    // --- Fix: new weighted strategy on the identical (independently seeded) jittered latency vector. ---
    HelixGroupStats newStats = statsWithJitteredLatencies(measured, new Random(SEED + 1), jitterStdDevMs);
    Random random = new Random(SEED);
    HelixGroupWeightedLeastLoadedStrategy newStrategy = new HelixGroupWeightedLeastLoadedStrategy(
        mockTimeoutProcessor(),
        TIMEOUT_MS,
        newStats,
        HelixGroupWeightedLeastLoadedStrategy.FULL_HEADROOM,
        HelixGroupWeightedLeastLoadedStrategy.DEFAULT_HEADROOM_EXPONENT,
        random::nextDouble);
    int[] newRouted = routeAndFinish(newStrategy, measured, groupCount, 0, requestCount);

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

    // The baseline exhibits the undesirable over-concentration: despite only a 1.18x latency spread, the old
    // strategy pushes the fastest group's share far above its ~22% fair share (reproducing the ~40% skew) --
    // but, unlike the static-latency case, it is a graded distribution, not a degenerate 100% winner-take-all.
    Assert.assertEquals(argMax(oldRouted), fastGroup, "Baseline should concentrate on the fastest group");
    Assert.assertTrue(
        oldFastShare > evenShare * 1.5,
        "Baseline should over-concentrate on the fastest group (>1.5x fair share); share=" + oldFastShare);
    Assert.assertTrue(
        oldFastShare < 0.6,
        "With realistic jitter the baseline should be a graded skew, not 100% winner-take-all; share=" + oldFastShare);
    for (int g = 0; g < groupCount; g++) {
      Assert.assertTrue(
          oldRouted[g] > 0,
          "With jitter every group should receive some traffic under the baseline; routed="
              + Arrays.toString(oldRouted));
    }

    // The fix removes that over-concentration: the fastest group settles near its (fair) Little's-Law share,
    // materially below the baseline, and no group is starved.
    Assert.assertTrue(
        newFastShare < oldFastShare - 0.1,
        "New strategy must materially reduce the over-concentration; old=" + oldFastShare + " new=" + newFastShare);
    Assert.assertTrue(
        newFastShare < evenShare * 1.3,
        "New strategy should keep the fastest group near an even/Little's-Law share; share=" + newFastShare);
    for (int g = 0; g < groupCount; g++) {
      Assert.assertTrue(
          newRouted[g] > 0.10 * requestCount,
          "No group should be starved under the new strategy; routed=" + Arrays.toString(newRouted));
    }
    // Routing under the new strategy is monotonic in base latency: the fastest group gets the most traffic, the
    // slowest the least.
    Assert.assertEquals(
        argMax(newRouted),
        fastGroup,
        "New strategy should still give the fastest group the largest (fair) share; routed="
            + Arrays.toString(newRouted));
    Assert.assertEquals(
        argMin(newRouted),
        argMax(measured),
        "New strategy should give the slowest group the smallest share; routed=" + Arrays.toString(newRouted));
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

  private static int argMax(double[] values) {
    int idx = 0;
    for (int i = 1; i < values.length; i++) {
      if (values[i] > values[idx]) {
        idx = i;
      }
    }
    return idx;
  }

  private static int argMin(int[] values) {
    int idx = 0;
    for (int i = 1; i < values.length; i++) {
      if (values[i] < values[idx]) {
        idx = i;
      }
    }
    return idx;
  }

  /**
   * Gradual-slowdown scenario: 5 groups start at equal latency, then 4 of them gradually get slower while group 0 stays
   * fast. We take a snapshot of the queries routed to each group at every step and assert the distribution
   * responds correctly over time:
   *
   * <ul>
   *   <li>At the start (all equal) traffic is spread roughly evenly across the 5 groups.</li>
   *   <li>As the other 4 groups slow down, the fast group's share grows monotonically snapshot over
   *       snapshot, and it ends holding a clear plurality.</li>
   *   <li>At every snapshot routing is monotonic in latency: no slower group ever receives more traffic than
   *       a faster group.</li>
   * </ul>
   */
  @Test
  public void testSnapshotsAsFourOfFiveGroupsGraduallySlowDown() {
    int groupCount = 5;
    double baseLatencyMs = 20.0;
    double[] latencies = new double[groupCount];
    Arrays.fill(latencies, baseLatencyMs);

    Random random = new Random(SEED);
    HelixGroupWeightedLeastLoadedStrategy strategy = new HelixGroupWeightedLeastLoadedStrategy(
        mockTimeoutProcessor(),
        TIMEOUT_MS,
        statsWithLatencies(latencies),
        HelixGroupWeightedLeastLoadedStrategy.FULL_HEADROOM,
        HelixGroupWeightedLeastLoadedStrategy.DEFAULT_HEADROOM_EXPONENT,
        random::nextDouble);

    int requestsPerSnapshot = 10000;
    long nextRequestId = 0;
    double previousFastGroupShare = -1.0;
    int snapshotIndex = 0;

    LOGGER.info("Snapshot | group latencies (ms)            | queries routed per group           | fast g0 share");

    // Step the 4 "other" groups from equal (20ms) up to 60ms; group 0 stays at 20ms the whole time.
    for (double otherLatencyMs = baseLatencyMs; otherLatencyMs <= 60.0; otherLatencyMs += 8.0) {
      for (int g = 1; g < groupCount; g++) {
        latencies[g] = otherLatencyMs;
      }

      int[] routed = routeAndFinish(strategy, latencies, groupCount, nextRequestId, requestsPerSnapshot);
      nextRequestId += requestsPerSnapshot;
      double fastGroupShare = routed[0] / (double) requestsPerSnapshot;

      LOGGER.info(
          String.format(
              "   %2d    | %-32s | %-34s | %5.1f%%",
              snapshotIndex,
              Arrays.toString(latencies),
              Arrays.toString(routed),
              100 * fastGroupShare));

      if (snapshotIndex == 0) {
        // All groups equal at the first snapshot: distribution should be close to even (20% each). No group
        // is "fastest" here, so we don't assert a winner.
        for (int g = 0; g < groupCount; g++) {
          double share = routed[g] / (double) requestsPerSnapshot;
          Assert.assertTrue(
              share > 0.15 && share < 0.25,
              "With equal latency each of 5 groups should get ~20%; group " + g + " got " + share);
        }
      } else {
        // Once the other groups are slower, routing must be monotonic in latency: the fast group (lowest
        // latency) gets the most traffic and no slower group exceeds it...
        Assert.assertEquals(
            argMax(routed),
            0,
            "Fast group 0 should receive the most traffic; routed=" + Arrays.toString(routed));
        for (int g = 1; g < groupCount; g++) {
          Assert.assertTrue(
              routed[0] >= routed[g],
              "No slower group may exceed the fast group; routed=" + Arrays.toString(routed));
        }
        // ...and the fast group's share must strictly grow snapshot over snapshot as the others slow down.
        Assert.assertTrue(
            fastGroupShare > previousFastGroupShare,
            "Fast group share should grow as the other groups slow down; was " + previousFastGroupShare + " now "
                + fastGroupShare);
      }
      previousFastGroupShare = fastGroupShare;
      snapshotIndex++;
    }

    // By the final snapshot (others at 60ms vs group 0 at 20ms) the fast group holds a clear plurality.
    Assert.assertTrue(
        previousFastGroupShare > 0.35,
        "Fast group should end with a clear plurality of traffic; share=" + previousFastGroupShare);
  }

  /**
   * The weight folds in the in-flight counter, so a large backlog of un-finished (in-flight) requests on the
   * otherwise-fastest group must steer subsequent traffic away from it toward the groups with spare capacity.
   */
  @Test
  public void testInFlightLoadSteersAwayFromBackloggedGroup() {
    int groupCount = 3;
    double[] latencies = new double[] { 20.0, 20.0, 20.0 };
    Random random = new Random(SEED);
    HelixGroupWeightedLeastLoadedStrategy strategy = new HelixGroupWeightedLeastLoadedStrategy(
        mockTimeoutProcessor(),
        TIMEOUT_MS,
        statsWithLatencies(latencies),
        HelixGroupWeightedLeastLoadedStrategy.FULL_HEADROOM,
        HelixGroupWeightedLeastLoadedStrategy.DEFAULT_HEADROOM_EXPONENT,
        random::nextDouble);

    // Build a deterministic in-flight backlog on group 0 by briefly making it far faster so it absorbs a run
    // of un-finished selects, then restore equal latency for the measurement batch.
    latencies[0] = 1.0;
    latencies[1] = 100.0;
    latencies[2] = 100.0;
    long requestId = 0;
    for (int i = 0; i < 40; i++) {
      strategy.selectGroup(requestId++, groupCount);
    }
    Arrays.fill(latencies, 20.0);

    // Group 0 now carries a large persistent in-flight backlog while groups 1 and 2 are idle. With equal
    // latency, the new traffic should overwhelmingly avoid the backlogged group.
    int[] routed = routeAndFinish(strategy, latencies, groupCount, requestId, 9000);
    Assert.assertTrue(
        routed[1] + routed[2] > routed[0],
        "Backlogged group should shed new traffic to less-loaded groups; routed=" + Arrays.toString(routed));
    Assert.assertTrue(
        routed[0] < 0.2 * 9000,
        "Heavily backlogged group should get a small fraction of new traffic; routed=" + Arrays.toString(routed));
  }

  /**
   * The headroom factor sheds traffic away from a group approaching its read-quota ceiling even though it is
   * the lowest-latency group and its in-flight counter looks low (a quota-saturated group rejects fast, so it
   * never accumulates pending load). This is the read-quota-aware behavior the routing needs to avoid 429s
   * while other groups still have headroom.
   */
  @Test
  public void testQuotaHeadroomShedsNearLimitGroup() {
    int groupCount = 3;
    double[] latencies = new double[] { 12.0, 20.0, 20.0 };
    // Group 0 is the fastest but essentially out of read-quota headroom; the others have full headroom.
    double[] headroom = new double[] { 0.02, 1.0, 1.0 };
    IntToDoubleFunction headroomProvider = groupId -> headroom[groupId];

    Random random = new Random(SEED);
    HelixGroupWeightedLeastLoadedStrategy strategy = new HelixGroupWeightedLeastLoadedStrategy(
        mockTimeoutProcessor(),
        TIMEOUT_MS,
        statsWithLatencies(latencies),
        headroomProvider,
        2.0,
        random::nextDouble);

    int[] routed = routeAndFinish(strategy, latencies, groupCount, 0, 9000);

    // Despite being the fastest, group 0 is near its quota ceiling and should receive far less traffic than
    // the groups with headroom.
    Assert.assertTrue(
        routed[0] < routed[1] && routed[0] < routed[2],
        "Group near its read-quota ceiling should be shed despite lowest latency; routed=" + Arrays.toString(routed));
    Assert.assertTrue(
        routed[0] < 0.1 * 9000,
        "A group at ~2% headroom should get a small fraction of traffic; routed=" + Arrays.toString(routed));
  }
}
