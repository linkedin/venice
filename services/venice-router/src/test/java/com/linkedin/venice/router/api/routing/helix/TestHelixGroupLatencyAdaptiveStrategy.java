package com.linkedin.venice.router.api.routing.helix;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyDouble;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.linkedin.alpini.base.concurrency.TimeoutProcessor;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.stats.routing.HelixGroupStats;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for {@link HelixGroupLatencyAdaptiveStrategy}: that measured latency drives the weighted draw, and the router's
 * per-request bookkeeping (failures, duplicate completions, timeouts). The selection math itself is covered by
 * {@code LatencyAdaptiveGroupSelectorTest}; the seeded {@link Random} keeps the draws reproducible.
 */
public class TestHelixGroupLatencyAdaptiveStrategy {
  private static final long TIMEOUT_MS = 10000;
  private static final long SEED = 42;

  private static TimeoutProcessor mockTimeoutProcessor() {
    TimeoutProcessor timeoutProcessor = mock(TimeoutProcessor.class);
    doReturn(mock(TimeoutProcessor.TimeoutFuture.class)).when(timeoutProcessor).schedule(any(), anyLong(), any());
    return timeoutProcessor;
  }

  /**
   * Build a weighted strategy that reads per-group latency from a live array with an injected seeded random. The
   * array is read live, so a scenario can mutate latency between batches and the strategy will observe it.
   */
  private static HelixGroupLatencyAdaptiveStrategy weightedStrategy(
      double[] latency,
      double evenUntilLatencyMs,
      double fullSkewAtLatencyMs,
      double interpolationExponent,
      Random random) {
    return new HelixGroupLatencyAdaptiveStrategy(
        mockTimeoutProcessor(),
        TIMEOUT_MS,
        mock(HelixGroupStats.class),
        groupId -> latency[groupId],
        evenUntilLatencyMs,
        fullSkewAtLatencyMs,
        interpolationExponent,
        random::nextDouble);
  }

  /** Convenience overload using the strategy's default stay-even and full-skew latency-ms thresholds. */
  private static HelixGroupLatencyAdaptiveStrategy weightedStrategy(
      double[] latency,
      double interpolationExponent,
      Random random) {
    return weightedStrategy(
        latency,
        HelixGroupLatencyAdaptiveStrategy.DEFAULT_EVEN_UNTIL_LATENCY_MS,
        HelixGroupLatencyAdaptiveStrategy.DEFAULT_FULL_SKEW_AT_LATENCY_MS,
        interpolationExponent,
        random);
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

  /** A latency-adaptive strategy over a caller-supplied (mock) stats so recording can be verified. */
  private static HelixGroupLatencyAdaptiveStrategy scalarStrategyOver(
      HelixGroupStats stats,
      TimeoutProcessor timeoutProcessor) {
    return new HelixGroupLatencyAdaptiveStrategy(
        timeoutProcessor,
        TIMEOUT_MS,
        stats,
        groupId -> 10.0,
        HelixGroupLatencyAdaptiveStrategy.DEFAULT_EVEN_UNTIL_LATENCY_MS,
        HelixGroupLatencyAdaptiveStrategy.DEFAULT_FULL_SKEW_AT_LATENCY_MS,
        1.0,
        new Random(SEED)::nextDouble);
  }

  /**
   * Once the slowest group's latency reaches the full-skew threshold, routing converges to the latency-proportional split, so the faster (lower-latency) group absorbs proportionally more
   * traffic. With latency {40, 40, 20} the slowest (40ms) is >= the 30ms full-skew default and the strengths are
   * {1/40, 1/40, 1/20}, i.e. shares {0.25, 0.25, 0.50}.
   */
  @Test
  public void testSkewsToFastGroupWhenLatencyDiverges() {
    int groupCount = 3;
    double[] latency = { 40.0, 40.0, 20.0 }; // slowest 40ms >= 30ms full-skew -> strength shares 0.25 / 0.25 / 0.50

    int[] routed = routeAndFinish(
        weightedStrategy(latency, HelixGroupLatencyAdaptiveStrategy.DEFAULT_INTERPOLATION_EXPONENT, new Random(SEED)),
        groupCount,
        0,
        60000);

    double[] expected = { 0.25, 0.25, 0.50 };
    for (int g = 0; g < groupCount; g++) {
      double share = routed[g] / 60000.0;
      Assert.assertTrue(
          Math.abs(share - expected[g]) < 0.03,
          "At full skew each group's share should approach its latency-derived share; group " + g + " share=" + share
              + " expected=" + expected[g]);
    }
  }

  /**
   * Edge case: a not-yet-measured group ({@code latency <= 0}) mixed with measured groups whose spread is wide
   * must be treated neutrally -- it is neither flooded (as a naive {@code 1/latency} with latency 0 would do)
   * nor starved. It should land near the even share while the measured groups skew by their latency.
   */
  @Test
  public void testNotYetMeasuredGroupTreatedNeutrally() {
    int groupCount = 3;
    // Groups 0 and 1 measured (one fast, one slow; slowest 60ms >= 30ms full-skew); group 2 has no data yet.
    double[] latency = { 20.0, 60.0, -1.0 };
    double evenShare = 1.0 / groupCount;

    int[] routed = routeAndFinish(
        weightedStrategy(latency, HelixGroupLatencyAdaptiveStrategy.DEFAULT_INTERPOLATION_EXPONENT, new Random(SEED)),
        groupCount,
        0,
        60000);

    double unmeasuredShare = routed[2] / 60000.0;
    Assert.assertTrue(
        routed[2] > 0.10 * 60000,
        "The unmeasured group must not be starved; routed=" + Arrays.toString(routed));
    Assert.assertTrue(
        Math.abs(unmeasuredShare - evenShare) < 0.12,
        "The unmeasured group should stay near the neutral/even share; share=" + unmeasuredShare);
    Assert.assertTrue(
        routed[0] > routed[1],
        "Among measured groups the faster one (0) should still out-draw the slower (1); routed="
            + Arrays.toString(routed));
  }

  /**
   * The probe floor keeps even a very slow group alive at full skew: it always retains at least
   * {@link HelixGroupLatencyAdaptiveStrategy#PROBE_FLOOR_FRACTION} of the even share, so the router keeps
   * observing its latency and the signal stays live and self-correcting.
   */
  @Test
  public void testProbeFloorKeepsSlowGroupAlive() {
    int groupCount = 3;
    // Group 1 is two orders of magnitude slower; without a floor its share would collapse toward zero at full skew.
    double[] latency = { 20.0, 4000.0, 20.0 };
    int requestCount = 60000;
    double evenShare = 1.0 / groupCount;
    double floor = HelixGroupLatencyAdaptiveStrategy.PROBE_FLOOR_FRACTION * evenShare;

    int[] routed = routeAndFinish(
        weightedStrategy(latency, HelixGroupLatencyAdaptiveStrategy.DEFAULT_INTERPOLATION_EXPONENT, new Random(SEED)),
        groupCount,
        0,
        requestCount);

    double slowShare = routed[1] / (double) requestCount;
    Assert.assertTrue(
        slowShare >= 0.5 * floor,
        "The slow group must retain at least ~the probe floor so it stays measured; share=" + slowShare + " floor="
            + floor);
    Assert.assertTrue(routed[1] > 0, "The slow group must never be fully starved; routed=" + Arrays.toString(routed));
  }

  /** Failure path: selecting a group twice for the same request id is a programming error and must be rejected. */
  @Test
  public void testDuplicateRequestIdThrows() {
    HelixGroupLatencyAdaptiveStrategy strategy = weightedStrategy(
        new double[] { 25.0, 25.0, 25.0 },
        HelixGroupLatencyAdaptiveStrategy.DEFAULT_INTERPOLATION_EXPONENT,
        new Random(SEED));
    strategy.selectGroup(7, 3);
    Assert.assertThrows(VeniceException.class, () -> strategy.selectGroup(7, 3));
  }

  @Test
  public void testFailedRequestDoesNotRecordLatencyButReleasesGroup() {
    HelixGroupStats stats = mock(HelixGroupStats.class);
    HelixGroupLatencyAdaptiveStrategy strategy = scalarStrategyOver(stats, mockTimeoutProcessor());
    int group = strategy.selectGroup(0L, 3);

    // An unhealthy (failed) response must not feed the latency signal, so a fast failure cannot make a group look fast.
    strategy.finishRequest(0L, group, 5, 123.0, false);
    verify(stats, never()).recordGroupResponseWaitingTime(anyInt(), anyDouble());

    // The group bookkeeping must still be released: re-selecting the same request id must not trip the "selected more
    // than once" guard, and a subsequent healthy request records exactly one sample.
    int reselected = strategy.selectGroup(0L, 3);
    strategy.finishRequest(0L, reselected, 5, 20.0, true);
    verify(stats, times(1)).recordGroupResponseWaitingTime(reselected, 20.0);
  }

  @Test
  public void testFinishRecordsLatencyAtMostOnce() {
    HelixGroupStats stats = mock(HelixGroupStats.class);
    HelixGroupLatencyAdaptiveStrategy strategy = scalarStrategyOver(stats, mockTimeoutProcessor());
    int group = strategy.selectGroup(0L, 3);

    strategy.finishRequest(0L, group, 5, 42.0, true);
    // A duplicate completion for the same request must not double-count the latency.
    strategy.finishRequest(0L, group, 5, 42.0, true);
    verify(stats, times(1)).recordGroupResponseWaitingTime(group, 42.0);
  }

  @Test
  public void testTimeoutPenalizesOnceThenLateFinishIsNoOp() {
    HelixGroupStats stats = mock(HelixGroupStats.class);
    TimeoutProcessor timeoutProcessor = mock(TimeoutProcessor.class);
    AtomicReference<Runnable> scheduled = new AtomicReference<>();
    doAnswer(invocation -> {
      scheduled.set(invocation.getArgument(0));
      return mock(TimeoutProcessor.TimeoutFuture.class);
    }).when(timeoutProcessor).schedule(any(), anyLong(), any());
    HelixGroupLatencyAdaptiveStrategy strategy = scalarStrategyOver(stats, timeoutProcessor);
    int group = strategy.selectGroup(0L, 3);

    // The leak-safeguard timeout fires: the stalled group is penalized once with the timeout latency.
    scheduled.get().run();
    verify(stats, times(1)).recordGroupResponseWaitingTime(group, (double) TIMEOUT_MS);

    // A late finish for the already-released request must not record a second sample.
    strategy.finishRequest(0L, group, 5, 7.0, true);
    verify(stats, times(1)).recordGroupResponseWaitingTime(anyInt(), anyDouble());
  }
}
