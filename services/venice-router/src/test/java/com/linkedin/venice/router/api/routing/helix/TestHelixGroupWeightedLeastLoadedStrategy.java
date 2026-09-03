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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.DoubleSupplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Integration-style tests for {@link HelixGroupWeightedLeastLoadedStrategy} that drive a stream of mock
 * requests through the strategy and snapshot how many queries were routed to each group as the per-group
 * <em>measured latency</em> and the aggregate read-quota utilization change.
 *
 * <p>The strategy interpolates each group's routed share between an even split and a latency-proportional split
 * as aggregate utilization {@code u} rises:
 * {@code share(g) = (1 - u^m) / G + u^m * strength(g) / sum(strength)} where {@code strength(g) = 1 / latency(g)}.
 * There is no configured per-group capacity: the "stronger" group is simply the one whose measured latency is
 * lower. Latency and utilization are injected through simple providers so each scenario can move them
 * deterministically, and randomness in the weighted draw is made deterministic by injecting a seeded
 * {@link Random}, so the snapshots are reproducible.
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

  /**
   * A mocked HelixGroupStats that returns the per-group base latency plus fresh Gaussian jitter on every read,
   * modelling the per-request latency variation a real router observes (used by the legacy-baseline
   * comparison). Draws are clamped to a small positive floor.
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
   * Build a weighted strategy that reads per-group latency from a live array and aggregate utilization from a
   * supplier, with an injected seeded random. The array and supplier are read live, so a scenario can mutate
   * latency or utilization between batches and the strategy will observe it.
   */
  private static HelixGroupWeightedLeastLoadedStrategy weightedStrategy(
      double[] latency,
      DoubleSupplier utilization,
      double evenUntilUtilization,
      double fullSkewAtUtilization,
      double interpolationExponent,
      Random random) {
    return new HelixGroupWeightedLeastLoadedStrategy(
        mockTimeoutProcessor(),
        TIMEOUT_MS,
        mock(HelixGroupStats.class),
        groupId -> latency[groupId],
        utilization,
        evenUntilUtilization,
        fullSkewAtUtilization,
        interpolationExponent,
        random::nextDouble);
  }

  /** Convenience overload for scenarios with a constant aggregate utilization and explicit knobs. */
  private static HelixGroupWeightedLeastLoadedStrategy weightedStrategy(
      double[] latency,
      double utilization,
      double evenUntilUtilization,
      double fullSkewAtUtilization,
      double interpolationExponent,
      Random random) {
    return weightedStrategy(
        latency,
        () -> utilization,
        evenUntilUtilization,
        fullSkewAtUtilization,
        interpolationExponent,
        random);
  }

  /** Convenience overload using the strategy's default stay-even and full-skew thresholds. */
  private static HelixGroupWeightedLeastLoadedStrategy weightedStrategy(
      double[] latency,
      DoubleSupplier utilization,
      double interpolationExponent,
      Random random) {
    return weightedStrategy(
        latency,
        utilization,
        HelixGroupWeightedLeastLoadedStrategy.DEFAULT_EVEN_UNTIL_UTILIZATION,
        HelixGroupWeightedLeastLoadedStrategy.DEFAULT_FULL_SKEW_AT_UTILIZATION,
        interpolationExponent,
        random);
  }

  /** Convenience overload using the strategy's default thresholds and a constant aggregate utilization. */
  private static HelixGroupWeightedLeastLoadedStrategy weightedStrategy(
      double[] latency,
      double utilization,
      double interpolationExponent,
      Random random) {
    return weightedStrategy(latency, () -> utilization, interpolationExponent, random);
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

  private static final double LATENCY_BASE_MS = 20.0;
  private static final double LATENCY_LOAD_FACTOR = 4.0;
  private static final double LATENCY_LOAD_EXPONENT = 3.0;
  // base * (1 + factor) -- the modeled latency at full per-group utilization; treated as the SLO ceiling.
  private static final double LATENCY_SLO_MS = LATENCY_BASE_MS * (1.0 + LATENCY_LOAD_FACTOR);

  /**
   * The environment's true latency for a group as a function of how loaded it is, given a hidden per-group
   * serving capacity the router never sees: latency sits near the base while the group has headroom and rises
   * convexly as its per-group utilization {@code rho = served / capacity} climbs. This is the physical signal a
   * real router would <em>measure</em> (via {@link HelixGroupStats#getGroupResponseWaitingTimeAvg}); the
   * strategy infers each group's strength from it. Left uncapped so an overloaded group ({@code rho > 1}) is
   * strongly penalised, which is what makes the closed loop self-correct.
   */
  private static double environmentLatencyMs(double served, double capacity) {
    double rho = capacity > 0 ? served / capacity : Double.MAX_VALUE;
    return LATENCY_BASE_MS * (1.0 + LATENCY_LOAD_FACTOR * Math.pow(rho, LATENCY_LOAD_EXPONENT));
  }

  /**
   * The analytic realized share the strategy targets for the given measured latencies and utilization, matching
   * {@link HelixGroupWeightedLeastLoadedStrategy}'s math (strength = 1/latency, interpolation, probe floor,
   * reservoir normalisation). Used to (a) assert the strategy's actual routed distribution matches its model and
   * (b) find the environment's fixed point in the closed-loop test.
   */
  private static double[] analyticShares(double[] latency, double u, double m) {
    return analyticShares(
        latency,
        u,
        HelixGroupWeightedLeastLoadedStrategy.DEFAULT_EVEN_UNTIL_UTILIZATION,
        HelixGroupWeightedLeastLoadedStrategy.DEFAULT_FULL_SKEW_AT_UTILIZATION,
        m);
  }

  private static double[] analyticShares(double[] latency, double u, double evenUntil, double fullSkew, double m) {
    int groupCount = latency.length;
    double even = 1.0 / groupCount;
    double floor = HelixGroupWeightedLeastLoadedStrategy.PROBE_FLOOR_FRACTION * even;
    double skew;
    if (u <= evenUntil) {
      skew = 0.0;
    } else if (u >= fullSkew) {
      skew = 1.0;
    } else {
      double position = (u - evenUntil) / (fullSkew - evenUntil);
      skew = m == 1.0 ? position : Math.pow(position, m);
    }

    double sumMeasured = 0.0;
    int measured = 0;
    for (int g = 0; g < groupCount; g++) {
      if (latency[g] > 0) {
        sumMeasured += 1.0 / Math.max(latency[g], HelixGroupWeightedLeastLoadedStrategy.MIN_LATENCY_MS);
        measured++;
      }
    }
    double neutral = measured > 0 ? sumMeasured / measured : 1.0;

    double[] strength = new double[groupCount];
    double totalStrength = 0.0;
    for (int g = 0; g < groupCount; g++) {
      strength[g] =
          latency[g] > 0 ? 1.0 / Math.max(latency[g], HelixGroupWeightedLeastLoadedStrategy.MIN_LATENCY_MS) : neutral;
      totalStrength += strength[g];
    }

    double[] shares = new double[groupCount];
    double sum = 0.0;
    for (int g = 0; g < groupCount; g++) {
      double strengthShare = totalStrength > 0 ? strength[g] / totalStrength : even;
      shares[g] = Math.max((1.0 - skew) * even + skew * strengthShare, floor);
      sum += shares[g];
    }
    for (int g = 0; g < groupCount; g++) {
      shares[g] /= sum; // reservoir realizes P(g) = share(g) / sum(share)
    }
    return shares;
  }

  /**
   * Headline behaviour: with plenty of aggregate headroom the strategy routes evenly even though one group is
   * measurably faster. Spreading evenly while there is headroom keeps every group's read-quota consumption low
   * and avoids the over-concentration that drives a single group to its 429 ceiling.
   */
  @Test
  public void testEvenWhenUtilizationLowDespiteLatencyDifference() {
    int groupCount = 3;
    // Group 2 is measurably faster (half the latency of the others), but utilization is low.
    double[] latency = { 40.0, 40.0, 20.0 };
    double evenShare = 1.0 / groupCount;

    int[] routed = routeAndFinish(
        weightedStrategy(
            latency,
            0.20,
            HelixGroupWeightedLeastLoadedStrategy.DEFAULT_INTERPOLATION_EXPONENT,
            new Random(SEED)),
        groupCount,
        0,
        60000);

    LOGGER.info(
        "Low utilization (u=0.20) with latency {} -> routed {} (should be ~even {})",
        Arrays.toString(latency),
        Arrays.toString(routed),
        String.format("%.1f%%", 100 * evenShare));

    for (int g = 0; g < groupCount; g++) {
      double share = routed[g] / 60000.0;
      Assert.assertTrue(
          Math.abs(share - evenShare) < 0.02,
          "At low utilization every group should get ~even share despite the latency gap; group " + g + " share="
              + share);
    }
  }

  /**
   * The complement of the previous test: as aggregate utilization approaches 1, routing converges to the
   * latency-proportional split, so the faster (lower-latency) group absorbs proportionally more traffic. With
   * latency {40, 40, 20} the strengths are {1/40, 1/40, 1/20}, i.e. shares {0.25, 0.25, 0.50}.
   */
  @Test
  public void testSkewsToFastGroupNearSaturation() {
    int groupCount = 3;
    double[] latency = { 40.0, 40.0, 20.0 }; // strength shares 0.25 / 0.25 / 0.50
    int fastGroup = argMin(latency);

    int[] routed = routeAndFinish(
        weightedStrategy(
            latency,
            0.98,
            HelixGroupWeightedLeastLoadedStrategy.DEFAULT_INTERPOLATION_EXPONENT,
            new Random(SEED)),
        groupCount,
        0,
        60000);

    LOGGER.info(
        "High utilization (u=0.98) with latency {} -> routed {} (should approach strength shares 25/25/50)",
        Arrays.toString(latency),
        Arrays.toString(routed));

    double[] expected =
        analyticShares(latency, 0.98, HelixGroupWeightedLeastLoadedStrategy.DEFAULT_INTERPOLATION_EXPONENT);
    for (int g = 0; g < groupCount; g++) {
      double share = routed[g] / 60000.0;
      Assert.assertTrue(
          Math.abs(share - expected[g]) < 0.03,
          "Near saturation each group's share should approach its latency-derived share; group " + g + " share=" + share
              + " expected=" + expected[g]);
    }
    Assert.assertEquals(argMax(routed), fastGroup, "The faster group should absorb the most traffic near saturation");
  }

  /**
   * The design reproduction: replay Ali Poursamadi's five-stage model with request rate (RPS) rising in tandem
   * with utilization, but driven by <em>measured latency</em> rather than a configured capacity. Three groups
   * are served; group 2 is consistently the faster member (~half the latency of each weak member). Each stage
   * offers a higher absolute load than the last (10k -> 20k -> 30k -> 38k -> 42k requests), so utilization
   * climbs from a fraction of the quota up to the full quota, and the model shifts from an even split (no group
   * anywhere near its ceiling) to a latency-proportional split (the faster member carrying its larger share).
   *
   * <p>The key health property this asserts is on <em>absolute</em> traffic, not just shares: because RPS rises
   * every stage, each group -- including the slower members -- must keep receiving <em>more</em> absolute
   * traffic as load grows (its share shrinks, but its throughput does not). Traffic is never taken away from the
   * slower machines; the faster member simply absorbs a growing <em>fraction</em> of the growing total.
   *
   * <p>Crucially, the only per-group signal fed in is measured latency. No capacity vector is provided -- the
   * router discovers which member is stronger purely from what it observes.
   */
  @Test
  public void testReproducesFiveStageLatencyModel() {
    double[] latency = { 40.0, 40.0, 20.0 };
    int fastGroup = argMin(latency);
    int groupCount = latency.length;
    double slack = 0.01 * STAGED_TOTAL_QUOTA;

    StagedRun run = runStagedScenario("Five-stage latency model - group 2 is 2x faster", latency);

    double previousFastShare = -1.0;
    int[] previousRouted = null;
    for (int stage = 0; stage < run.stageLoads.length; stage++) {
      int[] routed = run.routed[stage];
      int load = run.stageLoads[stage];
      double fastShare = routed[fastGroup] / (double) load;
      for (int g = 0; g < groupCount; g++) {
        Assert.assertTrue(
            routed[g] <= run.impliedCeiling[g] + slack,
            "Group " + g + " routed=" + routed[g] + " must not exceed its implied ceiling " + run.impliedCeiling[g]);
      }
      // The faster group's share must not shrink as load grows.
      if (previousFastShare >= 0) {
        Assert.assertTrue(
            fastShare >= previousFastShare - 0.005,
            "Faster group share should grow (not shrink) as load rises; was " + previousFastShare + " now "
                + fastShare);
      }
      // Ali's health property: as RPS rises every group keeps getting MORE absolute traffic.
      if (previousRouted != null) {
        for (int g = 0; g < groupCount; g++) {
          Assert.assertTrue(
              routed[g] >= previousRouted[g] - slack,
              "Group " + g + " absolute traffic must not drop as RPS rises; was " + previousRouted[g] + " now "
                  + routed[g]);
        }
      }
      previousFastShare = fastShare;
      previousRouted = routed;
    }
    // At the final stage every group is served at (not past) its implied ceiling -- the healthy saturation point,
    // and the served latencies converge instead of one weak group becoming the SLO outlier.
    for (int g = 0; g < groupCount; g++) {
      Assert.assertTrue(
          Math.abs(previousRouted[g] - run.impliedCeiling[g]) < slack,
          "At full quota group " + g + " should be served at its ceiling " + run.impliedCeiling[g] + "; routed="
              + previousRouted[g]);
    }
    double[] saturationLatency = run.servedLatency[run.stageLoads.length - 1];
    Assert.assertTrue(
        spread(saturationLatency) < 10.0,
        "At saturation group latencies should be balanced (no SLO outlier); latencies="
            + Arrays.toString(saturationLatency));
  }

  /**
   * Control scenario: when every host is identical latency-wise there is no "faster" group to skew toward, so
   * every group's inferred strength is equal and the strength term reduces to the even split at <em>every</em>
   * utilization. Routing therefore stays even and the request spread stays ~0 all the way up to full quota. This
   * is the common homogeneous-fault-zone case: the strategy is a safe no-op that behaves exactly like plain even
   * routing when there is no real latency signal to act on.
   */
  @Test
  public void testEvenHostsStayEvenAsRpsRises() {
    double[] latency = { 30.0, 30.0, 30.0 };
    int groupCount = latency.length;

    StagedRun run = runStagedScenario("Even hosts - identical latency (30/30/30)", latency);

    for (int stage = 0; stage < run.stageLoads.length; stage++) {
      int[] routed = run.routed[stage];
      int load = run.stageLoads[stage];
      double spreadPct = 100.0 * (maxOf(routed) - minOf(routed)) / load;
      Assert.assertTrue(
          spreadPct < 2.0,
          "Identical hosts must stay even at u=" + run.utilization[stage] + "; req spread=" + spreadPct + "%");
      for (int g = 0; g < groupCount; g++) {
        Assert.assertTrue(
            Math.abs(routed[g] / (double) load - 1.0 / groupCount) < 0.02,
            "Identical hosts: every group should stay near the even share; routed=" + Arrays.toString(routed));
      }
    }
  }

  /**
   * The single-slow-host scenario: two identical fast hosts plus one host at 2x latency. At low load routing is
   * even (the slow host is not yet a problem), but as RPS climbs toward the quota the slow host progressively
   * <em>sheds</em> share to the two fast hosts -- its absolute traffic still rises (never starved, never taken
   * below its floor), it simply carries a shrinking fraction of a growing total. The request spread grows with
   * load while the served latencies converge, so no host is pushed past its ceiling.
   */
  @Test
  public void testOneSlowHostShedsTrafficAsRpsRises() {
    double[] latency = { 30.0, 30.0, 60.0 };
    int slowGroup = 2;
    int groupCount = latency.length;

    StagedRun run = runStagedScenario("One slow host - group 2 is 2x slower (30/30/60)", latency);

    int lastStage = run.stageLoads.length - 1;
    int[] firstRouted = run.routed[0];
    int[] lastRouted = run.routed[lastStage];

    // Low load: even (the slow host is barely distinguished).
    double firstSpreadPct = 100.0 * (maxOf(firstRouted) - minOf(firstRouted)) / run.stageLoads[0];
    Assert.assertTrue(
        firstSpreadPct < 3.0,
        "At low load routing should still be near-even; spread=" + firstSpreadPct + "%");

    // High load: the slow host carries the least, the two fast hosts carry the most.
    Assert.assertTrue(
        lastRouted[slowGroup] < lastRouted[0] && lastRouted[slowGroup] < lastRouted[1],
        "At saturation the slow host should carry the least traffic; routed=" + Arrays.toString(lastRouted));

    // The slow host sheds share as load rises, but its absolute traffic still grows (never starved).
    double firstSlowShare = firstRouted[slowGroup] / (double) run.stageLoads[0];
    double lastSlowShare = lastRouted[slowGroup] / (double) run.stageLoads[lastStage];
    Assert.assertTrue(
        lastSlowShare < firstSlowShare - 0.05,
        "The slow host should shed share as RPS rises; was " + firstSlowShare + " now " + lastSlowShare);
    Assert.assertTrue(
        lastRouted[slowGroup] > firstRouted[slowGroup],
        "The slow host's absolute traffic should still rise with RPS; first=" + firstRouted[slowGroup] + " last="
            + lastRouted[slowGroup]);
    Assert.assertTrue(
        lastRouted[slowGroup] > 0.05 * run.stageLoads[lastStage],
        "The slow host must never be starved below its probe floor; routed=" + Arrays.toString(lastRouted));

    // Request spread widens with load; served latencies still converge (balanced saturation).
    Assert.assertTrue(
        (maxOf(lastRouted) - minOf(lastRouted)) > (maxOf(firstRouted) - minOf(firstRouted)),
        "Request spread should widen as RPS rises for a heterogeneous fleet");
    Assert.assertTrue(
        spread(run.servedLatency[lastStage]) < 10.0,
        "At saturation the served latencies should still converge; latencies="
            + Arrays.toString(run.servedLatency[lastStage]));
  }

  /**
   * The two controllable knobs in action, on the same one-slow-host fleet ({30, 30, 60}). Both knobs move
   * <em>where</em> on the utilization axis the strategy reacts, without touching the latency signal itself:
   *
   * <ul>
   *   <li><b>evenUntilUtilization (stay-even knob)</b> — the utilization up to which routing stays fully even.
   *       Lowering it (0.70 -> 0.50) makes the strategy shed traffic off the slow host <em>sooner</em> (at a
   *       lower utilization); raising it (0.70 -> 0.90) keeps the fleet even for longer.</li>
   *   <li><b>fullSkewAtUtilization (full-skew knob)</b> — the utilization at which routing reaches its maximum
   *       latency-proportional split. Lowering it (1.00 -> 0.85) reaches full protection <em>before</em>
   *       saturation, so the slow host is at its floor-bounded minimum earlier.</li>
   * </ul>
   *
   * Each configuration renders its own staged table so the effect of moving a knob is directly visible.
   */
  @Test
  public void testKnobsControlEvenBandAndFullSkewOnset() {
    double[] slowHost = { 30.0, 30.0, 60.0 };
    int slow = 2;
    int midStage = 2; // u = 30000 / 42000 = 0.714
    int highStage = 3; // u = 38000 / 42000 = 0.905
    double linear = 1.0;

    StagedRun defaults =
        runStagedScenario("Knobs A: even<=0.70, full-skew@1.00 (default)", slowHost, 0.70, 1.00, linear);
    StagedRun early =
        runStagedScenario("Knobs B: even<=0.50, full-skew@1.00 (reacts sooner)", slowHost, 0.50, 1.00, linear);
    StagedRun late =
        runStagedScenario("Knobs C: even<=0.90, full-skew@1.00 (stays even longer)", slowHost, 0.90, 1.00, linear);
    StagedRun earlyFull = runStagedScenario(
        "Knobs D: even<=0.50, full-skew@0.85 (max skew before saturation)",
        slowHost,
        0.50,
        0.85,
        linear);

    double defaultSlowMid = defaults.routed[midStage][slow] / (double) defaults.stageLoads[midStage];
    double earlySlowMid = early.routed[midStage][slow] / (double) early.stageLoads[midStage];
    double lateSlowMid = late.routed[midStage][slow] / (double) late.stageLoads[midStage];

    // Stay-even knob at 0.90: at u=0.71 (below the knob) the slow host is still routed ~evenly.
    Assert.assertEquals(
        lateSlowMid,
        1.0 / 3.0,
        0.02,
        "with stay-even=0.90 the slow host stays ~even at u=0.71; share=" + lateSlowMid);
    // Lowering the stay-even knob to 0.50 sheds slow-host traffic sooner than the 0.70 default at the same u.
    Assert.assertTrue(
        earlySlowMid < defaultSlowMid - 0.02,
        "lowering stay-even 0.70->0.50 sheds slow-host traffic sooner at u=0.71; early=" + earlySlowMid + " default="
            + defaultSlowMid);
    // And the default already sheds a little at u=0.71 (just past its 0.70 knob), so it sits below fully-even.
    Assert.assertTrue(
        defaultSlowMid < 1.0 / 3.0 + 0.01,
        "the default knob has just started reacting at u=0.71; share=" + defaultSlowMid);

    // Full-skew knob at 0.85: by u=0.905 the slow host is already at full skew (its floor-bounded minimum),
    // below where the default (full skew only at u=1.0) has it at the same utilization.
    double earlyFullSlowHigh = earlyFull.routed[highStage][slow] / (double) earlyFull.stageLoads[highStage];
    double defaultSlowHigh = defaults.routed[highStage][slow] / (double) defaults.stageLoads[highStage];
    Assert.assertTrue(
        earlyFullSlowHigh < defaultSlowHigh - 0.01,
        "reaching full skew at 0.85 sheds more slow-host traffic by u=0.905 than the 1.0 default; early="
            + earlyFullSlowHigh + " default=" + defaultSlowHigh);
    // No knob setting ever starves the slow host below its probe floor.
    for (StagedRun run: new StagedRun[] { defaults, early, late, earlyFull }) {
      int last = run.stageLoads.length - 1;
      Assert.assertTrue(
          run.routed[last][slow] > 0.05 * run.stageLoads[last],
          "the slow host must stay above its probe floor under every knob setting; routed="
              + Arrays.toString(run.routed[last]));
    }
  }

  /**
   * The honest self-correction proof. A hidden per-group serving capacity (the physical "strength" of each
   * member) is <em>never</em> given to the router; the router sees only the latency each group produces under
   * load. Starting from an equal-latency estimate, the closed loop -- route by latency, observe the resulting
   * latency, re-route -- converges to a stable operating point in which (a) the stronger member carries the most
   * traffic, (b) load is shifted off the overloaded weak members onto the underused strong member, (c) the
   * slower members are never starved (they keep enough traffic to stay measured), and (d) the latency spread
   * shrinks materially versus even routing.
   *
   * <p>It is deliberately honest about the limit of a pure {@code 1/latency} weighting: when the capacity gap is
   * large it <em>relieves</em> the weak members' overload rather than eliminating it (they settle above their
   * fair even share but below where even routing left them). Fully driving every member under its ceiling would
   * need a more aggressive response (a larger strength exponent, or a utilization signal) -- a knob for later.
   * The point this proves is that measured latency alone is a sufficient, non-circular routing signal that
   * strictly improves on capacity-oblivious even routing -- no configured capacity required.
   */
  @Test
  public void testClosedLoopConvergesToBalancedLatencyFromMeasuredLatencyAlone() {
    int groupCount = 3;
    // Hidden ground-truth capacity: group 2 is genuinely 2x stronger. The router never reads this array.
    double[] hiddenCapacity = { 10500.0, 10500.0, 21000.0 };
    double totalCapacity = 42000.0;
    double load = totalCapacity; // saturate the cluster so the skew is fully exercised (u = 1.0)
    double u = load / totalCapacity;
    double m = HelixGroupWeightedLeastLoadedStrategy.DEFAULT_INTERPOLATION_EXPONENT;

    // The router's only per-group signal: measured latency. Seed it neutral (equal) -- it knows nothing yet.
    double[] measuredLatency = { LATENCY_BASE_MS, LATENCY_BASE_MS, LATENCY_BASE_MS };
    double damping = 0.3; // under-relaxation models EWMA smoothing of the measured latency and keeps the loop stable
    int maxRounds = 500;
    double[] shares = null;
    int converged = -1;

    for (int round = 0; round < maxRounds; round++) {
      shares = analyticShares(measuredLatency, u, m);
      double maxDelta = 0.0;
      for (int g = 0; g < groupCount; g++) {
        double served = shares[g] * load;
        double observed = environmentLatencyMs(served, hiddenCapacity[g]);
        double smoothed = damping * observed + (1.0 - damping) * measuredLatency[g];
        maxDelta = Math.max(maxDelta, Math.abs(smoothed - measuredLatency[g]));
        measuredLatency[g] = smoothed;
      }
      if (maxDelta < 0.05) {
        converged = round;
        break;
      }
    }

    Assert.assertTrue(converged >= 0, "The latency-driven loop must reach a stable fixed point; did not converge");

    // Route real traffic through the strategy at the converged latency to prove its draw matches the model.
    int requestCount = 60000;
    int[] routed =
        routeAndFinish(weightedStrategy(measuredLatency, u, m, new Random(SEED)), groupCount, 0, requestCount);
    double[] realizedShare = new double[groupCount];
    double[] servedLatency = new double[groupCount];
    for (int g = 0; g < groupCount; g++) {
      realizedShare[g] = routed[g] / (double) requestCount;
      servedLatency[g] = environmentLatencyMs(realizedShare[g] * load, hiddenCapacity[g]);
    }

    LOGGER.info(
        "Closed loop converged in {} rounds using measured latency ALONE (hidden capacity {} never read):",
        converged,
        Arrays.toString(hiddenCapacity));
    LOGGER.info("  converged latency (ms) = {}", Arrays.toString(round1(measuredLatency)));
    LOGGER.info("  realized shares %%      = {}", Arrays.toString(round1(scale(realizedShare, 100))));
    LOGGER.info("  served latency (ms)    = {}", Arrays.toString(round1(servedLatency)));

    // The reference point: what plain even (capacity-oblivious) routing would produce at this load.
    double evenServed = load / (double) groupCount;
    double[] evenLatency = new double[groupCount];
    for (int g = 0; g < groupCount; g++) {
      evenLatency[g] = environmentLatencyMs(evenServed, hiddenCapacity[g]);
    }
    double evenSpread = spread(evenLatency);
    double convergedSpread = spread(servedLatency);
    LOGGER.info(
        "  even-routing latency (ms) = {} (spread {}) vs converged spread {}",
        Arrays.toString(round1(evenLatency)),
        String.format("%.1f", evenSpread),
        String.format("%.1f", convergedSpread));

    int strongGroup = argMax(new int[] { (int) hiddenCapacity[0], (int) hiddenCapacity[1], (int) hiddenCapacity[2] });
    // (a) The genuinely stronger member carries the most traffic -- discovered from latency, not told.
    Assert.assertEquals(
        argMax(routed),
        strongGroup,
        "The stronger member should carry the most traffic; routed=" + Arrays.toString(routed));
    // (b) Load is shifted off the overloaded weak members and onto the underused strong member. Note the model
    // relieves the overload rather than eliminating it: a pure 1/latency weighting under-corrects when the
    // capacity gap is large, so the weak members settle above their fair even share but below where even routing
    // left them -- the strong member picks up the difference.
    Assert.assertTrue(
        realizedShare[strongGroup] * load > evenServed,
        "The strong member should absorb more than an even split; served=" + (realizedShare[strongGroup] * load));
    for (int g = 0; g < groupCount; g++) {
      if (g != strongGroup) {
        Assert.assertTrue(
            realizedShare[g] * load < evenServed,
            "Load should be shifted off weak member " + g + "; served=" + (realizedShare[g] * load) + " even="
                + evenServed);
      }
      // (c) No member is starved: even a slower member keeps a meaningful share (>= probe floor).
      Assert.assertTrue(
          routed[g] > 0.05 * requestCount,
          "Slower members must not be starved; routed=" + Arrays.toString(routed));
    }
    // (d) The latency imbalance shrinks materially versus even routing -- the strong member's headroom is used to
    // pull the tail in rather than one weak member blowing far past the others.
    Assert.assertTrue(
        convergedSpread < evenSpread * 0.6,
        "Latency-driven routing should materially shrink the latency spread; even=" + evenSpread + " converged="
            + convergedSpread);
  }

  private static double spread(double[] values) {
    double min = values[0];
    double max = values[0];
    for (double v: values) {
      min = Math.min(min, v);
      max = Math.max(max, v);
    }
    return max - min;
  }

  /** Render a box-drawn table to the log: computes per-column widths and centers each cell. */
  private static void logTable(String title, String[] headers, List<String[]> rows) {
    int columns = headers.length;
    int[] width = new int[columns];
    for (int c = 0; c < columns; c++) {
      width[c] = headers[c].length();
    }
    for (String[] row: rows) {
      for (int c = 0; c < columns; c++) {
        width[c] = Math.max(width[c], row[c].length());
      }
    }
    String top = border(width, '┌', '┬', '┐');
    String mid = border(width, '├', '┼', '┤');
    String bottom = border(width, '└', '┴', '┘');

    StringBuilder table = new StringBuilder("\n").append(title).append('\n');
    table.append(top).append('\n').append(rowLine(headers, width)).append('\n').append(mid).append('\n');
    for (String[] row: rows) {
      table.append(rowLine(row, width)).append('\n');
    }
    table.append(bottom);
    LOGGER.info(table.toString());
  }

  private static String border(int[] width, char left, char joint, char right) {
    StringBuilder line = new StringBuilder().append(left);
    for (int c = 0; c < width.length; c++) {
      for (int i = 0; i < width[c] + 2; i++) {
        line.append('─');
      }
      line.append(c == width.length - 1 ? right : joint);
    }
    return line.toString();
  }

  private static String rowLine(String[] cells, int[] width) {
    StringBuilder line = new StringBuilder().append('│');
    for (int c = 0; c < cells.length; c++) {
      line.append(' ').append(center(cells[c], width[c])).append(" │");
    }
    return line.toString();
  }

  private static String center(String value, int width) {
    int pad = width - value.length();
    int left = pad / 2;
    int right = pad - left;
    StringBuilder out = new StringBuilder();
    for (int i = 0; i < left; i++) {
      out.append(' ');
    }
    out.append(value);
    for (int i = 0; i < right; i++) {
      out.append(' ');
    }
    return out.toString();
  }

  private static final double STAGED_TOTAL_QUOTA = 42000.0;
  private static final int[] STAGED_LOADS = { 10000, 20000, 30000, 38000, 42000 };

  /** The per-stage outputs of a staged scenario run, so a test can assert on them after the table is rendered. */
  private static final class StagedRun {
    final int[] stageLoads;
    final double[] utilization;
    final int[][] routed;
    final double[][] servedLatency;
    final double[] impliedCeiling;

    StagedRun(
        int[] stageLoads,
        double[] utilization,
        int[][] routed,
        double[][] servedLatency,
        double[] impliedCeiling) {
      this.stageLoads = stageLoads;
      this.utilization = utilization;
      this.routed = routed;
      this.servedLatency = servedLatency;
      this.impliedCeiling = impliedCeiling;
    }
  }

  /**
   * Drive the shared five-stage harness for a given measured-latency vector: route real traffic through the
   * strategy at each rising RPS stage, sanity-check that the routed distribution matches the analytic model,
   * render the box table, and return the per-stage outputs. The only per-group signal fed to the strategy is
   * {@code measuredLatency}; the implied per-group ceiling used for the latency column is derived from the
   * saturation shares purely for reporting and is never given to the strategy.
   */
  private StagedRun runStagedScenario(String scenario, double[] measuredLatency) {
    return runStagedScenario(
        scenario,
        measuredLatency,
        HelixGroupWeightedLeastLoadedStrategy.DEFAULT_EVEN_UNTIL_UTILIZATION,
        HelixGroupWeightedLeastLoadedStrategy.DEFAULT_FULL_SKEW_AT_UTILIZATION,
        HelixGroupWeightedLeastLoadedStrategy.DEFAULT_INTERPOLATION_EXPONENT);
  }

  private StagedRun runStagedScenario(
      String scenario,
      double[] measuredLatency,
      double evenUntil,
      double fullSkew,
      double m) {
    int groupCount = measuredLatency.length;
    double[] saturationShares = analyticShares(measuredLatency, 1.0, evenUntil, fullSkew, m);
    double[] impliedCeiling = new double[groupCount];
    for (int g = 0; g < groupCount; g++) {
      impliedCeiling[g] = saturationShares[g] * STAGED_TOTAL_QUOTA;
    }

    int[][] routedByStage = new int[STAGED_LOADS.length][];
    double[][] latencyByStage = new double[STAGED_LOADS.length][];
    double[] utilization = new double[STAGED_LOADS.length];
    List<String[]> tableRows = new ArrayList<>();
    long nextRequestId = 0;

    for (int stage = 0; stage < STAGED_LOADS.length; stage++) {
      int load = STAGED_LOADS[stage];
      double u = load / STAGED_TOTAL_QUOTA;
      utilization[stage] = u;

      int[] routed = routeAndFinish(
          weightedStrategy(measuredLatency, u, evenUntil, fullSkew, m, new Random(SEED)),
          groupCount,
          nextRequestId,
          load);
      nextRequestId += load;

      double[] shares = new double[groupCount];
      double[] servedLatency = new double[groupCount];
      double weightedLatencySum = 0.0;
      for (int g = 0; g < groupCount; g++) {
        shares[g] = routed[g] / (double) load;
        // The latency this stage's routing would produce, given each group's implied ceiling (a consequence,
        // reported to show that keeping groups at/under their ceiling keeps every group within SLO).
        servedLatency[g] = Math.min(LATENCY_SLO_MS, environmentLatencyMs(routed[g], impliedCeiling[g]));
        weightedLatencySum += routed[g] * servedLatency[g];
      }
      double totalAvgLatency = weightedLatencySum / load;
      int requestSpread = maxOf(routed) - minOf(routed);
      double requestSpreadPct = 100.0 * requestSpread / load;

      // Sanity: the real strategy's routed distribution matches the analytic model for this latency + u + knobs.
      double[] expected = analyticShares(measuredLatency, u, evenUntil, fullSkew, m);
      for (int g = 0; g < groupCount; g++) {
        Assert.assertTrue(
            Math.abs(shares[g] - expected[g]) < 0.02,
            scenario + " stage " + stage + " group " + g + " share=" + shares[g] + " should match model "
                + expected[g]);
      }

      tableRows.add(
          new String[] { String.valueOf(stage), String.valueOf(load), String.format("%.3f", u), Arrays.toString(routed),
              join(shares, 100.0, "%.1f"), String.format("%d (%.1f%%)", requestSpread, requestSpreadPct),
              join(servedLatency, 1.0, "%.1f"), String.format("%.1f", totalAvgLatency) });

      routedByStage[stage] = routed;
      latencyByStage[stage] = servedLatency;
    }

    logTable(
        scenario + " (implied ceilings "
            + Arrays.toString(Arrays.stream(impliedCeiling).mapToLong(Math::round).toArray()) + ")",
        new String[] { "stage", "load (RPS)", "u", "routed (absolute)", "shares %", "req spread (max-min)",
            "avg latency / group (ms)", "total avg (ms)" },
        tableRows);

    return new StagedRun(STAGED_LOADS, utilization, routedByStage, latencyByStage, impliedCeiling);
  }

  private static String join(double[] values, double scale, String perFormat) {
    StringBuilder out = new StringBuilder();
    for (int i = 0; i < values.length; i++) {
      if (i > 0) {
        out.append(" / ");
      }
      out.append(String.format(perFormat, values[i] * scale));
    }
    return out.toString();
  }

  private static int maxOf(int[] values) {
    int max = values[0];
    for (int v: values) {
      max = Math.max(max, v);
    }
    return max;
  }

  private static int minOf(int[] values) {
    int min = values[0];
    for (int v: values) {
      min = Math.min(min, v);
    }
    return min;
  }

  /**
   * When no group has been measured yet (every latency non-positive, i.e. {@link HelixGroupStats}'s "-1 for an
   * unused group"), every group is treated as neutral, so routing stays even at <em>every</em> utilization
   * level -- including a saturated batch. This is the safe default before any latency has been observed.
   */
  @Test
  public void testUnmeasuredGroupsStayEvenAtAllLoads() {
    int groupCount = 4;
    double evenShare = 1.0 / groupCount;
    double[] latency = { -1.0, -1.0, -1.0, -1.0 }; // nothing measured yet

    int[] routed = routeAndFinish(
        weightedStrategy(
            latency,
            0.99,
            HelixGroupWeightedLeastLoadedStrategy.DEFAULT_INTERPOLATION_EXPONENT,
            new Random(SEED)),
        groupCount,
        0,
        60000);

    LOGGER.info("No latency measured yet, high u -> routed {} (should be ~even)", Arrays.toString(routed));
    for (int g = 0; g < groupCount; g++) {
      double share = routed[g] / 60000.0;
      Assert.assertTrue(
          Math.abs(share - evenShare) < 0.02,
          "Unmeasured groups must stay even at all loads; group " + g + " share=" + share);
    }
  }

  /**
   * Edge case: a not-yet-measured group ({@code latency <= 0}) mixed with measured groups at high utilization
   * must be treated neutrally -- it is neither flooded (as a naive {@code 1/latency} with latency 0 would do)
   * nor starved. It should land near the even share while the measured groups skew by their latency.
   */
  @Test
  public void testNotYetMeasuredGroupTreatedNeutrally() {
    int groupCount = 3;
    // Groups 0 and 1 measured (one fast, one slow); group 2 has no data yet.
    double[] latency = { 20.0, 60.0, -1.0 };
    double evenShare = 1.0 / groupCount;

    int[] routed = routeAndFinish(
        weightedStrategy(
            latency,
            0.98,
            HelixGroupWeightedLeastLoadedStrategy.DEFAULT_INTERPOLATION_EXPONENT,
            new Random(SEED)),
        groupCount,
        0,
        60000);

    double unmeasuredShare = routed[2] / 60000.0;
    LOGGER.info(
        "Unmeasured group among measured ones -> routed {} (group 2 should be ~neutral)",
        Arrays.toString(routed));
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
   * The probe floor keeps even a very slow group alive at full utilization: it always retains at least
   * {@link HelixGroupWeightedLeastLoadedStrategy#PROBE_FLOOR_FRACTION} of the even share, so the router keeps
   * observing its latency and the signal stays live and self-correcting.
   */
  @Test
  public void testProbeFloorKeepsSlowGroupAlive() {
    int groupCount = 3;
    // Group 1 is an order of magnitude slower; without a floor its share would collapse toward zero at u=1.
    double[] latency = { 20.0, 400.0, 20.0 };
    int requestCount = 60000;
    double evenShare = 1.0 / groupCount;
    double floor = HelixGroupWeightedLeastLoadedStrategy.PROBE_FLOOR_FRACTION * evenShare;

    int[] routed = routeAndFinish(
        weightedStrategy(
            latency,
            1.0,
            HelixGroupWeightedLeastLoadedStrategy.DEFAULT_INTERPOLATION_EXPONENT,
            new Random(SEED)),
        groupCount,
        0,
        requestCount);

    double slowShare = routed[1] / (double) requestCount;
    LOGGER
        .info("Very slow group at u=1 -> routed {} (slow group should retain >= probe floor)", Arrays.toString(routed));
    Assert.assertTrue(
        slowShare >= 0.5 * floor,
        "The slow group must retain at least ~the probe floor so it stays measured; share=" + slowShare + " floor="
            + floor);
    Assert.assertTrue(routed[1] > 0, "The slow group must never be fully starved; routed=" + Arrays.toString(routed));
  }

  /** Boundary: a single group is always selected, and every request is accounted for. */
  @Test
  public void testSingleGroupAlwaysSelected() {
    double[] latency = { 25.0 };
    int[] routed = routeAndFinish(
        weightedStrategy(
            latency,
            0.9,
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
        new double[] { 25.0 },
        0.5,
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
        new double[] { 25.0, 25.0, 25.0 },
        0.5,
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
   * <p>The new strategy still reads latency, but with ample aggregate headroom (low utilization) it spreads
   * evenly across all groups regardless of the latency spread, holding each near its fair share and preserving
   * quota. The two strategies are compared on the same harness to show the fix removes the over-concentration.
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

    // --- Fix: new weighted strategy on the same latency vector but with ample headroom (low utilization). ---
    HelixGroupWeightedLeastLoadedStrategy newStrategy = weightedStrategy(
        measured,
        0.10, // ample headroom -> even regardless of the latency spread
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

  private static double[] round1(double[] values) {
    double[] out = new double[values.length];
    for (int i = 0; i < values.length; i++) {
      out[i] = Math.round(values[i] * 10.0) / 10.0;
    }
    return out;
  }

  private static double[] scale(double[] values, double factor) {
    double[] out = new double[values.length];
    for (int i = 0; i < values.length; i++) {
      out[i] = values[i] * factor;
    }
    return out;
  }
}
