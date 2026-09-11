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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Integration-style tests for {@link HelixGroupWeightedLeastLoadedStrategy} that drive a stream of mock
 * requests through the strategy and snapshot how many queries were routed to each group as the per-group
 * <em>measured latency</em> diverges.
 *
 * <p>The strategy is a best-effort latency equaliser. Its only per-group signal is measured latency; it infers
 * both <em>which</em> group is fast ({@code strength(g) = 1 / latency(g)}) and <em>how much</em> to skew from
 * the latency spread {@code ratio = max(latency) / min(latency)} across the measured groups:
 * {@code share(g) = (1 - skew) / G + skew * strength(g) / sum(strength)}, where {@code skew} ramps from 0 to 1
 * as {@code ratio} climbs from {@code evenUntilLatencyRatio} to {@code fullSkewAtLatencyRatio}. There is no
 * configured per-group capacity and no aggregate-utilization input: while the groups' latencies are close the
 * strategy routes evenly, and as one group's latency pulls ahead it sheds traffic onto the faster groups.
 *
 * <p>Latency is injected through a simple provider so each scenario can move it deterministically. The staged
 * scenarios go one step further and run a <em>closed loop</em>: a hidden per-group serving capacity (never read
 * by the router) turns each group's served load into a latency via {@link #environmentLatencyMs}, the strategy
 * re-routes on that observed latency, and the loop is iterated to a fixed point. Randomness in the weighted
 * draw is made deterministic by injecting a seeded {@link Random}, so the snapshots are reproducible.
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
   * Build a weighted strategy that reads per-group latency from a live array with an injected seeded random. The
   * array is read live, so a scenario can mutate latency between batches and the strategy will observe it.
   */
  private static HelixGroupWeightedLeastLoadedStrategy weightedStrategy(
      double[] latency,
      double evenUntilLatencyRatio,
      double fullSkewAtLatencyRatio,
      double interpolationExponent,
      Random random) {
    return new HelixGroupWeightedLeastLoadedStrategy(
        mockTimeoutProcessor(),
        TIMEOUT_MS,
        mock(HelixGroupStats.class),
        groupId -> latency[groupId],
        evenUntilLatencyRatio,
        fullSkewAtLatencyRatio,
        interpolationExponent,
        random::nextDouble);
  }

  /** Convenience overload using the strategy's default stay-even and full-skew latency-ratio thresholds. */
  private static HelixGroupWeightedLeastLoadedStrategy weightedStrategy(
      double[] latency,
      double interpolationExponent,
      Random random) {
    return weightedStrategy(
        latency,
        HelixGroupWeightedLeastLoadedStrategy.DEFAULT_EVEN_UNTIL_LATENCY_RATIO,
        HelixGroupWeightedLeastLoadedStrategy.DEFAULT_FULL_SKEW_AT_LATENCY_RATIO,
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

  private static int argMaxDouble(double[] values) {
    int idx = 0;
    for (int i = 1; i < values.length; i++) {
      if (values[i] > values[idx]) {
        idx = i;
      }
    }
    return idx;
  }

  private static final double LATENCY_BASE_MS = 20.0;
  private static final double LATENCY_LOAD_FACTOR = 4.0;
  private static final double LATENCY_LOAD_EXPONENT = 3.0;

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
   * The latency spread ratio (slowest / fastest) across the measured groups -- the exact signal the strategy
   * gates its skew on. Groups with non-positive latency are not yet measured and are excluded.
   */
  private static double latencyRatio(double[] latency) {
    double min = Double.MAX_VALUE;
    double max = 0.0;
    int measured = 0;
    for (double l: latency) {
      if (l > 0) {
        double clamped = Math.max(l, HelixGroupWeightedLeastLoadedStrategy.MIN_LATENCY_MS);
        min = Math.min(min, clamped);
        max = Math.max(max, clamped);
        measured++;
      }
    }
    return measured < 2 ? 1.0 : max / min;
  }

  /** The skew factor the strategy derives from a latency-ratio and the two knobs; mirrors the production math. */
  private static double skewFor(double ratio, double evenUntil, double fullSkew, double m) {
    if (ratio <= evenUntil) {
      return 0.0;
    }
    if (ratio >= fullSkew) {
      return 1.0;
    }
    double position = (ratio - evenUntil) / (fullSkew - evenUntil);
    return m == 1.0 ? position : Math.pow(position, m);
  }

  /**
   * The analytic realized share the strategy targets for the given measured latencies and knobs, matching
   * {@link HelixGroupWeightedLeastLoadedStrategy}'s math (strength = 1/latency, latency-ratio skew, probe floor,
   * reservoir normalisation). Used to (a) assert the strategy's actual routed distribution matches its model and
   * (b) find the environment's fixed point in the closed-loop scenarios.
   */
  private static double[] analyticShares(double[] latency, double m) {
    return analyticShares(
        latency,
        HelixGroupWeightedLeastLoadedStrategy.DEFAULT_EVEN_UNTIL_LATENCY_RATIO,
        HelixGroupWeightedLeastLoadedStrategy.DEFAULT_FULL_SKEW_AT_LATENCY_RATIO,
        m);
  }

  private static double[] analyticShares(double[] latency, double evenUntilRatio, double fullSkewRatio, double m) {
    int groupCount = latency.length;
    double even = 1.0 / groupCount;
    double floor = HelixGroupWeightedLeastLoadedStrategy.PROBE_FLOOR_FRACTION * even;
    double skew = skewFor(latencyRatio(latency), evenUntilRatio, fullSkewRatio, m);

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
   * Headline behaviour: when the groups' latencies are close (within the stay-even ratio) the strategy routes
   * evenly even though one group is measurably faster. Treating a small spread as noise keeps every group's
   * read-quota consumption low and avoids the over-concentration that drives a single group to its 429 ceiling.
   */
  @Test
  public void testEvenWhenLatencyCloseDespiteDifference() {
    int groupCount = 3;
    // Group 2 is measurably faster, but the spread (22/20 = 1.1x) is inside the stay-even ratio (1.2x default).
    double[] latency = { 22.0, 21.0, 20.0 };
    double evenShare = 1.0 / groupCount;

    int[] routed = routeAndFinish(
        weightedStrategy(
            latency,
            HelixGroupWeightedLeastLoadedStrategy.DEFAULT_INTERPOLATION_EXPONENT,
            new Random(SEED)),
        groupCount,
        0,
        60000);

    LOGGER.info(
        "Close latency {} (ratio {}) -> routed {} (should be ~even {})",
        Arrays.toString(latency),
        String.format("%.2f", latencyRatio(latency)),
        Arrays.toString(routed),
        String.format("%.1f%%", 100 * evenShare));

    for (int g = 0; g < groupCount; g++) {
      double share = routed[g] / 60000.0;
      Assert.assertTrue(
          Math.abs(share - evenShare) < 0.02,
          "Within the stay-even ratio every group should get ~even share despite the latency gap; group " + g
              + " share=" + share);
    }
  }

  /**
   * The complement of the previous test: once the latency spread reaches the full-skew ratio, routing converges
   * to the latency-proportional split, so the faster (lower-latency) group absorbs proportionally more traffic.
   * With latency {40, 40, 20} the ratio is 2.0 (>= full-skew default) and the strengths are {1/40, 1/40, 1/20},
   * i.e. shares {0.25, 0.25, 0.50}.
   */
  @Test
  public void testSkewsToFastGroupWhenLatencyDiverges() {
    int groupCount = 3;
    double[] latency = { 40.0, 40.0, 20.0 }; // ratio 2.0 -> full skew -> strength shares 0.25 / 0.25 / 0.50
    int fastGroup = argMin(latency);

    int[] routed = routeAndFinish(
        weightedStrategy(
            latency,
            HelixGroupWeightedLeastLoadedStrategy.DEFAULT_INTERPOLATION_EXPONENT,
            new Random(SEED)),
        groupCount,
        0,
        60000);

    LOGGER.info(
        "Wide latency spread {} (ratio {}) -> routed {} (should approach strength shares 25/25/50)",
        Arrays.toString(latency),
        String.format("%.2f", latencyRatio(latency)),
        Arrays.toString(routed));

    double[] expected = analyticShares(latency, HelixGroupWeightedLeastLoadedStrategy.DEFAULT_INTERPOLATION_EXPONENT);
    for (int g = 0; g < groupCount; g++) {
      double share = routed[g] / 60000.0;
      Assert.assertTrue(
          Math.abs(share - expected[g]) < 0.03,
          "At full skew each group's share should approach its latency-derived share; group " + g + " share=" + share
              + " expected=" + expected[g]);
    }
    Assert.assertEquals(argMax(routed), fastGroup, "The faster group should absorb the most traffic at full skew");
  }

  /**
   * The design reproduction: replay Ali Poursamadi's five-stage model with request rate (RPS) rising in tandem
   * across stages, driven <em>only</em> by measured latency. Three groups are served; group 2 is genuinely the
   * stronger member (2x the hidden serving capacity of each weak member), which the router never reads -- it
   * discovers group 2 is stronger purely from the lower latency group 2 produces under load.
   *
   * <p>Each stage offers a higher absolute load than the last (10k -> 20k -> 30k -> 38k -> 42k RPS). At low RPS
   * every group has headroom, latencies are close, the spread is inside the stay-even ratio, and routing is
   * even. As RPS climbs the weak members' latency pulls ahead, the spread widens past the stay-even ratio, and
   * the model shifts to a latency-proportional split with the stronger member carrying a growing fraction.
   *
   * <p>The key health property this asserts is on <em>absolute</em> traffic, not just shares: because RPS rises
   * every stage, each group -- including the weaker members -- must keep receiving <em>more</em> absolute
   * traffic as load grows (its share shrinks, but its throughput does not). Traffic is never taken away from the
   * weaker machines; the stronger member simply absorbs a growing <em>fraction</em> of the growing total.
   */
  @Test
  public void testReproducesFiveStageLatencyModel() {
    // Hidden ground-truth capacity: group 2 is genuinely 2x stronger. The router never reads this array.
    double[] hiddenCapacity = { 10500.0, 10500.0, 21000.0 };
    int groupCount = hiddenCapacity.length;
    int strongGroup = argMaxDouble(hiddenCapacity);
    double slack = 0.01 * STAGED_TOTAL_QUOTA;

    StagedRun run = runStagedScenario("Five-stage latency model - group 2 is 2x stronger", hiddenCapacity);

    double previousStrongShare = -1.0;
    int[] previousRouted = null;
    for (int stage = 0; stage < run.stageLoads.length; stage++) {
      int[] routed = run.routed[stage];
      int load = run.stageLoads[stage];
      double strongShare = routed[strongGroup] / (double) load;
      // The stronger group's share must not shrink as load grows.
      if (previousStrongShare >= 0) {
        Assert.assertTrue(
            strongShare >= previousStrongShare - 0.01,
            "Stronger group share should grow (not shrink) as load rises; was " + previousStrongShare + " now "
                + strongShare);
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
      previousStrongShare = strongShare;
      previousRouted = routed;
    }

    // At saturation the stronger member carries the most traffic and the weaker members are relieved but never
    // starved -- and the served-latency spread has shrunk materially versus capacity-oblivious even routing.
    int lastStage = run.stageLoads.length - 1;
    Assert.assertEquals(
        argMax(run.routed[lastStage]),
        strongGroup,
        "At saturation the stronger member should carry the most traffic; routed="
            + Arrays.toString(run.routed[lastStage]));
    double evenSpread = spread(evenRoutingLatencies(hiddenCapacity, STAGED_LOADS[lastStage]));
    Assert.assertTrue(
        spread(run.servedLatency[lastStage]) < 0.6 * evenSpread,
        "At saturation the served-latency spread should shrink materially vs even routing; even=" + evenSpread
            + " converged=" + spread(run.servedLatency[lastStage]));
  }

  /**
   * Control scenario: when every host is identical capacity-wise there is no "faster" group to skew toward, so
   * every stage's served latency is equal, the spread ratio stays ~1, and routing stays even and the request
   * spread stays ~0 all the way up to full quota. This is the common homogeneous-fault-zone case: the strategy
   * is a safe no-op that behaves exactly like plain even routing when there is no real latency signal to act on.
   */
  @Test
  public void testEvenHostsStayEvenAsRpsRises() {
    double[] hiddenCapacity = { 14000.0, 14000.0, 14000.0 };
    int groupCount = hiddenCapacity.length;

    StagedRun run = runStagedScenario("Even hosts - identical capacity (14k/14k/14k)", hiddenCapacity);

    for (int stage = 0; stage < run.stageLoads.length; stage++) {
      int[] routed = run.routed[stage];
      int load = run.stageLoads[stage];
      double spreadPct = 100.0 * (maxOf(routed) - minOf(routed)) / load;
      Assert.assertTrue(
          run.latencyRatio[stage] < 1.05,
          "Identical hosts should keep the latency spread ~1; ratio=" + run.latencyRatio[stage]);
      Assert.assertTrue(
          spreadPct < 2.0,
          "Identical hosts must stay even at RPS=" + load + "; req spread=" + spreadPct + "%");
      for (int g = 0; g < groupCount; g++) {
        Assert.assertTrue(
            Math.abs(routed[g] / (double) load - 1.0 / groupCount) < 0.02,
            "Identical hosts: every group should stay near the even share; routed=" + Arrays.toString(routed));
      }
    }
  }

  /**
   * The single-slow-host scenario: two identical strong hosts plus one weaker host (2/3 the capacity). At low
   * RPS routing is even (latencies are close, so the spread is inside the stay-even ratio), but as RPS climbs
   * the weaker host's latency pulls ahead, the spread widens, and it progressively <em>sheds</em> share to the
   * two strong hosts -- its absolute traffic still rises (never starved, never taken below its floor), it simply
   * carries a shrinking fraction of a growing total. The request spread grows with load while the served
   * latencies converge relative to even routing, so no host is pushed far past the others.
   */
  @Test
  public void testOneSlowHostShedsTrafficAsRpsRises() {
    double[] hiddenCapacity = { 16800.0, 16800.0, 8400.0 };
    int slowGroup = 2;
    int groupCount = hiddenCapacity.length;

    StagedRun run =
        runStagedScenario("One slow host - group 2 is half the capacity (16.8k/16.8k/8.4k)", hiddenCapacity);

    int lastStage = run.stageLoads.length - 1;
    int[] firstRouted = run.routed[0];
    int[] lastRouted = run.routed[lastStage];

    // Low load: even (the slow host is barely distinguished, spread inside the stay-even ratio).
    double firstSpreadPct = 100.0 * (maxOf(firstRouted) - minOf(firstRouted)) / run.stageLoads[0];
    Assert.assertTrue(
        firstSpreadPct < 3.0,
        "At low load routing should still be near-even; spread=" + firstSpreadPct + "%");

    // High load: the slow host carries the least, the two strong hosts carry the most.
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

    // Request spread widens with load; served latencies converge relative to even routing (relieved saturation).
    Assert.assertTrue(
        (maxOf(lastRouted) - minOf(lastRouted)) > (maxOf(firstRouted) - minOf(firstRouted)),
        "Request spread should widen as RPS rises for a heterogeneous fleet");
    double evenSpread = spread(evenRoutingLatencies(hiddenCapacity, STAGED_LOADS[lastStage]));
    Assert.assertTrue(
        spread(run.servedLatency[lastStage]) < 0.6 * evenSpread,
        "At saturation the served latencies should converge vs even routing; even=" + evenSpread + " converged="
            + spread(run.servedLatency[lastStage]));
  }

  /**
   * The two controllable knobs in action, on a fixed latency vector whose spread ({@code ratio = 32/20 = 1.6})
   * sits between the default stay-even (1.2) and full-skew (2.0) ratios. Both knobs move <em>where</em> on the
   * latency-spread axis the strategy reacts, without touching the latency signal itself:
   *
   * <ul>
   *   <li><b>evenUntilLatencyRatio (stay-even knob)</b> — the latency spread up to which routing stays fully
   *       even. Raising it above the observed 1.6x spread (1.2 -> 1.8) keeps the fleet even; lowering it makes
   *       the strategy react to smaller imbalances.</li>
   *   <li><b>fullSkewAtLatencyRatio (full-skew knob)</b> — the latency spread at which routing reaches its
   *       maximum latency-proportional split. Lowering it below the observed spread (2.0 -> 1.5) reaches full
   *       protection immediately, so the slow host is at its floor-bounded minimum.</li>
   * </ul>
   *
   * Each configuration renders a row so the effect of moving a knob is directly visible.
   */
  @Test
  public void testKnobsControlEvenBandAndFullSkewOnset() {
    int groupCount = 3;
    double[] latency = { 20.0, 20.0, 32.0 }; // ratio 1.6
    int slow = 2;
    int requestCount = 60000;
    double even = 1.0 / groupCount;
    double linear = 1.0;

    int[] defaultRouted =
        routeAndFinish(weightedStrategy(latency, 1.2, 2.0, linear, new Random(SEED)), groupCount, 0, requestCount);
    int[] stayEvenRouted =
        routeAndFinish(weightedStrategy(latency, 1.8, 2.0, linear, new Random(SEED)), groupCount, 0, requestCount);
    int[] fullSkewRouted =
        routeAndFinish(weightedStrategy(latency, 1.2, 1.5, linear, new Random(SEED)), groupCount, 0, requestCount);

    double defaultSlow = defaultRouted[slow] / (double) requestCount;
    double stayEvenSlow = stayEvenRouted[slow] / (double) requestCount;
    double fullSkewSlow = fullSkewRouted[slow] / (double) requestCount;

    List<String[]> rows = new ArrayList<>();
    rows.add(knobRow("even<=1.2, full-skew@2.0 (default)", latency, defaultRouted, requestCount));
    rows.add(knobRow("even<=1.8, full-skew@2.0 (stays even)", latency, stayEvenRouted, requestCount));
    rows.add(knobRow("even<=1.2, full-skew@1.5 (full skew now)", latency, fullSkewRouted, requestCount));
    logTable(
        "Knob control on a fixed 1.6x latency spread " + Arrays.toString(latency),
        new String[] { "knobs", "routed (absolute)", "shares %", "slow-host share %" },
        rows);

    // Stay-even knob raised above the 1.6x spread: the slow host is still routed ~evenly.
    Assert.assertEquals(
        stayEvenSlow,
        even,
        0.02,
        "with stay-even=1.8 (above the 1.6x spread) the slow host stays ~even; share=" + stayEvenSlow);
    // The default knob (stay-even 1.2) already reacts to the 1.6x spread, so the slow host sits below even.
    Assert.assertTrue(
        defaultSlow < even - 0.02,
        "the default knob reacts to the 1.6x spread and sheds slow-host traffic; share=" + defaultSlow);
    // Lowering the full-skew knob below the spread reaches full skew, shedding even more than the default ramp.
    Assert.assertTrue(
        fullSkewSlow < defaultSlow - 0.02,
        "reaching full skew at 1.5 sheds more slow-host traffic than the default ramp; full=" + fullSkewSlow
            + " default=" + defaultSlow);
    // No knob setting ever starves the slow host below its probe floor.
    double floor = HelixGroupWeightedLeastLoadedStrategy.PROBE_FLOOR_FRACTION * even;
    for (double slowShare: new double[] { defaultSlow, stayEvenSlow, fullSkewSlow }) {
      Assert.assertTrue(
          slowShare >= 0.5 * floor,
          "the slow host must stay above its probe floor under every knob setting; share=" + slowShare);
    }
  }

  private static String[] knobRow(String label, double[] latency, int[] routed, int requestCount) {
    double[] shares = new double[routed.length];
    for (int g = 0; g < routed.length; g++) {
      shares[g] = routed[g] / (double) requestCount;
    }
    return new String[] { label, Arrays.toString(routed), join(shares, 100.0, "%.1f"),
        String.format("%.1f", 100.0 * routed[routed.length - 1] / requestCount) };
  }

  /**
   * The honest self-correction proof. A hidden per-group serving capacity (the physical "strength" of each
   * member) is <em>never</em> given to the router; the router sees only the latency each group produces under
   * load. Starting from an equal-latency estimate, the closed loop -- route by latency, observe the resulting
   * latency, re-route -- converges to a stable operating point in which (a) the stronger member carries the most
   * traffic, (b) load is shifted off the overloaded weak members onto the underused strong member, (c) the
   * weaker members are never starved (they keep enough traffic to stay measured), and (d) the latency spread
   * shrinks materially versus even routing.
   *
   * <p>It is deliberately honest about the limit of a pure {@code 1/latency} weighting: when the capacity gap is
   * large it <em>relieves</em> the weak members' overload rather than eliminating it (they settle above their
   * fair even share but below where even routing left them). The point this proves is that measured latency
   * alone is a sufficient, non-circular routing signal that strictly improves on capacity-oblivious even
   * routing -- no configured capacity required.
   */
  @Test
  public void testClosedLoopConvergesToBalancedLatencyFromMeasuredLatencyAlone() {
    int groupCount = 3;
    // Hidden ground-truth capacity: group 2 is genuinely 2x stronger. The router never reads this array.
    double[] hiddenCapacity = { 10500.0, 10500.0, 21000.0 };
    double load = 42000.0; // saturate the cluster so the skew is fully exercised
    double m = HelixGroupWeightedLeastLoadedStrategy.DEFAULT_INTERPOLATION_EXPONENT;

    double[] measuredLatency = closedLoopLatency(
        hiddenCapacity,
        load,
        HelixGroupWeightedLeastLoadedStrategy.DEFAULT_EVEN_UNTIL_LATENCY_RATIO,
        HelixGroupWeightedLeastLoadedStrategy.DEFAULT_FULL_SKEW_AT_LATENCY_RATIO,
        m);
    Assert.assertNotNull(measuredLatency, "The latency-driven loop must reach a stable fixed point; did not converge");

    // Route real traffic through the strategy at the converged latency to prove its draw matches the model.
    int requestCount = 60000;
    int[] routed = routeAndFinish(weightedStrategy(measuredLatency, m, new Random(SEED)), groupCount, 0, requestCount);
    double[] realizedShare = new double[groupCount];
    double[] servedLatency = new double[groupCount];
    for (int g = 0; g < groupCount; g++) {
      realizedShare[g] = routed[g] / (double) requestCount;
      servedLatency[g] = environmentLatencyMs(realizedShare[g] * load, hiddenCapacity[g]);
    }

    LOGGER.info(
        "Closed loop converged using measured latency ALONE (hidden capacity {} never read):",
        Arrays.toString(hiddenCapacity));
    LOGGER.info(
        "  converged latency (ms) = {} (ratio {})",
        Arrays.toString(round1(measuredLatency)),
        String.format("%.2f", latencyRatio(measuredLatency)));
    LOGGER.info("  realized shares %%      = {}", Arrays.toString(round1(scale(realizedShare, 100))));
    LOGGER.info("  served latency (ms)    = {}", Arrays.toString(round1(servedLatency)));

    // The reference point: what plain even (capacity-oblivious) routing would produce at this load.
    double[] evenLatency = evenRoutingLatencies(hiddenCapacity, (int) load);
    double evenSpread = spread(evenLatency);
    double convergedSpread = spread(servedLatency);
    LOGGER.info(
        "  even-routing latency (ms) = {} (spread {}) vs converged spread {}",
        Arrays.toString(round1(evenLatency)),
        String.format("%.1f", evenSpread),
        String.format("%.1f", convergedSpread));

    double evenServed = load / groupCount;
    int strongGroup = argMaxDouble(hiddenCapacity);
    // (a) The genuinely stronger member carries the most traffic -- discovered from latency, not told.
    Assert.assertEquals(
        argMax(routed),
        strongGroup,
        "The stronger member should carry the most traffic; routed=" + Arrays.toString(routed));
    // (b) Load is shifted off the overloaded weak members and onto the underused strong member.
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
      // (c) No member is starved: even a weaker member keeps a meaningful share (>= probe floor).
      Assert.assertTrue(
          routed[g] > 0.05 * requestCount,
          "Weaker members must not be starved; routed=" + Arrays.toString(routed));
    }
    // (d) The latency imbalance shrinks materially versus even routing.
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

  /** The per-group latency plain even (capacity-oblivious) routing would produce at the given total load. */
  private static double[] evenRoutingLatencies(double[] hiddenCapacity, int load) {
    int groupCount = hiddenCapacity.length;
    double evenServed = load / (double) groupCount;
    double[] latency = new double[groupCount];
    for (int g = 0; g < groupCount; g++) {
      latency[g] = environmentLatencyMs(evenServed, hiddenCapacity[g]);
    }
    return latency;
  }

  /**
   * Iterate the environment/strategy closed loop to its fixed point for a single load: the strategy routes on
   * the current measured latency, the environment turns each group's served load into a new latency via the
   * hidden capacity, and an under-relaxed (EWMA-like) update is applied until the measured latencies stop
   * moving. Returns the converged measured latencies, or {@code null} if it did not converge.
   */
  private static double[] closedLoopLatency(
      double[] hiddenCapacity,
      double load,
      double evenUntilRatio,
      double fullSkewRatio,
      double m) {
    int groupCount = hiddenCapacity.length;
    double[] measuredLatency = new double[groupCount];
    Arrays.fill(measuredLatency, LATENCY_BASE_MS); // seed neutral: the router knows nothing yet
    double damping = 0.3; // under-relaxation models EWMA smoothing of measured latency and keeps the loop stable
    int maxRounds = 500;
    for (int round = 0; round < maxRounds; round++) {
      double[] shares = analyticShares(measuredLatency, evenUntilRatio, fullSkewRatio, m);
      double maxDelta = 0.0;
      for (int g = 0; g < groupCount; g++) {
        double served = shares[g] * load;
        double observed = environmentLatencyMs(served, hiddenCapacity[g]);
        double smoothed = damping * observed + (1.0 - damping) * measuredLatency[g];
        maxDelta = Math.max(maxDelta, Math.abs(smoothed - measuredLatency[g]));
        measuredLatency[g] = smoothed;
      }
      if (maxDelta < 0.05) {
        return measuredLatency;
      }
    }
    return null;
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
    final double[] latencyRatio;
    final double[] skew;
    final int[][] routed;
    final double[][] servedLatency;

    StagedRun(int[] stageLoads, double[] latencyRatio, double[] skew, int[][] routed, double[][] servedLatency) {
      this.stageLoads = stageLoads;
      this.latencyRatio = latencyRatio;
      this.skew = skew;
      this.routed = routed;
      this.servedLatency = servedLatency;
    }
  }

  /**
   * Drive the shared five-stage closed-loop harness for a given hidden per-group capacity vector: at each rising
   * RPS stage, iterate the environment/strategy loop to its fixed point (the only per-group signal the strategy
   * ever sees is the latency the environment produces), route real traffic through the strategy at the converged
   * latency, sanity-check that the routed distribution matches the analytic model, render the box table, and
   * return the per-stage outputs. The {@code hiddenCapacity} vector is <em>never</em> passed to the strategy.
   */
  private StagedRun runStagedScenario(String scenario, double[] hiddenCapacity) {
    return runStagedScenario(
        scenario,
        hiddenCapacity,
        HelixGroupWeightedLeastLoadedStrategy.DEFAULT_EVEN_UNTIL_LATENCY_RATIO,
        HelixGroupWeightedLeastLoadedStrategy.DEFAULT_FULL_SKEW_AT_LATENCY_RATIO,
        HelixGroupWeightedLeastLoadedStrategy.DEFAULT_INTERPOLATION_EXPONENT);
  }

  private StagedRun runStagedScenario(
      String scenario,
      double[] hiddenCapacity,
      double evenUntilRatio,
      double fullSkewRatio,
      double m) {
    int groupCount = hiddenCapacity.length;
    int[][] routedByStage = new int[STAGED_LOADS.length][];
    double[][] latencyByStage = new double[STAGED_LOADS.length][];
    double[] ratioByStage = new double[STAGED_LOADS.length];
    double[] skewByStage = new double[STAGED_LOADS.length];
    List<String[]> tableRows = new ArrayList<>();
    long nextRequestId = 0;

    for (int stage = 0; stage < STAGED_LOADS.length; stage++) {
      int load = STAGED_LOADS[stage];

      // The router's only per-group signal: the latency the environment produces at this load's fixed point.
      double[] measuredLatency = closedLoopLatency(hiddenCapacity, load, evenUntilRatio, fullSkewRatio, m);
      Assert.assertNotNull(measuredLatency, scenario + " stage " + stage + " closed loop did not converge");
      double ratio = latencyRatio(measuredLatency);
      double skew = skewFor(ratio, evenUntilRatio, fullSkewRatio, m);

      int[] routed = routeAndFinish(
          weightedStrategy(measuredLatency, evenUntilRatio, fullSkewRatio, m, new Random(SEED)),
          groupCount,
          nextRequestId,
          load);
      nextRequestId += load;

      double[] shares = new double[groupCount];
      double[] servedLatency = new double[groupCount];
      double weightedLatencySum = 0.0;
      for (int g = 0; g < groupCount; g++) {
        shares[g] = routed[g] / (double) load;
        // The latency this stage's routing actually produces, given each group's hidden capacity (a consequence
        // the strategy never reads; reported to show the served latencies converging vs even routing).
        servedLatency[g] = environmentLatencyMs(routed[g], hiddenCapacity[g]);
        weightedLatencySum += routed[g] * servedLatency[g];
      }
      double totalAvgLatency = weightedLatencySum / load;
      int requestSpread = maxOf(routed) - minOf(routed);
      double requestSpreadPct = 100.0 * requestSpread / load;

      // Sanity: the real strategy's routed distribution matches the analytic model for this latency + knobs.
      double[] expected = analyticShares(measuredLatency, evenUntilRatio, fullSkewRatio, m);
      for (int g = 0; g < groupCount; g++) {
        Assert.assertTrue(
            Math.abs(shares[g] - expected[g]) < 0.02,
            scenario + " stage " + stage + " group " + g + " share=" + shares[g] + " should match model "
                + expected[g]);
      }

      tableRows.add(
          new String[] { String.valueOf(stage), String.valueOf(load), String.format("%.2f", ratio),
              String.format("%.2f", skew), Arrays.toString(routed), join(shares, 100.0, "%.1f"),
              String.format("%d (%.1f%%)", requestSpread, requestSpreadPct), join(servedLatency, 1.0, "%.1f"),
              String.format("%.1f", totalAvgLatency) });

      routedByStage[stage] = routed;
      latencyByStage[stage] = servedLatency;
      ratioByStage[stage] = ratio;
      skewByStage[stage] = skew;
    }

    logTable(
        scenario + " (hidden capacity "
            + Arrays.toString(Arrays.stream(hiddenCapacity).mapToLong(Math::round).toArray()) + ", never read)",
        new String[] { "stage", "load (RPS)", "lat ratio", "skew", "routed (absolute)", "shares %",
            "req spread (max-min)", "avg latency / group (ms)", "total avg (ms)" },
        tableRows);

    return new StagedRun(STAGED_LOADS, ratioByStage, skewByStage, routedByStage, latencyByStage);
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
   * unused group"), fewer than two groups are measured so there is no spread to act on: every group is treated
   * as neutral and routing stays even. This is the safe default before any latency has been observed.
   */
  @Test
  public void testUnmeasuredGroupsStayEven() {
    int groupCount = 4;
    double evenShare = 1.0 / groupCount;
    double[] latency = { -1.0, -1.0, -1.0, -1.0 }; // nothing measured yet

    int[] routed = routeAndFinish(
        weightedStrategy(
            latency,
            HelixGroupWeightedLeastLoadedStrategy.DEFAULT_INTERPOLATION_EXPONENT,
            new Random(SEED)),
        groupCount,
        0,
        60000);

    LOGGER.info("No latency measured yet -> routed {} (should be ~even)", Arrays.toString(routed));
    for (int g = 0; g < groupCount; g++) {
      double share = routed[g] / 60000.0;
      Assert.assertTrue(
          Math.abs(share - evenShare) < 0.02,
          "Unmeasured groups must stay even; group " + g + " share=" + share);
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
    // Groups 0 and 1 measured (one fast, one slow, ratio 3.0 -> full skew); group 2 has no data yet.
    double[] latency = { 20.0, 60.0, -1.0 };
    double evenShare = 1.0 / groupCount;

    int[] routed = routeAndFinish(
        weightedStrategy(
            latency,
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
   * The probe floor keeps even a very slow group alive at full skew: it always retains at least
   * {@link HelixGroupWeightedLeastLoadedStrategy#PROBE_FLOOR_FRACTION} of the even share, so the router keeps
   * observing its latency and the signal stays live and self-correcting.
   */
  @Test
  public void testProbeFloorKeepsSlowGroupAlive() {
    int groupCount = 3;
    // Group 1 is two orders of magnitude slower; without a floor its share would collapse toward zero at full skew.
    double[] latency = { 20.0, 4000.0, 20.0 };
    int requestCount = 60000;
    double evenShare = 1.0 / groupCount;
    double floor = HelixGroupWeightedLeastLoadedStrategy.PROBE_FLOOR_FRACTION * evenShare;

    int[] routed = routeAndFinish(
        weightedStrategy(
            latency,
            HelixGroupWeightedLeastLoadedStrategy.DEFAULT_INTERPOLATION_EXPONENT,
            new Random(SEED)),
        groupCount,
        0,
        requestCount);

    double slowShare = routed[1] / (double) requestCount;
    LOGGER.info(
        "Very slow group at full skew -> routed {} (slow group should retain >= probe floor)",
        Arrays.toString(routed));
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
        HelixGroupWeightedLeastLoadedStrategy.DEFAULT_INTERPOLATION_EXPONENT,
        new Random(SEED));
    strategy.selectGroup(7, 3);
    Assert.assertThrows(VeniceException.class, () -> strategy.selectGroup(7, 3));
  }

  /** Failure path: the latency-ratio knobs must satisfy {@code 1 <= evenUntil < fullSkew} or the ctor rejects them. */
  @Test
  public void testInvalidRatioKnobsThrow() {
    Random random = new Random(SEED);
    // evenUntil below 1.0 is meaningless (a ratio is always >= 1).
    Assert.assertThrows(
        VeniceException.class,
        () -> weightedStrategy(new double[] { 25.0, 25.0 }, 0.9, 2.0, 1.0, random));
    // evenUntil must be strictly less than fullSkew.
    Assert.assertThrows(
        VeniceException.class,
        () -> weightedStrategy(new double[] { 25.0, 25.0 }, 2.0, 2.0, 1.0, random));
    Assert.assertThrows(
        VeniceException.class,
        () -> weightedStrategy(new double[] { 25.0, 25.0 }, 2.5, 2.0, 1.0, random));
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
   * <p>The new strategy still reads latency, but a 1.18x spread is <em>inside</em> its stay-even ratio (1.2x
   * default), so it treats the spread as noise and routes evenly, holding each group near its fair share and
   * preserving quota. The two strategies are compared on the same latency vector to show the fix removes the
   * over-concentration.
   */
  @Test
  public void testBaselineOldStrategyOverConcentratesVersusNewStrategy() {
    int groupCount = 5;
    // Real per-group average latency (ms) observed in production; group 4 is fastest. Spread 23.84/20.25 = 1.18x.
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

    // --- Fix: new weighted strategy on the same latency vector; the 1.18x spread is inside the stay-even ratio. ---
    HelixGroupWeightedLeastLoadedStrategy newStrategy = weightedStrategy(
        measured,
        HelixGroupWeightedLeastLoadedStrategy.DEFAULT_INTERPOLATION_EXPONENT,
        new Random(SEED));
    int[] newRouted = routeAndFinish(newStrategy, groupCount, 0, requestCount);

    double oldFastShare = oldRouted[fastGroup] / (double) requestCount;
    double newFastShare = newRouted[fastGroup] / (double) requestCount;

    LOGGER.info(
        "Baseline reproduction on production-observed latency vector {} (1.18x spread, ratio {}, {}ms jitter):",
        Arrays.toString(measured),
        String.format("%.2f", latencyRatio(measured)),
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

    // The fix removes that over-concentration: the 1.18x spread is inside the stay-even ratio, so the new
    // strategy spreads evenly and the (formerly hottest) group stays near its fair share and no group is starved.
    Assert.assertTrue(
        newFastShare < oldFastShare - 0.1,
        "New strategy must materially reduce the over-concentration; old=" + oldFastShare + " new=" + newFastShare);
    Assert.assertTrue(
        Math.abs(newFastShare - evenShare) < 0.02,
        "New strategy should keep every group near an even share within the stay-even ratio; share=" + newFastShare);
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
