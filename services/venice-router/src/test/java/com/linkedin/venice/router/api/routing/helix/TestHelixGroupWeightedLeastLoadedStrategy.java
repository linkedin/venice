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
      double interpolationExponent,
      Random random) {
    return new HelixGroupWeightedLeastLoadedStrategy(
        mockTimeoutProcessor(),
        TIMEOUT_MS,
        mock(HelixGroupStats.class),
        groupId -> latency[groupId],
        utilization,
        interpolationExponent,
        random::nextDouble);
  }

  /** Convenience overload for scenarios with a constant aggregate utilization. */
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
    int groupCount = latency.length;
    double even = 1.0 / groupCount;
    double floor = HelixGroupWeightedLeastLoadedStrategy.PROBE_FLOOR_FRACTION * even;
    double uPow = u <= 0 ? 0.0 : (u >= 1 ? 1.0 : Math.pow(u, m));

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
      shares[g] = Math.max((1.0 - uPow) * even + uPow * strengthShare, floor);
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
    int groupCount = 3;
    // Measured average latency per group (ms). Group 2 is consistently ~2x faster; strengths {1/40,1/40,1/20}
    // give latency-proportional shares 0.25 / 0.25 / 0.50 at full utilization.
    double[] latency = { 40.0, 40.0, 20.0 };
    int fastGroup = argMin(latency);
    // The implied full-utilization ceiling per group (its share of the total quota); used only for reporting and
    // the "never exceed ceiling" assertion -- it is NOT fed into the strategy.
    double totalQuota = 42000.0;
    double[] impliedCeiling = new double[groupCount];
    double[] saturationShares =
        analyticShares(latency, 1.0, HelixGroupWeightedLeastLoadedStrategy.DEFAULT_INTERPOLATION_EXPONENT);
    for (int g = 0; g < groupCount; g++) {
      impliedCeiling[g] = saturationShares[g] * totalQuota;
    }
    int[] stageLoads = { 10000, 20000, 30000, 38000, 42000 };
    double m = HelixGroupWeightedLeastLoadedStrategy.DEFAULT_INTERPOLATION_EXPONENT;
    double slack = 0.01 * totalQuota;

    double previousFastShare = -1.0;
    int[] previousRouted = null;
    double[] lastServedLatency = null;
    long nextRequestId = 0;

    LOGGER.info(
        "Five-stage latency model (measured latency {}, implied ceilings {}). "
            + "stage | load(RPS) | u | routed(abs) | shares%% | avg latency/group(ms) | total avg latency(ms)",
        Arrays.toString(latency),
        Arrays.toString(Arrays.stream(impliedCeiling).mapToLong(Math::round).toArray()));

    for (int stage = 0; stage < stageLoads.length; stage++) {
      int load = stageLoads[stage];
      double u = load / totalQuota;

      int[] routed = routeAndFinish(weightedStrategy(latency, u, m, new Random(SEED)), groupCount, nextRequestId, load);
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
      // Total avg latency = the latency an average request sees this stage (weighted by where traffic landed).
      double totalAvgLatency = weightedLatencySum / load;
      double fastShare = shares[fastGroup];

      LOGGER.info(
          String.format(
              "  %2d   |   %5d   | %.3f | %-22s | [%.1f, %.1f, %.1f] | [%.1f, %.1f, %.1f] | %.1f",
              stage,
              load,
              u,
              Arrays.toString(routed),
              100 * shares[0],
              100 * shares[1],
              100 * shares[2],
              servedLatency[0],
              servedLatency[1],
              servedLatency[2],
              totalAvgLatency));

      double[] expected = analyticShares(latency, u, m);
      for (int g = 0; g < groupCount; g++) {
        Assert.assertTrue(
            Math.abs(shares[g] - expected[g]) < 0.02,
            "Stage " + stage + " group " + g + " share=" + shares[g] + " should match model " + expected[g]);
        Assert.assertTrue(
            routed[g] <= impliedCeiling[g] + slack,
            "Group " + g + " routed=" + routed[g] + " must not exceed its implied ceiling " + impliedCeiling[g]);
      }
      // The faster group's share must not shrink as load grows.
      if (previousFastShare >= 0) {
        Assert.assertTrue(
            fastShare >= previousFastShare - 0.005,
            "Faster group share should grow (not shrink) as load rises; was " + previousFastShare + " now "
                + fastShare);
      }
      // Ali's health property: as RPS rises every group keeps getting MORE absolute traffic; the slower members
      // are never starved of throughput, they just take a shrinking share of a growing total.
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
      lastServedLatency = servedLatency;
    }

    // At the final stage every group is served at (not past) its implied ceiling -- the healthy saturation point.
    for (int g = 0; g < groupCount; g++) {
      Assert.assertTrue(
          Math.abs(previousRouted[g] - impliedCeiling[g]) < slack,
          "At full quota group " + g + " should be served at its ceiling " + impliedCeiling[g] + "; routed="
              + previousRouted[g]);
    }
    // ...and because per-group utilization is balanced at saturation, the served latencies converge instead of
    // one weak group becoming the SLO outlier.
    double minLatency = lastServedLatency[0];
    double topLatency = lastServedLatency[0];
    for (double l: lastServedLatency) {
      minLatency = Math.min(minLatency, l);
      topLatency = Math.max(topLatency, l);
    }
    Assert.assertTrue(
        topLatency - minLatency < 10.0,
        "At saturation group latencies should be balanced (no SLO outlier); latencies="
            + Arrays.toString(lastServedLatency));
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
