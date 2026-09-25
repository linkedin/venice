package com.linkedin.venice.stats.routing;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;
import java.util.function.IntToDoubleFunction;


/**
 * Latency-adaptive weighted group selection, shared by the router and fast-client routing strategies. Given each
 * group's measured average latency it picks a group by a weighted-random draw whose skew adapts to the slowest group's
 * latency: while every group is fast it routes evenly, and as the slowest group's latency climbs it steers share onto
 * the faster groups. The two thresholds are absolute latencies in milliseconds so they can be set against a
 * response-time SLA. Measured latency is the only signal - it determines both which group is fast and how much to skew:
 *
 * <pre>
 *   strength(g) = 1 / max(latency(g), MIN_LATENCY_MS)                     // faster group => stronger
 *   slowest     = max(latency)                  over measured groups (ms)
 *   skew        = 0                                          if slowest &lt;= evenUntilLatencyMs
 *                 1                                          if slowest &gt;= fullSkewAtLatencyMs
 *                 ((slowest - evenUntil) / (fullSkew - evenUntil))^m  otherwise
 *   share(g)    = (1 - skew) * (1 / G)  +  skew * strength(g) / sum(strength)
 *   share(g)    = max(share(g), PROBE_FLOOR_FRACTION * (1 / G))           // never fully starve a group
 * </pre>
 *
 * <p>Because share still weighs by {@code strength(g)}, groups that are all slow but equal in latency get equal
 * strength and routing stays even even at full skew - the skew only steers when a genuinely faster group exists. A
 * group not yet measured ({@code latency <= 0}) is treated as neutral (average strength) and excluded from the skew
 * calculation. The probe floor keeps every group observable so the latency signal stays live and the loop
 * self-corrects.
 *
 * <p>Holds no per-request state; thread-safe as long as the supplied {@code latencyProvider} and {@code randomSupplier}
 * are. Callers own any in-flight accounting.
 */
public class LatencyAdaptiveGroupSelector {
  public static final int MAX_ALLOWED_GROUP = 100;

  /**
   * Default stay-even threshold, as an absolute latency in milliseconds: routing stays fully even (skew {@code == 0})
   * while the slowest measured group's average latency is at or below this. Set it against the read-path SLA.
   */
  public static final double DEFAULT_EVEN_UNTIL_LATENCY_MS = 10.0;

  /**
   * Default full-skew threshold, as an absolute latency in milliseconds: routing reaches its full latency-proportional
   * split (skew {@code == 1}) once the slowest measured group's average latency is at or above this. Between
   * {@link #DEFAULT_EVEN_UNTIL_LATENCY_MS} and this the skew ramps 0 to 1.
   */
  public static final double DEFAULT_FULL_SKEW_AT_LATENCY_MS = 30.0;

  /**
   * Default in-band ramp exponent {@code m}, shaping the skew ramp between the two thresholds: {@code 1.0} is linear and
   * {@code > 1} steepens toward the full-skew threshold. Affects the transition band only.
   */
  public static final double DEFAULT_INTERPOLATION_EXPONENT = 1.0;

  /**
   * Lower bound on a group's latency before inverting it into a strength, so a near-zero latency cannot yield an
   * unbounded strength.
   */
  public static final double MIN_LATENCY_MS = 1.0;

  /**
   * Minimum share every group retains, as a fraction of the even share {@code 1 / G}, so a slow group is never fully
   * starved and its latency stays observable.
   */
  public static final double PROBE_FLOOR_FRACTION = 0.05;

  private final IntToDoubleFunction latencyProvider;
  private final double evenUntilLatencyMs;
  private final double fullSkewAtLatencyMs;
  private final double interpolationExponent;
  private final DoubleSupplier randomSupplier;

  /**
   * Uses the default knobs and {@link ThreadLocalRandom} for the weighted draw.
   *
   * @param latencyProvider maps a group id to its measured average response time in ms; non-positive means not yet
   *                        measured.
   */
  public LatencyAdaptiveGroupSelector(IntToDoubleFunction latencyProvider) {
    this(
        latencyProvider,
        DEFAULT_EVEN_UNTIL_LATENCY_MS,
        DEFAULT_FULL_SKEW_AT_LATENCY_MS,
        DEFAULT_INTERPOLATION_EXPONENT,
        () -> ThreadLocalRandom.current().nextDouble());
  }

  /**
   * @param latencyProvider        group id -> measured average response time in ms; non-positive means not yet measured
   *                              (treated as neutral and excluded from the skew calculation).
   * @param evenUntilLatencyMs     stay-even threshold in ms; while the slowest measured group is at or below it routing
   *                              stays even. Must be {@code >= 0} and {@code < fullSkewAtLatencyMs}.
   * @param fullSkewAtLatencyMs    full-skew threshold in ms; routing reaches its full latency-proportional split once
   *                              the slowest measured group is at or above it. Must be {@code > evenUntilLatencyMs}.
   * @param interpolationExponent  ramp exponent {@code m} between the thresholds ({@code 1.0} = linear). Must be
   *                              {@code > 0}.
   * @param randomSupplier         uniform random double in [0, 1); injectable for deterministic tests.
   */
  public LatencyAdaptiveGroupSelector(
      IntToDoubleFunction latencyProvider,
      double evenUntilLatencyMs,
      double fullSkewAtLatencyMs,
      double interpolationExponent,
      DoubleSupplier randomSupplier) {
    if (!(evenUntilLatencyMs >= 0.0 && evenUntilLatencyMs < fullSkewAtLatencyMs)) {
      throw new VeniceException(
          "Require 0 <= evenUntilLatencyMs < fullSkewAtLatencyMs, but received evenUntilLatencyMs=" + evenUntilLatencyMs
              + ", fullSkewAtLatencyMs=" + fullSkewAtLatencyMs);
    }
    if (!(interpolationExponent > 0.0)) {
      throw new VeniceException(
          "Require interpolationExponent > 0 (a non-positive exponent would push skew outside [0, 1] and distort "
              + "the share past full skew), but received interpolationExponent=" + interpolationExponent);
    }
    this.latencyProvider = latencyProvider;
    this.evenUntilLatencyMs = evenUntilLatencyMs;
    this.fullSkewAtLatencyMs = fullSkewAtLatencyMs;
    this.interpolationExponent = interpolationExponent;
    this.randomSupplier = randomSupplier;
  }

  /**
   * Weighted-random selection across all groups. Equivalent to {@link #selectGroup(int, int, int)} with no excluded
   * group.
   */
  public int selectGroup(int groupCount, int startGroupId) {
    return selectGroup(groupCount, startGroupId, -1);
  }

  /**
   * Weighted-random reservoir selection across the groups, optionally excluding one. Each candidate is adopted with
   * probability {@code share(g) / cumulativeShare}, so the final selection probability is proportional to
   * {@code share(g)} in a single pass. The scan starts at {@code startGroupId} only to avoid biasing toward group 0.
   *
   * @param groupCount      number of groups; must be in {@code [1, MAX_ALLOWED_GROUP]}.
   * @param startGroupId    group id the scan starts from (typically {@code requestId % groupCount}).
   * @param excludedGroupId group to exclude from selection and from the skew calculation (e.g. the group already tried,
   *                        so a retry lands elsewhere); {@code -1} excludes none.
   */
  public int selectGroup(int groupCount, int startGroupId, int excludedGroupId) {
    if (groupCount > MAX_ALLOWED_GROUP || groupCount <= 0) {
      throw new VeniceException(
          "The valid group num must fall into this range: [1, " + MAX_ALLOWED_GROUP + "], but received: " + groupCount);
    }
    double skew = latencySkew(groupCount, excludedGroupId);
    double evenShare = 1.0 / groupCount;
    double floor = PROBE_FLOOR_FRACTION * evenShare;
    double neutralStrength = neutralStrength(groupCount, excludedGroupId);
    double totalStrength = totalStrength(groupCount, neutralStrength, excludedGroupId);

    double cumulativeShare = 0.0;
    int selectedGroup = -1;
    for (int i = 0; i < groupCount; ++i) {
      int currentGroup = (i + startGroupId) % groupCount;
      if (currentGroup == excludedGroupId) {
        continue;
      }
      double share = shareForGroup(currentGroup, evenShare, floor, skew, neutralStrength, totalStrength);
      if (share <= 0.0) {
        continue;
      }
      cumulativeShare += share;
      if (randomSupplier.getAsDouble() * cumulativeShare < share) {
        selectedGroup = currentGroup;
      }
    }
    if (selectedGroup >= 0) {
      return selectedGroup;
    }
    // Every candidate collapsed to zero (should not happen given the probe floor) or the only group was excluded: fall
    // back to a group that still gets the request routed somewhere rather than dropped.
    return startGroupId == excludedGroupId ? (startGroupId + 1) % groupCount : startGroupId;
  }

  /**
   * The skew factor in {@code [0, 1]} derived from the slowest measured, non-excluded group's latency (ms). Groups not
   * yet measured are ignored here; with no measured group the skew is 0 (even routing).
   */
  private double latencySkew(int groupCount, int excludedGroupId) {
    double slowestLatencyMs = 0.0;
    for (int g = 0; g < groupCount; ++g) {
      if (g == excludedGroupId) {
        continue;
      }
      double latency = latencyProvider.applyAsDouble(g);
      if (latency <= 0.0) {
        continue;
      }
      slowestLatencyMs = Math.max(slowestLatencyMs, Math.max(latency, MIN_LATENCY_MS));
    }
    if (slowestLatencyMs <= evenUntilLatencyMs) {
      return 0.0;
    }
    if (slowestLatencyMs >= fullSkewAtLatencyMs) {
      return 1.0;
    }
    double position = (slowestLatencyMs - evenUntilLatencyMs) / (fullSkewAtLatencyMs - evenUntilLatencyMs);
    return interpolationExponent == 1.0 ? position : Math.pow(position, interpolationExponent);
  }

  /**
   * The target share for a group:
   * {@code max( (1 - skew) * evenShare + skew * strength(g) / totalStrength, floor )}. The floor keeps a slow group
   * from being fully starved so its latency stays observable.
   */
  private double shareForGroup(
      int groupId,
      double evenShare,
      double floor,
      double skew,
      double neutralStrength,
      double totalStrength) {
    double strengthShare = totalStrength > 0 ? strength(groupId, neutralStrength) / totalStrength : evenShare;
    double share = (1.0 - skew) * evenShare + skew * strengthShare;
    return Math.max(share, floor);
  }

  /**
   * A group's inferred strength: the reciprocal of its measured latency (faster => stronger). A group that has not been
   * measured yet (non-positive latency) is treated as neutral so it is neither flooded nor starved before there is
   * data.
   */
  private double strength(int groupId, double neutralStrength) {
    double latency = latencyProvider.applyAsDouble(groupId);
    if (latency <= 0.0) {
      return neutralStrength;
    }
    return 1.0 / Math.max(latency, MIN_LATENCY_MS);
  }

  /** The mean strength of the already-measured, non-excluded groups, used as the strength of not-yet-measured groups. */
  private double neutralStrength(int groupCount, int excludedGroupId) {
    double sum = 0.0;
    int measured = 0;
    for (int g = 0; g < groupCount; ++g) {
      if (g == excludedGroupId) {
        continue;
      }
      double latency = latencyProvider.applyAsDouble(g);
      if (latency > 0.0) {
        sum += 1.0 / Math.max(latency, MIN_LATENCY_MS);
        ++measured;
      }
    }
    // No group measured yet: any positive constant works since every group then gets the same neutral strength, which
    // reduces the strength term to an even split.
    return measured > 0 ? sum / measured : 1.0;
  }

  private double totalStrength(int groupCount, double neutralStrength, int excludedGroupId) {
    double total = 0.0;
    for (int g = 0; g < groupCount; ++g) {
      if (g == excludedGroupId) {
        continue;
      }
      total += strength(g, neutralStrength);
    }
    return total;
  }
}
