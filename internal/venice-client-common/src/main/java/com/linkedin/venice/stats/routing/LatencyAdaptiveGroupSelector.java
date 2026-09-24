package com.linkedin.venice.stats.routing;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;
import java.util.function.IntToDoubleFunction;


/**
 * Latency-adaptive weighted group selection, shared by the router ({@code HelixGroupLatencyAdaptiveStrategy}) and the
 * fast client ({@code HelixLatencyAdaptiveGroupRoutingStrategy}). Given each group's measured average latency it picks
 * a group by a weighted-random draw whose skew adapts to how far apart the groups' latencies are: while every group is
 * fast it routes evenly, and as one group's latency pulls ahead it steers share onto the faster groups.
 *
 * <p>Measured latency is the <em>only</em> signal - it tells the selector both <em>which</em> group is fast and
 * <em>how much</em> to skew:
 *
 * <pre>
 *   strength(g) = 1 / max(latency(g), MIN_LATENCY_MS)                     // fast (low-latency) group => stronger
 *   ratio       = max(latency) / min(latency)   over measured groups      // 1.0 == perfectly even latency
 *   skew        = 0                                          if ratio &lt;= evenUntilLatencyRatio  // stay-even knob
 *                 1                                          if ratio &gt;= fullSkewAtLatencyRatio  // full-skew knob
 *                 ((ratio - evenUntil) / (fullSkew - evenUntil))^m  otherwise                     // ramp between
 *   share(g)    = (1 - skew) * (1 / G)  +  skew * strength(g) / sum(strength)
 *   share(g)    = max(share(g), PROBE_FLOOR_FRACTION * (1 / G))           // never fully starve a group
 * </pre>
 *
 * <p>A group that has not been measured yet ({@code latency <= 0}) is treated as neutral (average strength) and is
 * excluded from the spread, so it is neither flooded nor starved before it has data. Fewer than two measured groups
 * means there is no spread to act on, so routing stays even. The probe floor keeps every candidate group observable so
 * the latency signal stays live and the loop self-corrects.
 *
 * <p>This class holds no per-request state and is safe to share across threads as long as the supplied
 * {@code latencyProvider} and {@code randomSupplier} are; callers own any in-flight accounting.
 */
public class LatencyAdaptiveGroupSelector {
  public static final int MAX_ALLOWED_GROUP = 100;

  /**
   * Default stay-even threshold, expressed as a latency spread ratio (slowest / fastest measured group): while the
   * spread is at or below this factor the groups are considered balanced and routing stays fully even (skew
   * {@code == 0}). {@code 1.2} means "stay even until the slowest group is more than 20% slower than the fastest".
   */
  public static final double DEFAULT_EVEN_UNTIL_LATENCY_RATIO = 1.2;

  /**
   * Default full-skew threshold, expressed as a latency spread ratio: at (and above) this factor the routing reaches
   * its full latency-proportional split (skew {@code == 1}). Between {@link #DEFAULT_EVEN_UNTIL_LATENCY_RATIO} and this
   * the skew ramps from 0 to 1 as the spread widens. {@code 2.0} means "reach full skew once the slowest group is at
   * least twice the fastest".
   */
  public static final double DEFAULT_FULL_SKEW_AT_LATENCY_RATIO = 2.0;

  /**
   * Default in-band ramp exponent {@code m}: shapes the skew ramp between the stay-even and full-skew thresholds.
   * {@code 1.0} is a linear ramp; values &gt; 1 keep the ramp gentle just past the stay-even threshold and steepen it
   * near the full-skew threshold. It only affects the transition band.
   */
  public static final double DEFAULT_INTERPOLATION_EXPONENT = 1.0;

  /**
   * Lower bound applied to a group's measured latency before inverting it into a strength, so a group reporting a
   * near-zero latency cannot be assigned an unbounded strength (and thus flood-routed).
   */
  public static final double MIN_LATENCY_MS = 1.0;

  /**
   * The minimum share every candidate group retains, expressed as a fraction of the even share {@code 1 / G}. It keeps
   * a slow group from being fully starved at high utilization so the caller keeps observing its latency and the signal
   * stays live and self-correcting.
   */
  public static final double PROBE_FLOOR_FRACTION = 0.05;

  private final IntToDoubleFunction latencyProvider;
  private final double evenUntilLatencyRatio;
  private final double fullSkewAtLatencyRatio;
  private final double interpolationExponent;
  private final DoubleSupplier randomSupplier;

  /**
   * Convenience constructor using the default stay-even / full-skew / ramp knobs and {@link ThreadLocalRandom} for the
   * weighted draw.
   *
   * @param latencyProvider maps a group id to its measured average response time in milliseconds; a non-positive value
   *                        means the group has not been measured yet.
   */
  public LatencyAdaptiveGroupSelector(IntToDoubleFunction latencyProvider) {
    this(
        latencyProvider,
        DEFAULT_EVEN_UNTIL_LATENCY_RATIO,
        DEFAULT_FULL_SKEW_AT_LATENCY_RATIO,
        DEFAULT_INTERPOLATION_EXPONENT,
        () -> ThreadLocalRandom.current().nextDouble());
  }

  /**
   * @param latencyProvider        maps a group id to its measured average response time in milliseconds; a non-positive
   *                              value means the group has not been measured yet and is treated as neutral (average
   *                              strength) and excluded from the spread.
   * @param evenUntilLatencyRatio  stay-even threshold as a latency spread ratio (slowest / fastest): routing stays
   *                              fully even while the spread is at or below this factor. Must be {@code >= 1} and
   *                              strictly less than {@code fullSkewAtLatencyRatio}.
   * @param fullSkewAtLatencyRatio full-skew threshold as a latency spread ratio: routing reaches its full
   *                              latency-proportional split at or above this factor. Must be strictly greater than
   *                              {@code evenUntilLatencyRatio}.
   * @param interpolationExponent  the {@code m} exponent shaping the skew ramp between the two thresholds ({@code 1.0}
   *                              = linear). Must be strictly greater than {@code 0}.
   * @param randomSupplier         supplies a uniform random double in [0, 1); injectable so tests can be deterministic.
   */
  public LatencyAdaptiveGroupSelector(
      IntToDoubleFunction latencyProvider,
      double evenUntilLatencyRatio,
      double fullSkewAtLatencyRatio,
      double interpolationExponent,
      DoubleSupplier randomSupplier) {
    if (!(evenUntilLatencyRatio >= 1.0 && evenUntilLatencyRatio < fullSkewAtLatencyRatio)) {
      throw new VeniceException(
          "Require 1 <= evenUntilLatencyRatio < fullSkewAtLatencyRatio, but received evenUntilLatencyRatio="
              + evenUntilLatencyRatio + ", fullSkewAtLatencyRatio=" + fullSkewAtLatencyRatio);
    }
    if (!(interpolationExponent > 0.0)) {
      throw new VeniceException(
          "Require interpolationExponent > 0 (a non-positive exponent would push skew outside [0, 1] and distort "
              + "the share past full skew), but received interpolationExponent=" + interpolationExponent);
    }
    this.latencyProvider = latencyProvider;
    this.evenUntilLatencyRatio = evenUntilLatencyRatio;
    this.fullSkewAtLatencyRatio = fullSkewAtLatencyRatio;
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
   * Weighted-random reservoir selection across the groups, optionally excluding one group. Each candidate group is
   * adopted with probability {@code share(g) / cumulativeShare}, yielding a final selection probability proportional to
   * {@code share(g)} in a single pass. The scan starts at {@code startGroupId} purely to avoid biasing toward group 0;
   * it does not affect the resulting distribution.
   *
   * @param groupCount      the number of groups; must be in {@code [1, MAX_ALLOWED_GROUP]}.
   * @param startGroupId    the group id the scan starts from (typically {@code requestId % groupCount}).
   * @param excludedGroupId a group id to exclude from selection and from the latency spread (e.g. the group already
   *                        tried on the original request, so a retry lands elsewhere); pass {@code -1} to exclude none.
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
   * The skew factor in {@code [0, 1]} derived from the current latency spread across measured, non-excluded groups.
   */
  private double latencySkew(int groupCount, int excludedGroupId) {
    double minLatency = Double.MAX_VALUE;
    double maxLatency = 0.0;
    int measured = 0;
    for (int g = 0; g < groupCount; ++g) {
      if (g == excludedGroupId) {
        continue;
      }
      double latency = latencyProvider.applyAsDouble(g);
      if (latency <= 0.0) {
        continue;
      }
      double clamped = Math.max(latency, MIN_LATENCY_MS);
      minLatency = Math.min(minLatency, clamped);
      maxLatency = Math.max(maxLatency, clamped);
      ++measured;
    }
    if (measured < 2) {
      return 0.0;
    }
    double ratio = maxLatency / minLatency;
    if (ratio <= evenUntilLatencyRatio) {
      return 0.0;
    }
    if (ratio >= fullSkewAtLatencyRatio) {
      return 1.0;
    }
    double position = (ratio - evenUntilLatencyRatio) / (fullSkewAtLatencyRatio - evenUntilLatencyRatio);
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
