package com.linkedin.venice.fastclient.meta;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.stats.routing.HelixGroupStats;
import com.linkedin.venice.stats.routing.LatencyAdaptiveGroupSelector;
import io.tehuti.metrics.MetricsRepository;
import java.util.concurrent.ThreadLocalRandom;


/**
 * Latency-adaptive Helix group routing strategy for the fast client. Instead of winner-take-all least-loaded selection
 * it does a latency-driven weighted-random draw across groups, sharing the selection math with the router via
 * {@link LatencyAdaptiveGroupSelector}.
 *
 * <p>Latency comes from {@link HelixGroupStats#getGroupResponseWaitingTimeAvg}, populated by
 * {@link HelixGroupRoutingStrategy#trackRequest} (multi-key, non-retry requests). A group with no datapoint yet is
 * treated as neutral so it is neither flooded nor starved before it has been measured.
 */
public class HelixLatencyAdaptiveGroupRoutingStrategy extends HelixGroupRoutingStrategy {
  private final LatencyAdaptiveGroupSelector selector;

  public HelixLatencyAdaptiveGroupRoutingStrategy(
      InstanceHealthMonitor instanceHealthMonitor,
      MetricsRepository metricsRepository,
      String storeName,
      double evenUntilLatencyMs,
      double fullSkewAtLatencyMs,
      double skewRampExponent) {
    super(instanceHealthMonitor, metricsRepository, storeName);
    this.selector = buildSelector(evenUntilLatencyMs, fullSkewAtLatencyMs, skewRampExponent);
  }

  /** Uses the default latency-adaptive knobs. */
  HelixLatencyAdaptiveGroupRoutingStrategy(InstanceHealthMonitor monitor, HelixGroupStats helixGroupStats) {
    this(
        monitor,
        helixGroupStats,
        LatencyAdaptiveGroupSelector.DEFAULT_EVEN_UNTIL_LATENCY_MS,
        LatencyAdaptiveGroupSelector.DEFAULT_FULL_SKEW_AT_LATENCY_MS,
        LatencyAdaptiveGroupSelector.DEFAULT_INTERPOLATION_EXPONENT);
  }

  HelixLatencyAdaptiveGroupRoutingStrategy(
      InstanceHealthMonitor monitor,
      HelixGroupStats helixGroupStats,
      double evenUntilLatencyMs,
      double fullSkewAtLatencyMs,
      double skewRampExponent) {
    super(monitor, helixGroupStats);
    this.selector = buildSelector(evenUntilLatencyMs, fullSkewAtLatencyMs, skewRampExponent);
  }

  private LatencyAdaptiveGroupSelector buildSelector(
      double evenUntilLatencyMs,
      double fullSkewAtLatencyMs,
      double skewRampExponent) {
    return new LatencyAdaptiveGroupSelector(
        helixGroupStats::getGroupResponseWaitingTimeAvg,
        evenUntilLatencyMs,
        fullSkewAtLatencyMs,
        skewRampExponent,
        () -> ThreadLocalRandom.current().nextDouble());
  }

  /**
   * Pick a group by a latency-adaptive weighted-random draw, excluding {@code groupIdForOriginalRequest} so a retry
   * lands on a different group than the one already tried ({@code -1} on the original request excludes nothing).
   */
  @Override
  public int getHelixGroupId(long requestId, int groupIdForOriginalRequest) {
    int groupCnt = getGroupCount();
    if (groupCnt <= 0) {
      throw new VeniceClientException("Unexpected group count: " + groupCnt);
    }
    int startPos = (int) (requestId % groupCnt);
    return selector.selectGroup(groupCnt, startPos, groupIdForOriginalRequest);
  }
}
