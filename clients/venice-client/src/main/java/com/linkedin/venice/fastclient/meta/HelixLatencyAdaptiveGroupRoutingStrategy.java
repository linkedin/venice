package com.linkedin.venice.fastclient.meta;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.stats.routing.HelixGroupStats;
import com.linkedin.venice.stats.routing.LatencyAdaptiveGroupSelector;
import io.tehuti.metrics.MetricsRepository;


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
      String storeName) {
    super(instanceHealthMonitor, metricsRepository, storeName);
    this.selector = new LatencyAdaptiveGroupSelector(helixGroupStats::getGroupResponseWaitingTimeAvg);
  }

  HelixLatencyAdaptiveGroupRoutingStrategy(InstanceHealthMonitor monitor, HelixGroupStats helixGroupStats) {
    super(monitor, helixGroupStats);
    this.selector = new LatencyAdaptiveGroupSelector(helixGroupStats::getGroupResponseWaitingTimeAvg);
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
