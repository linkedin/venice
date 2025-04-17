package com.linkedin.venice.fastclient.meta;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.stats.routing.HelixGroupStats;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * This class is used to route requests to the least loaded group based on the response time collected in the past.
 * So far, it only tracks the response time of multi-key requests.
 */
public class HelixLeastLoadedGroupRoutingStrategy extends HelixGroupRoutingStrategy {
  /**
   * The following constant defines the threshold for selecting a group based on its average latency.
   * If the group avg latency is lower than the minimum latency of all groups by more than 50%, it will be considered for selection.
   * This is used to avoid routing to a group that has significantly higher latency than other groups.
   */
  private static final double AVG_LATENCY_SPECTRUM_FOR_GROUP_SELECTION = 1.5;

  public HelixLeastLoadedGroupRoutingStrategy(
      InstanceHealthMonitor instanceHealthMonitor,
      MetricsRepository metricsRepository,
      String storeName) {
    super(instanceHealthMonitor, metricsRepository, storeName);
  }

  HelixLeastLoadedGroupRoutingStrategy(InstanceHealthMonitor monitor, HelixGroupStats helixGroupStats) {
    super(monitor, helixGroupStats);
  }

  /**
   * Get the least loaded group based on the response time collected in the past.
   */
  @Override
  public int getHelixGroupId(long requestId, int groupIdForOriginalRequest) {
    int groupCnt = getGroupCount();
    if (groupCnt <= 0) {
      throw new VeniceClientException("Unexpected group count: " + groupCnt);
    }
    int startPos = (int) (requestId % groupCnt);
    double minLatency = Double.MAX_VALUE;
    Map<Integer, Double> groupLatencyMapping = new HashMap<>();
    for (int i = 0; i < groupCnt; ++i) {
      int tmpGroupId = (startPos + i) % groupCnt;
      if (tmpGroupId == groupIdForOriginalRequest) {
        // Skip the original request group to avoid routing back to the same group.
        continue;
      }
      double avgGroupLatency = helixGroupStats.getGroupResponseWaitingTimeAvg(tmpGroupId);
      if (avgGroupLatency <= 0) {
        // No datapoint, which means this group hasn't received any traffic so far, so just return it.
        return tmpGroupId;
      }
      groupLatencyMapping.put(tmpGroupId, avgGroupLatency);
      if (avgGroupLatency < minLatency) {
        minLatency = avgGroupLatency;
      }
    }
    // We will randomly pick up one group if the avg latency is not 50% higher than the minimum latency to avoid skewed
    // traffic
    // towards one group if there is a slight difference in latency.
    final List<Integer> candidateGroups = new ArrayList<>(groupCnt);
    final double minLatencyFinal = minLatency;
    groupLatencyMapping.forEach((groupIdTmp, avgLatency) -> {
      // Only consider groups that have a latency not more than 50% higher than the minimum latency
      if (avgLatency <= minLatencyFinal * AVG_LATENCY_SPECTRUM_FOR_GROUP_SELECTION) {
        candidateGroups.add(groupIdTmp);
      }
    });
    if (candidateGroups.isEmpty()) {
      throw new VeniceClientException("Can't find a valid group to route the request");
    }
    Collections.shuffle(candidateGroups);
    return candidateGroups.get(0);
  }

}
