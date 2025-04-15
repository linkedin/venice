package com.linkedin.venice.fastclient.meta;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.fastclient.RequestContext;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.routing.HelixGroupStats;
import com.linkedin.venice.utils.LatencyUtils;
import io.tehuti.metrics.MetricsRepository;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;


/**
 * This strategy will assign an ordering of helix groups via round-robin for even distribution. The fanout in multi-get
 * will be performed using instances belonging to the assigned groups. If no instance belonging to the selected group is
 * found for a given partition, the instance with the next group in the assigned ordering will be used
 */
public class HelixGroupRoutingStrategy extends AbstractClientRoutingStrategy {
  protected AtomicReference<HelixGroupInfo> helixGroupInfoAtomicReference = new AtomicReference<>();
  protected final InstanceHealthMonitor instanceHealthMonitor;
  protected final HelixGroupStats helixGroupStats;

  public HelixGroupRoutingStrategy(
      InstanceHealthMonitor instanceHealthMonitor,
      MetricsRepository metricsRepository,
      String storeName) {
    this(instanceHealthMonitor, new HelixGroupStats(metricsRepository, storeName));
  }

  HelixGroupRoutingStrategy(InstanceHealthMonitor instanceHealthMonitor, HelixGroupStats helixGroupStats) {
    this.instanceHealthMonitor = instanceHealthMonitor;
    this.helixGroupInfoAtomicReference.set(new HelixGroupInfo(Collections.emptyMap()));
    this.helixGroupStats = helixGroupStats;
  }

  @Override
  public String getReplicas(long requestId, int groupId, List<String> replicas) {
    HelixGroupInfo helixGroupInfo = helixGroupInfoAtomicReference.get();
    if (replicas.isEmpty() || helixGroupInfo.getHelixGroupInfoMap().isEmpty()) {
      return null;
    }
    // select replicas from the selected group, going down the groups if more replicas are needed
    int groupCnt = helixGroupInfo.getGroupIds().size();
    List<Integer> groupIds = helixGroupInfo.getGroupIds();
    Map<String, Integer> instanceToGroupIdMapping = helixGroupInfo.getHelixGroupInfoMap();
    for (int i = 0; i < groupCnt; i++) {
      int tmpGroupId = groupIds.get((i + groupId) % groupCnt);
      for (String replica: replicas) {
        if (instanceToGroupIdMapping.get(replica) == tmpGroupId && instanceHealthMonitor.isRequestAllowed(replica)) {
          return replica;
        }
      }
    }

    return null;
  }

  public int getGroupCount() {
    HelixGroupInfo helixGroupInfo = helixGroupInfoAtomicReference.get();
    return helixGroupInfo.getGroupIds().size();
  }

  @Override
  public void updateHelixGroupInfo(Map<String, Integer> instanceToHelixGroupIdMap) {
    helixGroupInfoAtomicReference.set(new HelixGroupInfo(Collections.unmodifiableMap(instanceToHelixGroupIdMap)));
  }

  @Override
  public int getHelixGroupId(long requestId, int groupIdForOriginalRequest) {
    HelixGroupInfo helixGroupInfo = helixGroupInfoAtomicReference.get();
    if (helixGroupInfo.getHelixGroupInfoMap().isEmpty()) {
      throw new VeniceClientException("HelixGroupInfo is empty");
    }
    int newGroupId = (int) (requestId % helixGroupInfo.getGroupIds().size());
    if (newGroupId == groupIdForOriginalRequest) {
      newGroupId = (groupIdForOriginalRequest + 1) % helixGroupInfo.getGroupIds().size();
    }
    return newGroupId;
  }

  @Override
  public boolean trackRequest(RequestContext requestContext) {
    RequestType requestType = requestContext.getRequestType();
    if (requestType.equals(RequestType.SINGLE_GET)) {
      // So far, we mainly track multi-key requests here to monitor the group load.
      return false;
    }
    if (requestContext.isRetryRequest()) {
      // We don't track the retry requests since the retry request can be significantly smaller than
      // the original request, which will pollute the group load stats.
      return false;
    }
    int groupId = requestContext.getHelixGroupId();
    if (groupId < 0) {
      throw new VeniceClientException("Group ID is not set for request: " + requestContext.getRequestId());
    }
    helixGroupStats.recordGroupNum(getGroupCount());
    helixGroupStats.recordGroupRequest(groupId);
    // Track the latency per group
    long startTimeInNS = System.nanoTime();
    requestContext.getResultFuture().whenComplete((ignored1, ignored2) -> {
      helixGroupStats.recordGroupResponseWaitingTime(groupId, LatencyUtils.getElapsedTimeFromNSToMS(startTimeInNS));
    });
    return true;
  }
}
