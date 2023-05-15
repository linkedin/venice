package com.linkedin.venice.fastclient.meta;

import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * This strategy will assign an ordering of helix groups via round-robin for even distribution. The fanout in multi-get
 * will be performed using instances belonging to the assigned groups. If no instance belonging to the selected group is
 * found for a given partition, the instance with the next group in the assigned ordering will be used
 */
public class HelixScatterGatherRoutingStrategy implements ClientRoutingStrategy {
  private Map<String, Integer> helixGroupInfo = new VeniceConcurrentHashMap<>();
  private List<Integer> groupIds = new ArrayList<>();
  private final InstanceHealthMonitor instanceHealthMonitor;

  public HelixScatterGatherRoutingStrategy(InstanceHealthMonitor instanceHealthMonitor) {
    this.instanceHealthMonitor = instanceHealthMonitor;
  }

  @Override
  public List<String> getReplicas(long requestId, List<String> replicas, int requiredReplicaCount) {
    if (replicas.isEmpty() || helixGroupInfo.isEmpty()) {
      return Collections.emptyList();
    }
    // select replicas from the selected group, going down the groups if more replicas are needed
    int groupCnt = groupIds.size();
    int startPos = (int) requestId % groupCnt;
    List<String> selectedReplicas = new ArrayList<>();
    for (int i = 0; i < groupCnt; i++) {
      int groupId = groupIds.get((i + startPos) % groupCnt);
      for (String replica: replicas) {
        if (selectedReplicas.size() == requiredReplicaCount) {
          return selectedReplicas;
        }
        if (helixGroupInfo.get(replica) == groupId && !instanceHealthMonitor.isInstanceBlocked(replica)) {
          selectedReplicas.add(replica);
        }
      }
    }

    return selectedReplicas;
  }

  public void updateHelixGroupInfo(Map<String, Integer> helixGroupInfo) {
    this.helixGroupInfo = helixGroupInfo;
    this.groupIds = helixGroupInfo.values().stream().distinct().sorted().collect(Collectors.toList());
  }
}
