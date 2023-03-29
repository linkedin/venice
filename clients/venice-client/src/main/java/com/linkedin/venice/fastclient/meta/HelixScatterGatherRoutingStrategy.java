package com.linkedin.venice.fastclient.meta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * This strategy will assign an ordering of helix groups via round-robin for even distribution. The fanout in multi-get
 * will be performed using instances belonging to the assigned groups. If no instance belonging to the selected group is
 * found for a given partition, the instance with the next group in the assigned ordering will be used
 */
public class HelixScatterGatherRoutingStrategy implements ClientRoutingStrategy {
  private Map<String, Integer> helixGroupInfo;

  public HelixScatterGatherRoutingStrategy(Map<String, Integer> helixGroupInfo) {
    this.helixGroupInfo = helixGroupInfo;
  }

  @Override
  public List<String> getReplicas(long requestId, List<String> replicas, int requiredReplicaCount) {
    if (replicas.isEmpty()) {
      return Collections.emptyList();
    }
    Map<Integer, List<String>> groupToFilteredReplicas = new HashMap<>();
    for (String replica: replicas) {
      groupToFilteredReplicas.computeIfAbsent(helixGroupInfo.get(replica), k -> new ArrayList<>()).add(replica);
    }
    List<Integer> groupIds = new ArrayList<>(groupToFilteredReplicas.keySet());

    // select replicas from the selected group, going down the groups if more replicas are needed
    int groupCnt = groupIds.size();
    int startPos = (int) requestId % groupCnt;
    List<String> selectedReplicas = new ArrayList<>();
    for (int i = 0; i < groupCnt; i++) {
      int groupId = groupIds.get((i + startPos) % groupCnt);
      for (String replica: groupToFilteredReplicas.get(groupId)) {
        if (selectedReplicas.size() == requiredReplicaCount) {
          return selectedReplicas;
        }
        selectedReplicas.add(replica);
      }
    }

    return selectedReplicas;
  }

  public void setHelixGroupInfo(Map<String, Integer> helixGroupInfo) {
    this.helixGroupInfo = helixGroupInfo;
  }
}
