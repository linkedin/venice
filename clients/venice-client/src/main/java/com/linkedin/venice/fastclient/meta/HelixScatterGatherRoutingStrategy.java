package com.linkedin.venice.fastclient.meta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;


/**
 * This strategy will assign an ordering of helix groups via round-robin for even distribution. The fanout in multi-get
 * will be performed using instances belonging to the assigned groups. If no instance belonging to the selected group is
 * found for a given partition, the instance with the next group in the assigned ordering will be used
 */
public class HelixScatterGatherRoutingStrategy extends AbstractClientRoutingStrategy {
  private AtomicReference<HelixGroupInfo> helixGroupInfoAtomicReference = new AtomicReference<>();
  private final InstanceHealthMonitor instanceHealthMonitor;

  public HelixScatterGatherRoutingStrategy(InstanceHealthMonitor instanceHealthMonitor) {
    this.instanceHealthMonitor = instanceHealthMonitor;
    helixGroupInfoAtomicReference.set(new HelixGroupInfo(Collections.emptyMap()));
  }

  @Override
  public List<String> getReplicas(long requestId, List<String> replicas, int requiredReplicaCount) {
    HelixGroupInfo helixGroupInfo = helixGroupInfoAtomicReference.get();
    if (replicas.isEmpty() || helixGroupInfo.getHelixGroupInfoMap().isEmpty()) {
      return Collections.emptyList();
    }
    // select replicas from the selected group, going down the groups if more replicas are needed
    int groupCnt = helixGroupInfo.getGroupIds().size();
    int startPos = (int) (requestId % groupCnt);
    List<String> selectedReplicas = new ArrayList<>();
    for (int i = 0; i < groupCnt; i++) {
      int groupId = helixGroupInfo.getGroupIds().get((i + startPos) % groupCnt);
      for (String replica: replicas) {
        if (selectedReplicas.size() == requiredReplicaCount) {
          return selectedReplicas;
        }
        if (helixGroupInfo.getHelixGroupInfoMap().get(replica) == groupId
            && !instanceHealthMonitor.isInstanceBlocked(replica) && instanceHealthMonitor.isInstanceHealthy(replica)) {
          selectedReplicas.add(replica);
        }
      }
    }

    return selectedReplicas;
  }

  @Override
  public void updateHelixGroupInfo(Map<String, Integer> instanceToHelixGroupIdMap) {
    helixGroupInfoAtomicReference.set(new HelixGroupInfo(Collections.unmodifiableMap(instanceToHelixGroupIdMap)));
  }
}
