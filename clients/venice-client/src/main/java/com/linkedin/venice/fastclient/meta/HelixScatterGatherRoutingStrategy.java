package com.linkedin.venice.fastclient.meta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * This strategy will assign an ordering of helix groups via round-robin for even distribution. The fanout in multi-get
 * will be performed using instances belonging to the assigned groups. If no instance belonging to the selected group is
 * found for a given partition, the instance with the next group in the assigned ordering will be used
 */
public class HelixScatterGatherRoutingStrategy implements ClientRoutingStrategy {
  private final Map<CharSequence, Integer> helixGroupInfo;

  public HelixScatterGatherRoutingStrategy(Map<CharSequence, Integer> helixGroupInfo) {
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
    List<Integer> shuffledGroupIds = shuffleGroupIds(groupToFilteredReplicas.keySet());

    // select replicas from the selected group, going down the ordering if needed
    List<String> selectedReplicas = new ArrayList<>();
    for (int groupId: shuffledGroupIds) {
      for (String replica: groupToFilteredReplicas.get(groupId)) {
        if (selectedReplicas.size() == requiredReplicaCount) {
          return selectedReplicas;
        }
        selectedReplicas.add(replica);
      }
    }

    return selectedReplicas;
  }

  /**
   * Select a group ordering of the group IDs by round-robin
   * @param groupIds
   * @return list of groups in random permutation
   */
  public List<Integer> shuffleGroupIds(Set<Integer> groupIds) {
    List<Integer> shuffledGroupIds = new ArrayList<>(groupIds);
    Collections.shuffle(shuffledGroupIds);

    return shuffledGroupIds;
  }
}
