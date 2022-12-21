package com.linkedin.venice.fastclient.meta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;


/**
 * This strategy is trying to route the request to the least-loaded instances.
 * 1. If the weight of every instance is same, it will fall back to the round-robin fashion.
 * 2. This strategy will skip any blocked instance.
 * 3. When any selected instance is marked as unhealthy, this strategy will try to back-fill with the healthy instances,
 *    and there are two purposes:
 *    a. The latency shouldn't be affected since this strategy will still try to send request to the required healthy instances.
 *    b. The unhealthy instance will still receive any requests, so we could mark it healthy once it is recovered.
 */
public class LeastLoadedClientRoutingStrategy implements ClientRoutingStrategy {
  private final InstanceHealthMonitor instanceHealthMonitor;

  public LeastLoadedClientRoutingStrategy(InstanceHealthMonitor instanceHealthMonitor) {
    this.instanceHealthMonitor = instanceHealthMonitor;
  }

  @Override
  public List<String> getReplicas(long requestId, List<String> replicas, int requiredReplicaCount) {
    if (replicas.isEmpty()) {
      return Collections.emptyList();
    }
    int replicaCnt = replicas.size();
    int startPos = (int) requestId % replicaCnt;
    List<String> availReplicas = new ArrayList<>();
    for (int i = 0; i < replicaCnt; ++i) {
      String replica = replicas.get((i + startPos) % replicaCnt);
      if (!instanceHealthMonitor.isInstanceBlocked(replica)) {
        availReplicas.add(replica);
      }
    }

    availReplicas.sort(Comparator.comparingInt(instanceHealthMonitor::getPendingRequestCounter));

    if (requiredReplicaCount < availReplicas.size()) {
      List<String> selectedReplicas = new ArrayList<>();
      /**
       * Check whether any unhealthy replica has been selected or not, if yes, try to add more healthy replicas.
       */
      int selectedUnhealthyReplicaCnt = 0;
      for (int i = 0; i < requiredReplicaCount; ++i) {
        String currentReplica = availReplicas.get(i);
        selectedReplicas.add(currentReplica);
        if (!instanceHealthMonitor.isInstanceHealthy(currentReplica)) {
          ++selectedUnhealthyReplicaCnt;
        }
      }
      if (selectedUnhealthyReplicaCnt > 0) {
        /**
         * If any unhealthy replica is selected, we will try to back-fill with the same number of healthy replicas.
         * With this way, we could achieve the following goals:
         * 1. The unhealthy replica will still receive some requests to bring it back once it is recovered.
         * 2. The request latency won't be affected since we are still trying to return the required healthy replicas as
         *    much as possible.
         */
        int backfillingHealthyReplicaCnt = 0;
        for (int i = requiredReplicaCount; i < availReplicas.size()
            && backfillingHealthyReplicaCnt < selectedUnhealthyReplicaCnt; ++i) {
          String currentReplica = availReplicas.get(i);
          if (instanceHealthMonitor.isInstanceHealthy(currentReplica)) {
            selectedReplicas.add(currentReplica);
            ++backfillingHealthyReplicaCnt;
          }
        }
      }

      return selectedReplicas;
    } else {
      return availReplicas;
    }
  }
}
