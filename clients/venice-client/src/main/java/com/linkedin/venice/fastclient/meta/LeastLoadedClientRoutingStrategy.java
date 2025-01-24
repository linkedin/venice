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
public class LeastLoadedClientRoutingStrategy extends AbstractClientRoutingStrategy {
  private final InstanceHealthMonitor instanceHealthMonitor;

  public LeastLoadedClientRoutingStrategy(InstanceHealthMonitor instanceHealthMonitor) {
    this.instanceHealthMonitor = instanceHealthMonitor;
  }

  @Override
  public List<String> getReplicas(long ignored, List<String> replicas, int requiredReplicaCount) {
    if (replicas.isEmpty()) {
      return Collections.emptyList();
    }
    List<String> availReplicas = new ArrayList<>();
    /**
     * For even distribution, we need to shuffle the replicas.
     */
    Collections.shuffle(replicas);
    for (String replica: replicas) {
      if (!instanceHealthMonitor.isInstanceBlocked(replica) && instanceHealthMonitor.isInstanceHealthy(replica)) {
        availReplicas.add(replica);
      }
    }
    availReplicas.sort(Comparator.comparingInt(instanceHealthMonitor::getPendingRequestCounter));

    if (requiredReplicaCount < availReplicas.size()) {
      List<String> selectedReplicas = new ArrayList<>();

      for (int i = 0; i < requiredReplicaCount; ++i) {
        String currentReplica = availReplicas.get(i);
        selectedReplicas.add(currentReplica);
      }

      return selectedReplicas;
    } else {
      return availReplicas;
    }
  }
}
