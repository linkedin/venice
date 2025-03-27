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
  public String getReplicas(long requestId, int groupId, List<String> replicas) {
    if (replicas.isEmpty()) {
      return null;
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
    if (availReplicas.isEmpty()) {
      return null;
    }
    /**
     * TODO: maybe we can apply the response-waiting-time-based rather than pending request counter based least-loaded strategy here
     * since application QPS normally is much lower and pending request count can be very low.
     */
    availReplicas.sort(Comparator.comparingInt(instanceHealthMonitor::getPendingRequestCounter));
    return availReplicas.get(0);
  }
}
