package com.linkedin.venice.fastclient.meta;

import com.linkedin.restli.common.HttpStatus;
import com.linkedin.venice.fastclient.ClientConfig;
import com.linkedin.venice.utils.Utils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;


public abstract class AbstractStoreMetadata implements StoreMetadata {
  private final InstanceHealthMonitor instanceHealthMonitor;
  private final ClientRoutingStrategy routingStrategy;
  protected final String storeName;

  public AbstractStoreMetadata(ClientConfig clientConfig) {
    this.instanceHealthMonitor = new InstanceHealthMonitor(clientConfig);
    if (clientConfig.getClientRoutingStrategy() != null) {
      this.routingStrategy = clientConfig.getClientRoutingStrategy();
    } else {
      this.routingStrategy = new LeastLoadedClientRoutingStrategy(this.instanceHealthMonitor);
    }
    this.storeName = clientConfig.getStoreName();
  }

  @Override
  public String getStoreName() {
    return storeName;
  }

  @Override
  public int getPartitionId(int version, byte[] key) {
    return getPartitionId(version, ByteBuffer.wrap(key));
  }

  /**
   * TODO: A quick question: Will it be a better way to have a list of all replicas in the caller of this function by calling getReplicas()
   *  and remove the ones that are already used in the caller and this function is used as a wrapper to just call
   *  routingStrategy.getReplicas() and get requiredReplicaCount number of replicas from that list? or this is intended as we are
   *  optimizing for the first try and not for retries?
   *
   *  If the intention is not to optimize for the first try and not for retries, have a couple of questions on the implementation:
   *  1. Calling getReplicas() for every retry => Is this necessary to get the updated metadata and not have the old stale metadata?
   *  2. loop and check whether some replica is already tried => is this okay as the number of replicas is very small?
   */
  @Override
  public List<String> getReplicas(
      long requestId,
      int version,
      int partitionId,
      int requiredReplicaCount,
      Set<String> excludedInstances) {
    List<String> replicas = getReplicas(version, partitionId);
    List<String> filteredReplicas;

    if (excludedInstances.isEmpty()) {
      filteredReplicas = replicas;
    } else {
      filteredReplicas = new ArrayList<>(replicas.size());
      replicas.forEach(replica -> {
        if (!excludedInstances.contains(replica)) {
          filteredReplicas.add(replica);
        }
      });
    }

    return routingStrategy.getReplicas(requestId, filteredReplicas, requiredReplicaCount);
  }

  @Override
  public CompletableFuture<HttpStatus> sendRequestToInstance(String instance, int version, int partitionId) {
    return instanceHealthMonitor.sendRequestToInstance(instance);
  }

  @Override
  public InstanceHealthMonitor getInstanceHealthMonitor() {
    return instanceHealthMonitor;
  }

  @Override
  public void close() throws IOException {
    Utils.closeQuietlyWithErrorLogged(instanceHealthMonitor);
  }

}
