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
    if ( null != clientConfig.getClientRoutingStrategy()) {
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

  @Override
  public List<String> getReplicas(long requestId, int version, int partitionId, int requiredReplicaCount, Set<String> excludedInstances) {
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
