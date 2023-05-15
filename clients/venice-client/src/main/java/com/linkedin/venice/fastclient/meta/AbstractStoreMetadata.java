package com.linkedin.venice.fastclient.meta;

import com.linkedin.restli.common.HttpStatus;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.fastclient.ClientConfig;
import com.linkedin.venice.utils.Utils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;


public abstract class AbstractStoreMetadata implements StoreMetadata {
  private final InstanceHealthMonitor instanceHealthMonitor;
  protected ClientRoutingStrategy routingStrategy;
  protected final String storeName;

  public AbstractStoreMetadata(ClientConfig clientConfig) {
    this.instanceHealthMonitor = new InstanceHealthMonitor(clientConfig);
    ClientRoutingStrategyType clientRoutingStrategyType = clientConfig.getClientRoutingStrategyType();
    switch (clientRoutingStrategyType) {
      case HELIX_ASSISTED:
        this.routingStrategy = new HelixScatterGatherRoutingStrategy(instanceHealthMonitor);
        break;
      case LEAST_LOADED:
        this.routingStrategy = new LeastLoadedClientRoutingStrategy(this.instanceHealthMonitor);
        break;
      default:
        throw new VeniceClientException("Unexpected routing strategy type: " + clientRoutingStrategyType.toString());
    }
    this.storeName = clientConfig.getStoreName();
  }

  /**
   * For testing only.
   */
  public void setRoutingStrategy(ClientRoutingStrategy routingStrategy) {
    this.routingStrategy = routingStrategy;
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
  public CompletableFuture<HttpStatus> trackHealthBasedOnRequestToInstance(
      String instance,
      int version,
      int partitionId) {
    return instanceHealthMonitor.trackHealthBasedOnRequestToInstance(instance);
  }

  @Override
  public InstanceHealthMonitor getInstanceHealthMonitor() {
    return instanceHealthMonitor;
  }

  @Override
  public void close() throws IOException {
    Utils.closeQuietlyWithErrorLogged(instanceHealthMonitor);
  }

  public VeniceCompressor getCompressor(
      CompressionStrategy compressionStrategy,
      int version,
      CompressorFactory compressorFactory,
      Map<Integer, ByteBuffer> versionZstdDictionaryMap) {
    if (compressionStrategy == CompressionStrategy.ZSTD_WITH_DICT) {
      String resourceName = getResourceName(version);
      VeniceCompressor compressor = compressorFactory.getVersionSpecificCompressor(resourceName);
      if (compressor == null) {
        ByteBuffer dictionary = versionZstdDictionaryMap.get(version);
        if (dictionary == null) {
          throw new VeniceClientException(
              String.format(
                  "No dictionary available for decompressing zstd payload for store %s version %d ",
                  storeName,
                  version));
        } else {
          compressor = compressorFactory
              .createVersionSpecificCompressorIfNotExist(compressionStrategy, resourceName, dictionary.array());
        }
      }
      return compressor;
    } else {
      return compressorFactory.getCompressor(compressionStrategy);
    }
  }

  private String getResourceName(int version) {
    return storeName + "_v" + version;
  }

}
