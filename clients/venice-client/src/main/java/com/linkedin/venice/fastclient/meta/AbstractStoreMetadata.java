package com.linkedin.venice.fastclient.meta;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.fastclient.ClientConfig;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.ChainedCompletableFuture;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public abstract class AbstractStoreMetadata implements StoreMetadata {
  private static final Logger LOGGER = LogManager.getLogger(AbstractStoreMetadata.class);
  private final InstanceHealthMonitor instanceHealthMonitor;
  protected volatile AbstractClientRoutingStrategy routingStrategy;
  protected final String storeName;

  public AbstractStoreMetadata(ClientConfig clientConfig) {
    this.instanceHealthMonitor = clientConfig.getInstanceHealthMonitor();
    this.storeName = clientConfig.getStoreName();
    ClientRoutingStrategyType clientRoutingStrategyType = clientConfig.getClientRoutingStrategyType();
    LOGGER.info("Chose the following routing strategy: {} for store: {}", clientRoutingStrategyType, storeName);
    this.routingStrategy = getRoutingStrategy(clientRoutingStrategyType);
  }

  private AbstractClientRoutingStrategy getRoutingStrategy(ClientRoutingStrategyType clientRoutingStrategyType) {
    switch (clientRoutingStrategyType) {
      case HELIX_ASSISTED:
        return new HelixScatterGatherRoutingStrategy(instanceHealthMonitor);
      case LEAST_LOADED:
        return new LeastLoadedClientRoutingStrategy(this.instanceHealthMonitor);
      default:
        throw new VeniceClientException("Unexpected routing strategy type: " + clientRoutingStrategyType);
    }
  }

  public void setRoutingStrategy(ClientRoutingStrategyType strategyType) {
    this.routingStrategy = getRoutingStrategy(strategyType);
    LOGGER.info("Switched to the following routing strategy: {} for store: {}", strategyType, storeName);
  }

  /**
   * For testing only.
   */
  public void setRoutingStrategy(AbstractClientRoutingStrategy routingStrategy) {
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
  public ChainedCompletableFuture<Integer, Integer> trackHealthBasedOnRequestToInstance(
      String instance,
      int version,
      int partitionId,
      CompletableFuture<TransportClientResponse> transportFuture) {
    return instanceHealthMonitor.trackHealthBasedOnRequestToInstance(instance, transportFuture);
  }

  @Override
  public InstanceHealthMonitor getInstanceHealthMonitor() {
    return instanceHealthMonitor;
  }

  @Override
  public int getBatchGetLimit() {
    return Store.DEFAULT_BATCH_GET_LIMIT;
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
