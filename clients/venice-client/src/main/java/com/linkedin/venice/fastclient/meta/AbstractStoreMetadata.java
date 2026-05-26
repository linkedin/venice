package com.linkedin.venice.fastclient.meta;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.fastclient.ClientConfig;
import com.linkedin.venice.fastclient.GetRequestContext;
import com.linkedin.venice.fastclient.MultiKeyRequestContext;
import com.linkedin.venice.fastclient.RequestContext;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.ChainedCompletableFuture;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public abstract class AbstractStoreMetadata implements StoreMetadata {
  private static final Logger LOGGER = LogManager.getLogger(AbstractStoreMetadata.class);
  private static final AtomicLong REQUEST_ID_GENERATOR = new AtomicLong();
  private final ClientConfig clientConfig;
  private final InstanceHealthMonitor instanceHealthMonitor;
  protected volatile AbstractClientRoutingStrategy routingStrategy;
  protected final String storeName;
  private final CopyOnWriteArrayList<StoreVersionSwitchListener> versionSwitchListeners = new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<StoreConfigChangeListener> storeConfigChangeListeners =
      new CopyOnWriteArrayList<>();

  public AbstractStoreMetadata(ClientConfig clientConfig) {
    this.clientConfig = clientConfig;
    this.instanceHealthMonitor = clientConfig.getInstanceHealthMonitor();
    this.storeName = clientConfig.getStoreName();
    ClientRoutingStrategyType clientRoutingStrategyType = clientConfig.getClientRoutingStrategyType();
    LOGGER.info("Chose the following routing strategy: {} for store: {}", clientRoutingStrategyType, storeName);
    this.routingStrategy = getRoutingStrategy(clientRoutingStrategyType);
  }

  private AbstractClientRoutingStrategy getRoutingStrategy(ClientRoutingStrategyType clientRoutingStrategyType) {
    switch (clientRoutingStrategyType) {
      case HELIX_ASSISTED:
        return clientConfig.isEnableLeastLoadedRoutingStrategyForHelixGroupRouting()
            ? new HelixLeastLoadedGroupRoutingStrategy(
                instanceHealthMonitor,
                clientConfig.getMetricsRepository(),
                getStoreName())
            : new HelixGroupRoutingStrategy(instanceHealthMonitor, clientConfig.getMetricsRepository(), getStoreName());
      case LEAST_LOADED:
        return new LeastLoadedClientRoutingStrategy(this.instanceHealthMonitor);
      default:
        throw new VeniceClientException("Unexpected routing strategy type: " + clientRoutingStrategyType);
    }
  }

  public void setRoutingStrategy(ClientRoutingStrategyType strategyType) {
    this.routingStrategy = getRoutingStrategy(strategyType);
    LOGGER.info(
        "Switched to the following routing strategy: {} for store: {} and the new strategy: {}",
        strategyType,
        storeName,
        routingStrategy.getClass().getSimpleName());
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
  public String getReplica(long requestId, int groupId, int version, int partitionId, Set<String> excludedInstances) {
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

    return routingStrategy.getReplicas(requestId, groupId, filteredReplicas);
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

  @Override
  public <K> void routeRequest(RequestContext requestContext, RecordSerializer<K> keySerializer) {
    requestContext.setServerClusterName(getClusterName());
    requestContext.setInstanceHealthMonitor(getInstanceHealthMonitor());

    int currentVersion = requestContext.getCurrentVersion();
    if (currentVersion <= 0) {
      /**
       * For retry request, the current version field should be setup by the caller already.
       */
      currentVersion = getCurrentStoreVersion();
      requestContext.setCurrentVersion(currentVersion);
    }
    requestContext.setRequestId(REQUEST_ID_GENERATOR.getAndIncrement());
    int groupId = routingStrategy.getHelixGroupId(requestContext.getRequestId(), requestContext.getHelixGroupId());
    requestContext.setHelixGroupId(groupId);
    /**
     * Track the request for the routing strategy.
     */
    routingStrategy.trackRequest(requestContext);

    RequestType requestType = requestContext.getRequestType();
    if (requestType.equals(RequestType.SINGLE_GET)) {
      GetRequestContext<K> getRequestContext = (GetRequestContext) requestContext;
      byte[] serializedKey = getRequestContext.getSerializedKey();
      if (serializedKey == null) {
        long nanoTsBeforeSerialization = System.nanoTime();
        serializedKey = keySerializer.serialize(getRequestContext.getKey());
        getRequestContext.setSerializedKey(serializedKey);
        requestContext.setRequestSerializationTime(LatencyUtils.getElapsedTimeFromNSToMS(nanoTsBeforeSerialization));
      }

      int partitionId = getRequestContext.getPartitionId();
      if (partitionId < 0) {
        partitionId = getPartitionId(currentVersion, serializedKey);
        if (partitionId < 0) {
          throw new VeniceClientException("Invalid partition id found: " + partitionId + " for single key lookup");
        }
        getRequestContext.setPartitionId(partitionId);
      }

      String route = getReplica(
          requestContext.getRequestId(),
          groupId,
          currentVersion,
          partitionId,
          getRequestContext.getRouteRequestMap().keySet());
      if (route == null) {
        getRequestContext.addNonAvailableReplicaPartition(partitionId);
      } else {
        getRequestContext.setRoute(route);
      }
      return;
    }
    if (requestType.equals(RequestType.MULTI_GET_STREAMING) || requestType.equals(RequestType.COMPUTE_STREAMING)) {
      MultiKeyRequestContext<K, Object> multiKeyRequestContext = (MultiKeyRequestContext) requestContext;
      Set<K> keys = multiKeyRequestContext.getKeys();

      Map<Integer, String> partitionRouteMap = new HashMap<>();
      final int currentVersionFinal = currentVersion;
      for (K key: keys) {
        byte[] keyBytes = keySerializer.serialize(key);
        // For each key determine partition
        int partitionId = getPartitionId(currentVersion, keyBytes);
        // Find routes for each partition
        String route = partitionRouteMap.computeIfAbsent(
            partitionId,
            (ignored) -> getReplica(
                requestContext.getRequestId(),
                groupId,
                currentVersionFinal,
                partitionId,
                multiKeyRequestContext.getRoutesForPartitionMapping()
                    .getOrDefault(Integer.valueOf(partitionId), Collections.emptySet())));
        if (route == null) {
          /* If a partition doesn't have an available route then there is something wrong about or metadata and this is
           * an error */
          multiKeyRequestContext.addNonAvailableReplicaPartition(partitionId);
          continue;
        }
        multiKeyRequestContext.addKey(route, key, keyBytes, partitionId);
      }
      multiKeyRequestContext.setFanoutSize(multiKeyRequestContext.getRoutes().size());
      return;
    }
    throw new VeniceClientException("Unknown request type: " + requestType);
  }

  @Override
  public void registerVersionSwitchListener(StoreVersionSwitchListener listener) {
    if (listener == null) {
      throw new IllegalArgumentException("StoreVersionSwitchListener must not be null");
    }
    versionSwitchListeners.addIfAbsent(listener);
  }

  @Override
  public void unregisterVersionSwitchListener(StoreVersionSwitchListener listener) {
    if (listener == null) {
      return;
    }
    versionSwitchListeners.remove(listener);
  }

  /**
   * Notify all registered listeners that the store's current serving version changed from {@code previousVersion}
   * to {@code newVersion}. Each listener is invoked synchronously on the calling thread; listener exceptions are
   * caught and logged so that one bad listener cannot break the metadata refresh or starve other listeners.
   */
  protected void fireVersionSwitch(int previousVersion, int newVersion) {
    if (versionSwitchListeners.isEmpty()) {
      return;
    }
    for (StoreVersionSwitchListener listener: versionSwitchListeners) {
      try {
        listener.onVersionSwitch(previousVersion, newVersion);
      } catch (Throwable t) {
        LOGGER.error(
            "Store {} version-switch listener {} threw on transition {} -> {}",
            storeName,
            listener.getClass().getName(),
            previousVersion,
            newVersion,
            t);
      }
    }
  }

  @Override
  public void registerStoreConfigChangeListener(StoreConfigChangeListener listener) {
    if (listener == null) {
      throw new IllegalArgumentException("StoreConfigChangeListener must not be null");
    }
    storeConfigChangeListeners.addIfAbsent(listener);
  }

  @Override
  public void unregisterStoreConfigChangeListener(StoreConfigChangeListener listener) {
    if (listener == null) {
      return;
    }
    storeConfigChangeListeners.remove(listener);
  }

  /**
   * Notify all registered listeners that the store-level config snapshot has changed. Fires only when
   * {@code previous} and {@code current} differ by value-equality, so callers may invoke this on every refresh
   * without manual diffing. Listener exceptions are caught and logged.
   *
   * @param previous snapshot from the prior refresh, or {@code null} on the first refresh after client start
   * @param current  snapshot just materialized; must not be {@code null}
   */
  protected void fireStoreConfigChange(StoreConfigSnapshot previous, StoreConfigSnapshot current) {
    if (current == null) {
      throw new IllegalArgumentException("current snapshot must not be null");
    }
    if (current.equals(previous)) {
      return;
    }
    if (storeConfigChangeListeners.isEmpty()) {
      return;
    }
    for (StoreConfigChangeListener listener: storeConfigChangeListeners) {
      try {
        listener.onStoreConfigChange(previous, current);
      } catch (Throwable t) {
        LOGGER.error(
            "Store {} store-config-change listener {} threw on transition {} -> {}",
            storeName,
            listener.getClass().getName(),
            previous,
            current,
            t);
      }
    }
  }
}
