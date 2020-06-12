package com.linkedin.venice;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.common.StoreMetadataType;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.BackupStrategy;
import com.linkedin.venice.meta.ETLStoreConfig;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.meta.systemstore.schemas.CurrentStoreStates;
import com.linkedin.venice.meta.systemstore.schemas.CurrentVersionStates;
import com.linkedin.venice.meta.systemstore.schemas.StoreMetadataKey;
import com.linkedin.venice.meta.systemstore.schemas.StoreMetadataValue;
import com.linkedin.venice.meta.systemstore.schemas.StoreProperties;
import com.linkedin.venice.meta.systemstore.schemas.StoreVersionState;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.apache.log4j.Logger;

import static com.linkedin.venice.common.VeniceSystemStoreUtils.*;
import static java.lang.Thread.*;


public class MetadataStoreBasedStoreRepository implements ReadOnlyStoreRepository {
  private static final Logger logger = Logger.getLogger(MetadataStoreBasedStoreRepository.class);

  private final Set<StoreDataChangedListener> listeners = new CopyOnWriteArraySet<>();
  // A lock to make sure that all updates are serialized and events are delivered in the correct order
  private final ReentrantLock updateLock = new ReentrantLock();
  private final AtomicLong totalStoreReadQuota = new AtomicLong();
  private final Map<String, Store> storeMap = new VeniceConcurrentHashMap<>();
  // subscribedStores keeps track of all regular stores it is monitoring
  private final Set<String> subscribedStores = new HashSet<>();
  private final Map<String, AvroSpecificStoreClient<StoreMetadataKey, StoreMetadataValue>> storeClientMap = new VeniceConcurrentHashMap<>();
  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
  private final String clusterName;
  private final String routerUrl;

  public MetadataStoreBasedStoreRepository(String clusterName, String routerUrl) {
    this.clusterName = clusterName;
    this.routerUrl = routerUrl;
    this.scheduler.scheduleAtFixedRate(this::refresh, 0, 3, TimeUnit.MINUTES);
  }


  public void subscribe(String storeName) {
    updateLock.lock();
    try {
      subscribedStores.add(storeName);
      refreshOneStore(storeName);
    } finally {
      updateLock.unlock();
    }
  }

  public void unsubscribe(String storeName) {
    updateLock.lock();
    try {
      subscribedStores.remove(storeName);
      removeStore(storeName);
    } finally {
      updateLock.unlock();
    }
  }

  @Override
  public Store getStore(String storeName) {
    Store store = storeMap.get(getZkStoreName(storeName));
    if (store != null) {
      return store.cloneStore();
    }
    return null;
  }

  @Override
  public Store getStoreOrThrow(String storeName) throws VeniceNoStoreException {
    Store store = storeMap.get(getZkStoreName(storeName));
    if (store != null) {
      return store.cloneStore();
    }
    throw new VeniceNoStoreException(storeName, clusterName);
  }

  @Override
  public boolean hasStore(String storeName) {
    return storeMap.containsKey(getZkStoreName(storeName));
  }

  @Override
  public Store refreshOneStore(String storeName) {
    updateLock.lock();
    try {
      Store newStore = getStoreFromFromSystemStore(storeName);
      // Make sure we don't add back stores that are unsubscribed.
      if (newStore != null && subscribedStores.contains(storeName)) {
        putStore(newStore);
      } else {
        removeStore(storeName);
      }
      return newStore;
    } finally {
      updateLock.unlock();
    }
  }

  @Override
  public List<Store> getAllStores() {
    return new ArrayList<>(storeMap.values());
  }

  @Override
  public long getTotalStoreReadQuota() {
    return totalStoreReadQuota.get();
  }

  @Override
  public void registerStoreDataChangedListener(StoreDataChangedListener listener) {
    listeners.add(listener);
  }

  @Override
  public void unregisterStoreDataChangedListener(StoreDataChangedListener listener) {
    listeners.remove(listener);
  }

  @Override
  public int getBatchGetLimit(String storeName) {
    return getStoreOrThrow(storeName).getBatchGetLimit();
  }

  @Override
  public boolean isReadComputationEnabled(String storeName) {
    return getStoreOrThrow(storeName).isReadComputationEnabled();
  }

  @Override
  public boolean isSingleGetRouterCacheEnabled(String storeName) {
    return getStoreOrThrow(storeName).isSingleGetRouterCacheEnabled();
  }

  @Override
  public boolean isBatchGetRouterCacheEnabled(String storeName) {
    return getStoreOrThrow(storeName).isBatchGetRouterCacheEnabled();
  }

  @Override
  public void refresh() {
    logger.info("Refresh started for cluster " + clusterName + "'s " + getClass().getSimpleName());
    updateLock.lock();
    try {
      List<Store> newStores = getStoresFromSystemStores();
      Set<String> deletedStoreNames = storeMap.values().stream().map(Store::getName).collect(Collectors.toSet());
      for (Store newStore : newStores) {
        putStore(newStore);
        deletedStoreNames.remove(newStore.getName());
      }

      for (String storeName : deletedStoreNames) {
        removeStore(storeName);
      }
      logger.info("Refresh finished for cluster " + clusterName + "'s " + getClass().getSimpleName());
    } finally {
      updateLock.unlock();
    }
  }

  /**
   * TODO: we may need to rename this function to be 'close' since this resource should not used any more
   * after calling this function.
   */
  @Override
  public void clear() {
    updateLock.lock();
    try {
      subscribedStores.clear();
      storeMap.forEach((k,v) -> removeStore(k));
      storeMap.clear();
      totalStoreReadQuota.set(0);
    } finally {
      updateLock.unlock();
    }

    scheduler.shutdown();
    try {
      if (!scheduler.awaitTermination(60, TimeUnit.SECONDS)) {
        scheduler.shutdownNow();
      }
    } catch (InterruptedException e) {
      currentThread().interrupt();
    }

    storeClientMap.clear();
  }

  protected Store getStoreFromFromSystemStore(String storeName) {
    StoreMetadataKey storeCurrentStatesKey = new StoreMetadataKey();
    storeCurrentStatesKey.keyStrings = Arrays.asList(storeName, clusterName);
    storeCurrentStatesKey.metadataType = StoreMetadataType.CURRENT_STORE_STATES.getValue();

    StoreMetadataKey storeCurrentVersionStatesKey = new StoreMetadataKey();
    storeCurrentVersionStatesKey.keyStrings = Arrays.asList(storeName, clusterName);
    storeCurrentVersionStatesKey.metadataType = StoreMetadataType.CURRENT_VERSION_STATES.getValue();

    updateLock.lock();
    try {
      storeClientMap.computeIfAbsent(storeName, k -> {
        ClientConfig<StoreMetadataValue> clientConfig = ClientConfig.defaultSpecificClientConfig(
                VeniceSystemStoreUtils.getMetadataStoreName(storeName),
                StoreMetadataValue.class).setVeniceURL(this.routerUrl);
        return ClientFactory.getAndStartSpecificAvroClient(clientConfig);
      });
    } finally {
      updateLock.unlock();
    }
    AvroSpecificStoreClient<StoreMetadataKey, StoreMetadataValue> client = storeClientMap.get(storeName);
    try {
      CurrentStoreStates currentStoreStates = (CurrentStoreStates) client.get(storeCurrentStatesKey).get().metadataUnion;
      CurrentVersionStates currentVersionStates = (CurrentVersionStates) client.get(storeCurrentVersionStatesKey).get().metadataUnion;
      return getStoreFromStoreMetadata(currentStoreStates, currentVersionStates);
    } catch (ExecutionException | InterruptedException e) {
      throw new VeniceClientException(e);
    }
  }

  protected List<Version> getVersionsFromCurrentVersionStates(String storeName, CurrentVersionStates currentVersionStates) {
    List<Version> versionList = new ArrayList<>();
    for (StoreVersionState storeVersionState : currentVersionStates.currentVersionStates) {
      PartitionerConfig partitionerConfig = new PartitionerConfig(
          storeVersionState.partitionerConfig.partitionerClass.toString(),
          Utils.getStringMapFromCharSequenceMap(storeVersionState.partitionerConfig.partitionerParams),
          storeVersionState.partitionerConfig.amplificationFactor
      );

      Version version = new Version(storeName, storeVersionState.versionNumber, storeVersionState.creationTime, storeVersionState.pushJobId.toString(), storeVersionState.partitionCount, partitionerConfig);
      version.setChunkingEnabled(storeVersionState.chunkingEnabled);
      version.setCompressionStrategy(CompressionStrategy.valueOf(storeVersionState.compressionStrategy.toString()));
      version.setLeaderFollowerModelEnabled(storeVersionState.leaderFollowerModelEnabled);
      version.setPushType(Version.PushType.valueOf(storeVersionState.pushType.toString()));
      version.setStatus(VersionStatus.valueOf(storeVersionState.status.toString()));
      version.setBufferReplayEnabledForHybrid(storeVersionState.bufferReplayEnabledForHybrid);
      version.setPushStreamSourceAddress(storeVersionState.pushStreamSourceAddress.toString());
      version.setNativeReplicationEnabled(storeVersionState.nativeReplicationEnabled);

      versionList.add(version);
    }
    return versionList;
  }

  protected Store getStoreFromStoreMetadata(CurrentStoreStates currentStoreStates, CurrentVersionStates currentVersionStates) {
    StoreProperties storeProperties = currentStoreStates.states;

    HybridStoreConfig hybridStoreConfig = null;
    if (storeProperties.hybrid) {
      hybridStoreConfig = new HybridStoreConfig(storeProperties.hybridStoreConfig.rewindTimeInSeconds, storeProperties.hybridStoreConfig.offsetLagThresholdToGoOnline);
    }

    PartitionerConfig partitionerConfig = null;
    if (storeProperties.partitionerConfig != null) {
      partitionerConfig = new PartitionerConfig(
          storeProperties.partitionerConfig.partitionerClass.toString(),
          Utils.getStringMapFromCharSequenceMap(storeProperties.partitionerConfig.partitionerParams),
          storeProperties.partitionerConfig.amplificationFactor
      );
    }

    Store store = new Store(storeProperties.name.toString(),
        storeProperties.owner.toString(),
        storeProperties.createdTime,
        PersistenceType.valueOf(storeProperties.persistenceType.toString()),
        RoutingStrategy.valueOf(storeProperties.routingStrategy.toString()),
        ReadStrategy.valueOf(storeProperties.readStrategy.toString()),
        OfflinePushStrategy.valueOf(storeProperties.offLinePushStrategy.toString()),
        currentVersionStates.currentVersion,
        storeProperties.storageQuotaInByte,
        storeProperties.readQuotaInCU,
        hybridStoreConfig,
        partitionerConfig
    );
    store.setVersions(getVersionsFromCurrentVersionStates(storeProperties.name.toString(), currentVersionStates));
    store.setBackupStrategy(BackupStrategy.valueOf(storeProperties.backupStrategy.toString()));
    store.setBatchGetLimit(storeProperties.batchGetLimit);
    store.setBatchGetRouterCacheEnabled(storeProperties.batchGetRouterCacheEnabled);
    store.setBootstrapToOnlineTimeoutInHours(storeProperties.bootstrapToOnlineTimeoutInHours);
    store.setChunkingEnabled(storeProperties.chunkingEnabled);
    store.setClientDecompressionEnabled(storeProperties.clientDecompressionEnabled);
    store.setCompressionStrategy(CompressionStrategy.valueOf(storeProperties.compressionStrategy.toString()));
    store.setEnableReads(storeProperties.enableReads);
    store.setEnableWrites(storeProperties.enableWrites);
    ETLStoreConfig etlStoreConfig = null;
    if (storeProperties.etlStoreConfig != null) {
      etlStoreConfig = new ETLStoreConfig(storeProperties.etlStoreConfig.etledUserProxyAccount.toString(),
          storeProperties.etlStoreConfig.regularVersionETLEnabled,
          storeProperties.etlStoreConfig.futureVersionETLEnabled
      );
    }
    store.setEtlStoreConfig(etlStoreConfig);
    store.setHybridStoreDiskQuotaEnabled(storeProperties.hybridStoreDiskQuotaEnabled);
    store.setIncrementalPushEnabled(storeProperties.incrementalPushEnabled);
    store.setLargestUsedVersionNumber(storeProperties.largestUsedVersionNumber);
    store.setLatestSuperSetValueSchemaId(storeProperties.latestSuperSetValueSchemaId);
    store.setLeaderFollowerModelEnabled(storeProperties.leaderFollowerModelEnabled);
    store.setMigrating(storeProperties.migrating);
    store.setNativeReplicationEnabled(storeProperties.nativeReplicationEnabled);
    store.setNumVersionsToPreserve(storeProperties.numVersionsToPreserve);
    store.setPartitionCount(storeProperties.partitionCount);
    store.setPushStreamSourceAddress(storeProperties.pushStreamSourceAddress.toString());
    store.setReadComputationEnabled(storeProperties.readComputationEnabled);
    store.setReadQuotaInCU(storeProperties.readQuotaInCU);
    store.setSchemaAutoRegisterFromPushJobEnabled(storeProperties.schemaAutoRegisterFromPushJobEnabled);
    store.setSingleGetRouterCacheEnabled(storeProperties.singleGetRouterCacheEnabled);
    store.setStoreMetadataSystemStoreEnabled(true);
    store.setStorageQuotaInByte(storeProperties.storageQuotaInByte);
    store.setSuperSetSchemaAutoGenerationForReadComputeEnabled(storeProperties.superSetSchemaAutoGenerationForReadComputeEnabled);
    store.setWriteComputationEnabled(storeProperties.writeComputationEnabled);

    return store;
  }

  protected List<Store> getStoresFromSystemStores() {
    return subscribedStores.stream().map(this::getStoreFromFromSystemStore).collect(Collectors.toList());
  }

  protected Store putStore(Store newStore) {
    updateLock.lock();
    try {
      // Workaround to make old metadata compatible with new fields
      newStore.fixMissingFields();

      Store oldStore = storeMap.put(getZkStoreName(newStore.getName()), newStore);
      if (oldStore == null) {
        totalStoreReadQuota.addAndGet(newStore.getReadQuotaInCU());
        notifyStoreCreated(newStore);
      } else if (!oldStore.equals(newStore)) {
        totalStoreReadQuota.addAndGet(newStore.getReadQuotaInCU() - oldStore.getReadQuotaInCU());
        notifyStoreChanged(newStore);
      }
      return oldStore;
    } finally {
      updateLock.unlock();
    }
  }

  protected Store removeStore(String storeName) {
    updateLock.lock();
    try {
      Store oldStore = storeMap.remove(getZkStoreName(storeName));
      if (oldStore != null) {
        totalStoreReadQuota.addAndGet(-oldStore.getReadQuotaInCU());
        notifyStoreDeleted(storeName);
      }
      return oldStore;
    } finally {
      updateLock.unlock();
    }
  }

  protected void notifyStoreCreated(Store store) {
    for (StoreDataChangedListener listener : listeners) {
      try {
        listener.handleStoreCreated(store);
      } catch (Throwable e) {
        logger.error("Could not handle store creation event for store: " + store.getName(), e);
      }
    }
  }

  protected void notifyStoreDeleted(String storeName) {
    for (StoreDataChangedListener listener : listeners) {
      try {
        listener.handleStoreDeleted(storeName);
      } catch (Throwable e) {
        logger.error("Could not handle store deletion event for store: " + storeName, e);
      }
    }
  }

  protected void notifyStoreChanged(Store store) {
    for (StoreDataChangedListener listener : listeners) {
      try {
        listener.handleStoreChanged(store);
      } catch (Throwable e) {
        logger.error("Could not handle store updating event for store: " + store.getName(), e);
      }
    }
  }
}
