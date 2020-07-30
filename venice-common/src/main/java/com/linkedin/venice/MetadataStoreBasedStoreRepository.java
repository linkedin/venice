package com.linkedin.venice;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.common.StoreMetadataType;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.exceptions.MissingKeyInStoreMetadataException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.BackupStrategy;
import com.linkedin.venice.meta.ETLStoreConfig;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.meta.SubscriptionBasedReadOnlyStoreRepository;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.meta.systemstore.schemas.CurrentStoreStates;
import com.linkedin.venice.meta.systemstore.schemas.CurrentVersionStates;
import com.linkedin.venice.meta.systemstore.schemas.StoreKeySchemas;
import com.linkedin.venice.meta.systemstore.schemas.StoreMetadataKey;
import com.linkedin.venice.meta.systemstore.schemas.StoreMetadataValue;
import com.linkedin.venice.meta.systemstore.schemas.StoreProperties;
import com.linkedin.venice.meta.systemstore.schemas.StoreValueSchemas;
import com.linkedin.venice.meta.systemstore.schemas.StoreVersionState;
import com.linkedin.venice.schema.DerivedSchemaEntry;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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


public class MetadataStoreBasedStoreRepository implements SubscriptionBasedReadOnlyStoreRepository, ReadOnlySchemaRepository {
  private static final Logger logger = Logger.getLogger(MetadataStoreBasedStoreRepository.class);
  private static final int KEY_SCHEMA_ID = 1;
  private static final int WAIT_TIME_FOR_NON_DETERMINISTIC_ACTIONS = Time.MS_PER_SECOND;
  private static final int SUBSCRIBE_TIMEOUT_IN_SECONDS = 30;

  // subscribedStores keeps track of all regular stores it is monitoring
  private final Set<String> subscribedStores = new HashSet<>();
  // Local cache for stores.
  private final Map<String, Store> storeMap = new VeniceConcurrentHashMap<>();
  // Local cache for key/value schemas. SchemaData supports one key schema per store only, which may need to be changed for key schema evolvability.
  private final Map<String, SchemaData> schemaMap = new VeniceConcurrentHashMap<>();
  // A lock to make sure that all updates are serialized and events are delivered in the correct order
  private final ReentrantLock updateLock = new ReentrantLock();
  // Local cache for system store clients.
  private final Map<String, AvroSpecificStoreClient<StoreMetadataKey, StoreMetadataValue>> storeClientMap = new VeniceConcurrentHashMap<>();
  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
  private final Set<StoreDataChangedListener> listeners = new CopyOnWriteArraySet<>();
  private final AtomicLong totalStoreReadQuota = new AtomicLong();
  private final String clusterName;
  private final ClientConfig<StoreMetadataValue> clientConfig;

  public MetadataStoreBasedStoreRepository(String clusterName, ClientConfig<StoreMetadataValue> clientConfig, long refreshIntervalInSeconds) {
    this.clusterName = clusterName;
    this.clientConfig = clientConfig;
    this.scheduler.scheduleAtFixedRate(this::refresh, 0, refreshIntervalInSeconds, TimeUnit.SECONDS);
  }

  @Override
  public void subscribe(String storeName) throws InterruptedException {
    updateLock.lock();
    try {
      if (subscribedStores.contains(storeName)) {
        return;
      }
      long timeoutTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(SUBSCRIBE_TIMEOUT_IN_SECONDS);
      while (true) {
        try {
          refreshOneStore(storeName);
          // Make sure refresh completes first before we add store to subscription, so periodic refresh thread won't throw exception.
          subscribedStores.add(storeName);
          return;
        } catch (MissingKeyInStoreMetadataException e) {
          if (System.currentTimeMillis() > timeoutTime) {
            throw e;
          }
          Thread.sleep(WAIT_TIME_FOR_NON_DETERMINISTIC_ACTIONS);
        }
      }
    } finally {
      updateLock.unlock();
    }
  }

  @Override
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
      if (newStore != null) {
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

  /**
   * This function is used to retrieve key schema for the given store.
   * If key schema for the given store doesn't exist, will return null;
   * Otherwise, it will return the key schema;
   * @throws {@link com.linkedin.venice.exceptions.VeniceNoStoreException} if the store doesn't exist;
   * @return
   *    null, if key schema for the given store doesn't exist;
   *    key schema entry, otherwise;
   */
  @Override
  public SchemaEntry getKeySchema(String storeName) {
    fetchStoreSchemaIfNotInCache(storeName);
    SchemaData schemaData = schemaMap.get(getZkStoreName(storeName));
    if (null == schemaData) {
      throw new VeniceNoStoreException(storeName);
    }
    return schemaData.getKeySchema();
  }

  /**
   * This function is used to retrieve the value schema for the given store and value schema id.
   *
   * Caller shouldn't modify the returned SchemeEntry
   *
   * @throws {@link com.linkedin.venice.exceptions.VeniceNoStoreException} if the store doesn't exist;
   * @return
   *    null, if the schema doesn't exist;
   *    value schema entry, otherwise;
   */
  @Override
  public SchemaEntry getValueSchema(String storeName, int id) {
    return getValueSchemaInternally(storeName, id);
  }

  /**
   * This function is used to check whether the value schema id is valid in the given store.
   *
   * @throws {@link com.linkedin.venice.exceptions.VeniceNoStoreException} if the store doesn't exist;
   */
  @Override
  public boolean hasValueSchema(String storeName, int id) {
    SchemaEntry valueSchema = getValueSchemaInternally(storeName, id);
    return null != valueSchema;
  }

  /**
   * This function is used to retrieve value schema id for the given store and schema.
   *
   * @throws {@link com.linkedin.venice.exceptions.VeniceNoStoreException} if the store doesn't exist;
   * @throws {@link org.apache.avro.SchemaParseException} if the schema is invalid;
   * @return
   *    {@link com.linkedin.venice.schema.SchemaData#INVALID_VALUE_SCHEMA_ID}, if the schema doesn't exist in the given store;
   *    schema id (int), if the schema exists in the given store
   */
  @Override
  public int getValueSchemaId(String storeName, String valueSchemaStr) {
    fetchStoreSchemaIfNotInCache(storeName);
    SchemaData schemaData = schemaMap.get(getZkStoreName(storeName));
    if (null == schemaData) {
      throw new VeniceNoStoreException(storeName);
    }
    // Could throw SchemaParseException
    SchemaEntry valueSchema = new SchemaEntry(SchemaData.INVALID_VALUE_SCHEMA_ID, valueSchemaStr);
    return schemaData.getSchemaID(valueSchema);
  }

  /**
   * This function is used to retrieve all the value schemas for the given store.
   * @throws {@link com.linkedin.venice.exceptions.VeniceNoStoreException} if the store doesn't exist;
   */
  @Override
  public Collection<SchemaEntry> getValueSchemas(String storeName) {
    fetchStoreSchemaIfNotInCache(storeName);
    SchemaData schemaData = schemaMap.get(getZkStoreName(storeName));
    if (null == schemaData) {
      throw new VeniceNoStoreException(storeName);
    }
    return schemaData.getValueSchemas();
  }

  @Override
  public SchemaEntry getLatestValueSchema(String storeName) {
    fetchStoreSchemaIfNotInCache(storeName);
    SchemaData schemaData = schemaMap.get(getZkStoreName(storeName));
    if (null == schemaData) {
      throw new VeniceNoStoreException(storeName);
    }
    Store store = getStoreOrThrow(storeName);
    int latestValueSchemaId;
    if (store.getLatestSuperSetValueSchemaId() != SchemaData.INVALID_VALUE_SCHEMA_ID) {
      latestValueSchemaId = store.getLatestSuperSetValueSchemaId();
    } else {
      latestValueSchemaId = schemaData.getMaxValueSchemaId();
    }
    if (latestValueSchemaId == SchemaData.INVALID_VALUE_SCHEMA_ID) {
      throw new VeniceException(storeName + " doesn't have latest schema!");
    }
    return schemaData.getValueSchema(latestValueSchemaId);
  }

  @Override
  public Pair<Integer, Integer> getDerivedSchemaId(String storeName, String derivedSchemaStr) {
    throw new VeniceException("Derived schema is not included in system store.");
  }

  @Override
  public DerivedSchemaEntry getDerivedSchema(String storeName, int valueSchemaId, int derivedSchemaId) {
    throw new VeniceException("Derived schema is not included in system store.");
  }

  @Override
  public Collection<DerivedSchemaEntry> getDerivedSchemas(String storeName) {
    throw new VeniceException("Derived schema is not included in system store.");
  }

  @Override
  public DerivedSchemaEntry getLatestDerivedSchema(String storeName, int valueSchemaId) {
    throw new VeniceException("Derived schema is not included in system store.");
  }

  /**
   * This method will be triggered periodically to keep the store/schema information up-to-date.
   */
  @Override
  public void refresh() {
    logger.info("Refresh started for cluster " + clusterName + "'s " + getClass().getSimpleName());
    updateLock.lock();
    try {
      List<Store> newStores = getStoresFromSystemStores();
      Set<String> deletedStoreNames = storeMap.values().stream().map(Store::getName).collect(Collectors.toSet());
      for (Store newStore : newStores) {
        putStore(newStore);
        putStoreSchema(newStore.getName());
        deletedStoreNames.remove(newStore.getName());
      }

      for (String storeName : deletedStoreNames) {
        removeStore(storeName);
        removeStoreSchema(storeName);
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
    updateLock.lock();
    try {
      subscribedStores.clear();
      storeMap.clear();
      schemaMap.clear();
      storeClientMap.clear();
      totalStoreReadQuota.set(0);
    } finally {
      updateLock.unlock();
    }
  }

  protected Store getStoreFromFromSystemStore(String storeName) {
    StoreMetadataKey storeCurrentStatesKey = new StoreMetadataKey();
    storeCurrentStatesKey.keyStrings = Arrays.asList(storeName, clusterName);
    storeCurrentStatesKey.metadataType = StoreMetadataType.CURRENT_STORE_STATES.getValue();
    StoreMetadataKey storeCurrentVersionStatesKey = new StoreMetadataKey();
    storeCurrentVersionStatesKey.keyStrings = Arrays.asList(storeName, clusterName);
    storeCurrentVersionStatesKey.metadataType = StoreMetadataType.CURRENT_VERSION_STATES.getValue();

    AvroSpecificStoreClient<StoreMetadataKey, StoreMetadataValue> client = getAvroClientForSystemStore(storeName);
    StoreMetadataValue storeMetadataValue;
    try {
      storeMetadataValue = client.get(storeCurrentStatesKey).get();
      if (storeMetadataValue == null) {
        throw new MissingKeyInStoreMetadataException(storeCurrentStatesKey.toString(), CurrentStoreStates.class.getName());
      }
      CurrentStoreStates currentStoreStates = (CurrentStoreStates) storeMetadataValue.metadataUnion;

      storeMetadataValue = client.get(storeCurrentVersionStatesKey).get();
      if (storeMetadataValue == null) {
        throw new MissingKeyInStoreMetadataException(storeCurrentVersionStatesKey.toString(), CurrentVersionStates.class.getName());
      }
      CurrentVersionStates currentVersionStates = (CurrentVersionStates) storeMetadataValue.metadataUnion;

      return getStoreFromStoreMetadata(currentStoreStates, currentVersionStates);
    } catch (ExecutionException | InterruptedException e) {
      throw new VeniceClientException(e);
    }
  }

  protected List<Store> getStoresFromSystemStores() {
    return subscribedStores.stream().map(this::getStoreFromFromSystemStore).collect(Collectors.toList());
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
      // Remove the store name from the subscription.
      subscribedStores.remove(storeName);
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

  protected AvroSpecificStoreClient<StoreMetadataKey, StoreMetadataValue> getAvroClientForSystemStore(String storeName) {
    storeClientMap.computeIfAbsent(storeName,  k -> {
      ClientConfig<StoreMetadataValue> clonedClientConfig = ClientConfig.cloneConfig(clientConfig)
          .setStoreName(VeniceSystemStoreUtils.getMetadataStoreName(storeName));
      return ClientFactory.getAndStartSpecificAvroClient(clonedClientConfig);
    });
    return storeClientMap.get(storeName);
  }

  protected void fetchStoreSchemaIfNotInCache(String storeName) {
    if (!schemaMap.containsKey(getZkStoreName(storeName))) {
      putStoreSchema(storeName);
    }
  }

  protected void putStoreSchema(String storeName) {
    updateLock.lock();
    try {
      if (!hasStore(storeName)) {
        throw new VeniceNoStoreException(storeName);
      }
      schemaMap.put(getZkStoreName(storeName), getSchemaDataFromSystemStore(storeName));
    } finally {
      updateLock.unlock();
    }
  }

  protected SchemaEntry getValueSchemaInternally(String storeName, int id) {
    fetchStoreSchemaIfNotInCache(storeName);
    SchemaData schemaData = schemaMap.get(getZkStoreName(storeName));
    if (null == schemaData) {
      throw new VeniceNoStoreException(storeName);
    }
    return schemaData.getValueSchema(id);
  }

  protected SchemaData getSchemaDataFromSystemStore(String storeName) {
    // Prepare system store query keys.
    StoreMetadataKey storeKeySchemasKey = new StoreMetadataKey();
    storeKeySchemasKey.keyStrings = Arrays.asList(storeName, clusterName);
    storeKeySchemasKey.metadataType = StoreMetadataType.STORE_KEY_SCHEMAS.getValue();
    StoreMetadataKey storeValueSchemasKey = new StoreMetadataKey();
    storeValueSchemasKey.keyStrings = Arrays.asList(storeName, clusterName);
    storeValueSchemasKey.metadataType = StoreMetadataType.STORE_VALUE_SCHEMAS.getValue();

    AvroSpecificStoreClient<StoreMetadataKey, StoreMetadataValue> client = getAvroClientForSystemStore(storeName);

    StoreKeySchemas storeKeySchemas;
    StoreValueSchemas storeValueSchemas;
    StoreMetadataValue storeMetadataValue;
    try {
      storeMetadataValue = client.get(storeKeySchemasKey).get();
      if (storeMetadataValue == null) {
        throw new MissingKeyInStoreMetadataException(storeKeySchemasKey.toString(), StoreKeySchemas.class.getName());
      }
      storeKeySchemas = (StoreKeySchemas) storeMetadataValue.metadataUnion;

      storeMetadataValue = client.get(storeValueSchemasKey).get();
      if (storeMetadataValue == null) {
        throw new MissingKeyInStoreMetadataException(storeValueSchemasKey.toString(), StoreValueSchemas.class.getName());
      }
      storeValueSchemas = (StoreValueSchemas) storeMetadataValue.metadataUnion;
    } catch (ExecutionException | InterruptedException e) {
      throw new VeniceClientException(e);
    }

    // If the local cache doesn't have the schema entry for this store,
    // it could be added recently, and we need to add/monitor it locally
    SchemaData schemaData = new SchemaData(storeName);
    // Fetch first key schema from keySchemaMap. For now only one key schema is used for each store.
    String keySchemaString = storeKeySchemas.keySchemaMap.values().iterator().next().toString();
    // Since key schema are not mutated (not even the child zk path) there is no need to set watches
    schemaData.setKeySchema(new SchemaEntry(KEY_SCHEMA_ID, keySchemaString));
    // Fetch value schema
    storeValueSchemas.valueSchemaMap.forEach((key, val) -> schemaData.addValueSchema(new SchemaEntry(Integer.parseInt(key.toString()), val.toString())));

    return schemaData;
  }

  /**
   * This function is used to remove schema entry for the given store from local cache,
   * and related listeners as well.
   */
  protected void removeStoreSchema(String storeName) {
    updateLock.lock();
    try {
      if (!schemaMap.containsKey(getZkStoreName(storeName))) {
        return;
      }
      logger.info("Remove schema for store locally: " + storeName);
      schemaMap.remove(getZkStoreName(storeName));
    } finally {
      updateLock.unlock();
    }
  }
}
