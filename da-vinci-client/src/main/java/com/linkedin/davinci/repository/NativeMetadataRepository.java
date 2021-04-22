package com.linkedin.davinci.repository;

import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.common.MetadataStoreUtils;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.exceptions.MissingKeyInStoreMetadataException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.BackupStrategy;
import com.linkedin.venice.meta.ClusterInfoProvider;
import com.linkedin.venice.meta.ETLStoreConfig;
import com.linkedin.venice.meta.ETLStoreConfigImpl;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.PartitionerConfigImpl;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStore;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.meta.SubscriptionBasedReadOnlyStoreRepository;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.meta.systemstore.schemas.CurrentStoreStates;
import com.linkedin.venice.meta.systemstore.schemas.CurrentVersionStates;
import com.linkedin.venice.meta.systemstore.schemas.StoreAttributes;
import com.linkedin.venice.meta.systemstore.schemas.StoreKeySchemas;
import com.linkedin.venice.meta.systemstore.schemas.StoreMetadataKey;
import com.linkedin.venice.meta.systemstore.schemas.StoreMetadataValue;
import com.linkedin.venice.meta.systemstore.schemas.StoreProperties;
import com.linkedin.venice.meta.systemstore.schemas.StoreValueSchemas;
import com.linkedin.venice.meta.systemstore.schemas.StoreVersionState;
import com.linkedin.venice.schema.DerivedSchemaEntry;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.service.ICProvider;
import com.linkedin.venice.system.store.MetaStoreDataType;
import com.linkedin.venice.systemstore.schemas.StoreClusterConfig;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.log4j.Logger;

import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.system.store.MetaStoreWriter.*;
import static java.lang.Thread.*;


/**
 * Venice in-house implementation of a read only metadata repository where callers can retrieve various metadata such as
 * Store objects and their corresponding schemas. The implementers of this abstract class all relies on some flavors of
 * Venice system store to carry the metadata from Venice internal components (source) to external consumers such as a
 * DaVinci client (destination). This abstract class includes the implementation of an in-memory cache for all subscribed
 * stores' metadata. Callers are served by the cache and the cache is refreshed periodically by updating it with methods
 * provided by the implementers.
 */
public abstract class NativeMetadataRepository
    implements SubscriptionBasedReadOnlyStoreRepository, ReadOnlySchemaRepository, ClusterInfoProvider {
  protected static final int SUBSCRIBE_TIMEOUT_IN_SECONDS = 30;
  protected static final int KEY_SCHEMA_ID = 1;
  protected static final int DEFAULT_SYSTEM_STORE_CURRENT_VERSION = 1;

  private static final long DEFAULT_REFRESH_INTERVAL_IN_SECONDS = 60;
  private static final Logger logger = Logger.getLogger(NativeMetadataRepository.class);
  private static final int WAIT_TIME_FOR_NON_DETERMINISTIC_ACTIONS = Time.MS_PER_SECOND;

  protected final ClientConfig clientConfig;

  // A map of subscribed user store name to their corresponding Store object.
  protected final Map<String, Store> subscribedStoreMap = new VeniceConcurrentHashMap<>();
  // A map of user store name to their corresponding StoreConfig object.
  private final Map<String, StoreConfig> storeConfigMap = new VeniceConcurrentHashMap<>();
  // Local cache for key/value schemas. SchemaData supports one key schema per store only, which may need to be changed for key schema evolvability.
  private final Map<String, SchemaData> schemaMap = new VeniceConcurrentHashMap<>();
  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
  private final Set<StoreDataChangedListener> listeners = new CopyOnWriteArraySet<>();
  private final AtomicLong totalStoreReadQuota = new AtomicLong();

  protected NativeMetadataRepository(ClientConfig clientConfig, VeniceProperties backendConfig) {
    long refreshIntervalInSeconds = backendConfig.getLong(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS,
        NativeMetadataRepository.DEFAULT_REFRESH_INTERVAL_IN_SECONDS);
    this.scheduler.scheduleAtFixedRate(this::refresh, 0, refreshIntervalInSeconds, TimeUnit.SECONDS);
    this.clientConfig = clientConfig;
  }

  public static NativeMetadataRepository getInstance(ClientConfig clientConfig, VeniceProperties backendConfig) {
    return getInstance(clientConfig, backendConfig, null);
  }

  public static NativeMetadataRepository getInstance(ClientConfig clientConfig, VeniceProperties backendConfig,
      ICProvider icProvider) {
    // Not using a factory pattern here because the different implementations are temporary. Eventually we will only use DaVinciClientMetaStoreBasedRepository.
    // If all feature configs are enabled then:
    // DaVinciClientMetaStoreBasedRepository > ThinClientMetadataStoreBasedRepository.
    Map<String, String> metaStoreVersions = backendConfig.getMap(CLIENT_META_SYSTEM_STORE_VERSION_MAP, new HashMap<>());
    if (backendConfig.getBoolean(CLIENT_USE_DA_VINCI_BASED_SYSTEM_STORE_REPOSITORY, false)) {
      logger.info("Initializing " + NativeMetadataRepository.class.getSimpleName() + " with "
          + DaVinciClientMetaStoreBasedRepository.class.getSimpleName());
      return new DaVinciClientMetaStoreBasedRepository(clientConfig, backendConfig, metaStoreVersions);
    } else if (backendConfig.getBoolean(CLIENT_USE_META_SYSTEM_STORE_REPOSITORY, false)) {
      logger.info("Initializing " + NativeMetadataRepository.class.getSimpleName() + " with "
          + ThinClientMetaStoreBasedRepository.class.getSimpleName());
      return new ThinClientMetaStoreBasedRepository(clientConfig, backendConfig, icProvider);
    } else {
      logger.info("Initializing " + NativeMetadataRepository.class.getSimpleName() + " with "
          + ThinClientMetadataStoreBasedRepository.class.getSimpleName());
      return new ThinClientMetadataStoreBasedRepository(clientConfig, backendConfig, icProvider);
    }
  }

  @Override
  public void subscribe(String storeName) throws InterruptedException {
    if (subscribedStoreMap.containsKey(storeName)) {
      return;
    }
    long timeoutTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(SUBSCRIBE_TIMEOUT_IN_SECONDS);
    while (true) {
      try {
        refreshOneStore(storeName);
        return;
      } catch (MissingKeyInStoreMetadataException e) {
        if (System.currentTimeMillis() > timeoutTime) {
          // Unable to subscribe to the given store, cleanup so periodic refresh thread won't throw exceptions.
          subscribedStoreMap.remove(storeName);
          throw e;
        }
        Thread.sleep(WAIT_TIME_FOR_NON_DETERMINISTIC_ACTIONS);
      }
    }
  }

  @Override
  public void unsubscribe(String storeName) {
    removeStore(storeName);
  }

  @Override
  public Store getStore(String storeName) {
    Store store = subscribedStoreMap.get(storeName);
    if (store != null) {
      return new ReadOnlyStore(store);
    }
    return null;
  }

  @Override
  public Store getStoreOrThrow(String storeName) throws VeniceNoStoreException {
    Store store = subscribedStoreMap.get(storeName);
    if (store != null) {
      return new ReadOnlyStore(store);
    }
    throw new VeniceNoStoreException(storeName);
  }

  @Override
  public boolean hasStore(String storeName) {
    return subscribedStoreMap.containsKey(storeName);
  }

  @Override
  public Store refreshOneStore(String storeName) {
    getAndSetStoreConfigFromSystemStore(storeName);
    StoreConfig storeConfig = storeConfigMap.get(storeName);
    if (storeConfig == null) {
      throw new VeniceException("StoreConfig is missing unexpectedly for store: " + storeName);
    }
    Store newStore = getStoreFromSystemStore(storeName, storeConfig.getCluster());
    // isDeleting check to detect deleted store is only supported by meta system store based implementation.
    if (newStore != null && !storeConfig.isDeleting()) {
      putStore(newStore);
      putStoreSchema(storeName);
    } else {
      removeStore(storeName);
    }
    return newStore;
  }

  // Unlike getStore, this method does not clone the store objects.
  @Override
  public List<Store> getAllStores() {
    return new ArrayList<>(subscribedStoreMap.values());
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
    SchemaData schemaData = schemaMap.get(storeName);
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
    SchemaData schemaData = schemaMap.get(storeName);
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
    SchemaData schemaData = schemaMap.get(storeName);
    if (null == schemaData) {
      throw new VeniceNoStoreException(storeName);
    }
    return schemaData.getValueSchemas();
  }

  @Override
  public SchemaEntry getLatestValueSchema(String storeName) {
    fetchStoreSchemaIfNotInCache(storeName);
    SchemaData schemaData = schemaMap.get(storeName);
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
    try {
      logger.debug("Refresh started for " + getClass().getSimpleName());
      for (String storeName : subscribedStoreMap.keySet()) {
        refreshOneStore(storeName);
      }
      logger.debug("Refresh finished for " + getClass().getSimpleName());
    } catch (Exception e) {
      // Catch all exceptions here so the scheduled periodic refresh doesn't break and transient errors can be retried.
      logger.warn("Caught an exception when trying to refresh " + getClass().getSimpleName(), e);
    }
  }

  /**
   * TODO: we may need to rename this function to be 'close' since this resource should not used any more
   * after calling this function.
   */
  @Override
  public void clear() {
    scheduler.shutdown();
    try {
      if (!scheduler.awaitTermination(60, TimeUnit.SECONDS)) {
        scheduler.shutdownNow();
      }
    } catch (InterruptedException e) {
      currentThread().interrupt();
    }
    subscribedStoreMap.forEach((k, v) -> removeStore(k));
    subscribedStoreMap.clear();
    storeConfigMap.clear();
    schemaMap.clear();
    totalStoreReadQuota.set(0);
  }

  /**
   * Get the store cluster config from system store and update the local cache with it. Different implementation will
   * get the data differently but should all populate the store cluster config map.
   */
  protected void getAndSetStoreConfigFromSystemStore(String storeName) {
    storeConfigMap.put(storeName, getStoreConfigFromSystemStore(storeName));
  }

  protected abstract StoreConfig getStoreConfigFromSystemStore(String storeName);

  protected abstract Store getStoreFromSystemStore(String storeName, String clusterName);

  protected abstract StoreMetadataValue getStoreMetadata(String storeName, StoreMetadataKey key);

  protected abstract StoreMetaValue getStoreMetaValue(String storeName, StoreMetaKey key);

  // Helper function with common code for retrieving StoreConfig from meta system store.
  protected StoreConfig getStoreConfigFromMetaSystemStore(String storeName) {
    StoreClusterConfig clusterConfig = getStoreMetaValue(storeName,
        MetaStoreDataType.STORE_CLUSTER_CONFIG.getStoreMetaKey(
            Collections.singletonMap(KEY_STRING_STORE_NAME, storeName))).storeClusterConfig;
    return new StoreConfig(clusterConfig);
  }

  // Helper function with common code for retrieving SchemaData from meta system store.
  protected SchemaData getSchemaDataFromMetaSystemStore(String storeName) {
    SchemaData schemaData = new SchemaData(storeName);
    StoreMetaKey keySchemaKey =
        MetaStoreDataType.STORE_KEY_SCHEMAS.getStoreMetaKey(Collections.singletonMap(KEY_STRING_STORE_NAME, storeName));
    StoreMetaKey valueSchemaKey = MetaStoreDataType.STORE_VALUE_SCHEMAS.getStoreMetaKey(
        Collections.singletonMap(KEY_STRING_STORE_NAME, storeName));
    Map<CharSequence, CharSequence> keySchemaMap =
        getStoreMetaValue(storeName, keySchemaKey).storeKeySchemas.keySchemaMap;
    if (keySchemaMap.isEmpty()) {
      throw new VeniceException("No key schema found for store: " + storeName);
    }
    Map.Entry<CharSequence, CharSequence> keySchemaEntry = keySchemaMap.entrySet().iterator().next();
    schemaData.setKeySchema(
        new SchemaEntry(Integer.parseInt(keySchemaEntry.getKey().toString()), keySchemaEntry.getValue().toString()));
    Map<CharSequence, CharSequence> valueSchemaMap =
        getStoreMetaValue(storeName, valueSchemaKey).storeValueSchemas.valueSchemaMap;
    valueSchemaMap.forEach(
        (k, v) -> schemaData.addValueSchema(new SchemaEntry(Integer.parseInt(k.toString()), v.toString())));
    return schemaData;
  }

  // Helper function with common code for retrieving StoreConfig data from metadata system store based implementations.
  protected StoreConfig getStoreConfigFromMetadataSystemStore(String storeName) {
    // Metadata system store based implementations doesn't support any other fields of the StoreConfig other than cluster.
    // Please use Meta system store based implementation if other StoreConfig fields are needed.
    StoreMetadataKey storeAttributeKey = MetadataStoreUtils.getStoreAttributesKey(storeName);
    StoreMetadataValue storeMetadataValue = getStoreMetadata(storeName, storeAttributeKey);
    if (storeMetadataValue != null) {
      StoreAttributes storeAttributes = (StoreAttributes) storeMetadataValue.metadataUnion;
      StoreConfig storeConfig = new StoreConfig(storeName);
      storeConfig.setCluster(storeAttributes.sourceCluster.toString());
      return storeConfig;
    } else {
      throw new MissingKeyInStoreMetadataException(storeAttributeKey.toString(), StoreAttributes.class.getName());
    }
  }

  // Helper function with common code for retrieving Store data from metadata system store based implementations
  protected Store getStoreFromMetadataSystemStore(String storeName, String clusterName) {
    StoreMetadataKey storeCurrentStatesKey = MetadataStoreUtils.getCurrentStoreStatesKey(storeName, clusterName);
    StoreMetadataKey storeCurrentVersionStatesKey =
        MetadataStoreUtils.getCurrentVersionStatesKey(storeName, clusterName);

    StoreMetadataValue storeMetadataValue;
    storeMetadataValue = getStoreMetadata(storeName, storeCurrentStatesKey);
    if (storeMetadataValue == null) {
      throw new MissingKeyInStoreMetadataException(storeCurrentStatesKey.toString(),
          CurrentStoreStates.class.getName());
    }
    CurrentStoreStates currentStoreStates = (CurrentStoreStates) storeMetadataValue.metadataUnion;

    storeMetadataValue = getStoreMetadata(storeName, storeCurrentVersionStatesKey);
    if (storeMetadataValue == null) {
      throw new MissingKeyInStoreMetadataException(storeCurrentVersionStatesKey.toString(),
          CurrentVersionStates.class.getName());
    }
    CurrentVersionStates currentVersionStates = (CurrentVersionStates) storeMetadataValue.metadataUnion;

    return getStoreFromStoreMetadata(currentStoreStates, currentVersionStates);
  }

  // Helper functions to parse version data retrieved from metadata system store based implementations
  protected List<Version> getVersionsFromCurrentVersionStates(String storeName,
      CurrentVersionStates currentVersionStates) {
    List<Version> versionList = new ArrayList<>();
    for (StoreVersionState storeVersionState : currentVersionStates.currentVersionStates) {
      PartitionerConfig partitionerConfig =
          new PartitionerConfigImpl(storeVersionState.partitionerConfig.partitionerClass.toString(),
              Utils.getStringMapFromCharSequenceMap(storeVersionState.partitionerConfig.partitionerParams),
              storeVersionState.partitionerConfig.amplificationFactor);

      Version version = new VersionImpl(storeName, storeVersionState.versionNumber, storeVersionState.creationTime,
          storeVersionState.pushJobId.toString(), storeVersionState.partitionCount, partitionerConfig);
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

  // Helper functions to parse Store data retrieved from metadata system store based implementations
  protected Store getStoreFromStoreMetadata(CurrentStoreStates currentStoreStates,
      CurrentVersionStates currentVersionStates) {
    StoreProperties storeProperties = currentStoreStates.states;

    HybridStoreConfig hybridStoreConfig = null;
    if (storeProperties.hybrid) {
      hybridStoreConfig = new HybridStoreConfigImpl(storeProperties.hybridStoreConfig.rewindTimeInSeconds,
          storeProperties.hybridStoreConfig.offsetLagThresholdToGoOnline,
          storeProperties.hybridStoreConfig.producerTimestampLagThresholdToGoOnlineInSeconds);
    }

    PartitionerConfig partitionerConfig = null;
    if (storeProperties.partitionerConfig != null) {
      partitionerConfig = new PartitionerConfigImpl(storeProperties.partitionerConfig.partitionerClass.toString(),
          Utils.getStringMapFromCharSequenceMap(storeProperties.partitionerConfig.partitionerParams),
          storeProperties.partitionerConfig.amplificationFactor);
    }

    Store store =
        new ZKStore(storeProperties.name.toString(), storeProperties.owner.toString(), storeProperties.createdTime,
            PersistenceType.valueOf(storeProperties.persistenceType.toString()),
            RoutingStrategy.valueOf(storeProperties.routingStrategy.toString()),
            ReadStrategy.valueOf(storeProperties.readStrategy.toString()),
            OfflinePushStrategy.valueOf(storeProperties.offLinePushStrategy.toString()),
            currentVersionStates.currentVersion, storeProperties.storageQuotaInByte, storeProperties.readQuotaInCU,
            hybridStoreConfig, partitionerConfig, 1 // TODO: figure out how to get hold of a sensible RF value here
        );
    store.setVersions(getVersionsFromCurrentVersionStates(storeProperties.name.toString(), currentVersionStates));
    store.setBackupStrategy(BackupStrategy.valueOf(storeProperties.backupStrategy.toString()));
    store.setBatchGetLimit(storeProperties.batchGetLimit);
    store.setBootstrapToOnlineTimeoutInHours(storeProperties.bootstrapToOnlineTimeoutInHours);
    store.setChunkingEnabled(storeProperties.chunkingEnabled);
    store.setClientDecompressionEnabled(storeProperties.clientDecompressionEnabled);
    store.setCompressionStrategy(CompressionStrategy.valueOf(storeProperties.compressionStrategy.toString()));
    store.setEnableReads(storeProperties.enableReads);
    store.setEnableWrites(storeProperties.enableWrites);
    ETLStoreConfig etlStoreConfig = null;
    if (storeProperties.etlStoreConfig != null) {
      etlStoreConfig = new ETLStoreConfigImpl(storeProperties.etlStoreConfig.etledUserProxyAccount.toString(),
          storeProperties.etlStoreConfig.regularVersionETLEnabled,
          storeProperties.etlStoreConfig.futureVersionETLEnabled);
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
    store.setStoreMetadataSystemStoreEnabled(true);
    store.setStorageQuotaInByte(storeProperties.storageQuotaInByte);
    store.setWriteComputationEnabled(storeProperties.writeComputationEnabled);
    if (storeProperties.daVinciPushStatusStoreEnabled != null) {
      store.setDaVinciPushStatusStoreEnabled(storeProperties.daVinciPushStatusStoreEnabled);
    } else {
      store.setDaVinciPushStatusStoreEnabled(false);
    }

    return store;
  }

  // Helper function with common code for retrieving schema data from metadata system store based implementations
  protected SchemaData getSchemaDataFromMetadataSystemStore(String storeName) {
    StoreMetadataKey storeKeySchemasKey = MetadataStoreUtils.getStoreKeySchemasKey(storeName);
    StoreMetadataKey storeValueSchemasKey = MetadataStoreUtils.getStoreValueSchemasKey(storeName);
    StoreKeySchemas storeKeySchemas;
    StoreValueSchemas storeValueSchemas;
    StoreMetadataValue storeMetadataValue;
    storeMetadataValue = getStoreMetadata(storeName, storeKeySchemasKey);
    if (storeMetadataValue == null) {
      throw new MissingKeyInStoreMetadataException(storeKeySchemasKey.toString(), StoreKeySchemas.class.getName());
    }
    storeKeySchemas = (StoreKeySchemas) storeMetadataValue.metadataUnion;

    storeMetadataValue = getStoreMetadata(storeName, storeValueSchemasKey);
    if (storeMetadataValue == null) {
      throw new MissingKeyInStoreMetadataException(storeValueSchemasKey.toString(), StoreValueSchemas.class.getName());
    }
    storeValueSchemas = (StoreValueSchemas) storeMetadataValue.metadataUnion;

    // If the local cache doesn't have the schema entry for this store,
    // it could be added recently, and we need to add/monitor it locally
    SchemaData schemaData = new SchemaData(storeName);
    // Fetch first key schema from keySchemaMap. For now only one key schema is used for each store.
    String keySchemaString = storeKeySchemas.keySchemaMap.values().iterator().next().toString();
    // Since key schema are not mutated (not even the child zk path) there is no need to set watches
    schemaData.setKeySchema(new SchemaEntry(KEY_SCHEMA_ID, keySchemaString));
    // Fetch value schema
    storeValueSchemas.valueSchemaMap.forEach(
        (key, val) -> schemaData.addValueSchema(new SchemaEntry(Integer.parseInt(key.toString()), val.toString())));

    return schemaData;
  }

  protected Store putStore(Store newStore) {
    // Workaround to make old metadata compatible with new fields
    newStore.fixMissingFields();
    Store oldStore = subscribedStoreMap.put(newStore.getName(), newStore);
    if (oldStore == null) {
      totalStoreReadQuota.addAndGet(newStore.getReadQuotaInCU());
      notifyStoreCreated(newStore);
    } else if (!oldStore.equals(newStore)) {
      totalStoreReadQuota.addAndGet(newStore.getReadQuotaInCU() - oldStore.getReadQuotaInCU());
      notifyStoreChanged(newStore);
    }
    return oldStore;
  }

  protected Store removeStore(String storeName) {
    // Remove the store name from the subscription.
    Store oldStore = subscribedStoreMap.remove(storeName);
    if (oldStore != null) {
      totalStoreReadQuota.addAndGet(-oldStore.getReadQuotaInCU());
      notifyStoreDeleted(storeName);
    }
    removeStoreSchema(storeName);
    return oldStore;
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

  protected void fetchStoreSchemaIfNotInCache(String storeName) {
    if (!schemaMap.containsKey(storeName)) {
      putStoreSchema(storeName);
    }
  }

  protected void putStoreSchema(String storeName) {
    if (!hasStore(storeName)) {
      throw new VeniceNoStoreException(storeName);
    }
    schemaMap.put(storeName, getSchemaDataFromSystemStore(storeName));
  }

  protected SchemaEntry getValueSchemaInternally(String storeName, int id) {
    fetchStoreSchemaIfNotInCache(storeName);
    SchemaData schemaData = schemaMap.get(storeName);
    if (null == schemaData) {
      throw new VeniceNoStoreException(storeName);
    }
    return schemaData.getValueSchema(id);
  }

  protected abstract SchemaData getSchemaDataFromSystemStore(String storeName);

  /**
   * This function is used to remove schema entry for the given store from local cache,
   * and related listeners as well.
   */
  protected void removeStoreSchema(String storeName) {
    if (!schemaMap.containsKey(storeName)) {
      return;
    }
    schemaMap.remove(storeName);
  }

  @Override
  public String getVeniceCluster(String storeName) {
    VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName);
    String regularStoreName = systemStoreType == null ? storeName : systemStoreType.extractRegularStoreName(storeName);
    StoreConfig storeConfig = storeConfigMap.get(regularStoreName);
    return storeConfig == null ? null : storeConfig.getCluster();
  }
}
