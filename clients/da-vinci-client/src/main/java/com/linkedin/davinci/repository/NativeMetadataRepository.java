package com.linkedin.davinci.repository;

import static com.linkedin.venice.ConfigKeys.CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS;
import static com.linkedin.venice.system.store.MetaStoreWriter.KEY_STRING_SCHEMA_ID;
import static com.linkedin.venice.system.store.MetaStoreWriter.KEY_STRING_STORE_NAME;
import static java.lang.Thread.currentThread;

import com.linkedin.venice.client.exceptions.ServiceDiscoveryException;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.exceptions.MissingKeyInStoreMetadataException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.ClusterInfoProvider;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStore;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.meta.SubscriptionBasedReadOnlyStoreRepository;
import com.linkedin.venice.schema.GeneratedSchemaID;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.service.ICProvider;
import com.linkedin.venice.system.store.MetaStoreDataType;
import com.linkedin.venice.systemstore.schemas.StoreClusterConfig;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


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
  protected static final int THIN_CLIENT_RETRY_COUNT = 3;
  protected static final long THIN_CLIENT_RETRY_BACKOFF_MS = 10000;

  private static final long DEFAULT_REFRESH_INTERVAL_IN_SECONDS = 60;
  private static final Logger LOGGER = LogManager.getLogger(NativeMetadataRepository.class);

  protected final ClientConfig clientConfig;

  // A map of subscribed user store name to their corresponding Store object.
  protected final Map<String, Store> subscribedStoreMap = new VeniceConcurrentHashMap<>();
  // A map of user store name to their corresponding StoreConfig object.
  private final Map<String, StoreConfig> storeConfigMap = new VeniceConcurrentHashMap<>();
  // Local cache for key/value schemas. SchemaData supports one key schema per store only, which may need to be changed
  // for key schema evolvability.
  private final Map<String, SchemaData> schemaMap = new VeniceConcurrentHashMap<>();
  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
  private final Set<StoreDataChangedListener> listeners = new CopyOnWriteArraySet<>();
  private final AtomicLong totalStoreReadQuota = new AtomicLong();

  private final long refreshIntervalInSeconds;

  private AtomicBoolean started = new AtomicBoolean(false);

  protected NativeMetadataRepository(ClientConfig clientConfig, VeniceProperties backendConfig) {
    refreshIntervalInSeconds = backendConfig.getLong(
        CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS,
        NativeMetadataRepository.DEFAULT_REFRESH_INTERVAL_IN_SECONDS);
    this.clientConfig = clientConfig;
  }

  public synchronized void start() {
    if (started.get() && scheduler.isShutdown()) {
      // The only way the started flag would be true and the scheduler shutdown would be if we already
      // started and called 'clear' on this object. So here we abort the call to prevent it being restarted again
      throw new VeniceException(
          "Calling start() failed! NativeMetadataRepository has already been cleared and shutdown!");
    }
    if (!started.get()) {
      this.scheduler.scheduleAtFixedRate(this::refresh, 0, refreshIntervalInSeconds, TimeUnit.SECONDS);
      started.set(true);
    }
  }

  private void throwIfNotStartedOrCleared() {
    if (!started.get()) {
      throw new VeniceException("NativeMetadataRepository isn't started yet! Call start() before use.");
    } else if (scheduler.isShutdown()) {
      throw new VeniceException("NativeMetadataRepository has already been cleared and shutdown!");
    }
  }

  public static NativeMetadataRepository getInstance(ClientConfig clientConfig, VeniceProperties backendConfig) {
    return getInstance(clientConfig, backendConfig, null);
  }

  public static NativeMetadataRepository getInstance(
      ClientConfig clientConfig,
      VeniceProperties backendConfig,
      ICProvider icProvider) {
    LOGGER.info(
        "Initializing {} with {}",
        NativeMetadataRepository.class.getSimpleName(),
        ThinClientMetaStoreBasedRepository.class.getSimpleName());
    return new ThinClientMetaStoreBasedRepository(clientConfig, backendConfig, icProvider);
  }

  @Override
  public void subscribe(String storeName) throws InterruptedException {
    throwIfNotStartedOrCleared();
    if (!subscribedStoreMap.containsKey(storeName)) {
      refreshOneStore(storeName);
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
    try {
      getAndSetStoreConfigFromSystemStore(storeName);
      StoreConfig storeConfig = storeConfigMap.get(storeName);
      if (storeConfig == null) {
        throw new VeniceException("StoreConfig is missing unexpectedly for store: " + storeName);
      }
      Store newStore = getStoreFromSystemStore(storeName, storeConfig.getCluster());
      // isDeleting check to detect deleted store is only supported by meta system store based implementation.
      if (newStore != null && !storeConfig.isDeleting()) {
        putStore(newStore);
        getAndCacheSchemaDataFromSystemStore(storeName);
      } else {
        removeStore(storeName);
      }
      return newStore;
    } catch (ServiceDiscoveryException | MissingKeyInStoreMetadataException e) {
      throw new VeniceNoStoreException(storeName, e);
    }
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
    SchemaData schemaData = getSchemaDataFromReadThroughCache(storeName);
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
    return valueSchema != null;
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
    SchemaData schemaData = getSchemaDataFromReadThroughCache(storeName);
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
    SchemaData schemaData = getSchemaDataFromReadThroughCache(storeName);
    return schemaData.getValueSchemas();
  }

  @Override
  public SchemaEntry getSupersetOrLatestValueSchema(String storeName) {
    SchemaData schemaData = getSchemaDataFromReadThroughCache(storeName);
    int latestValueSchemaId = getSupersetSchemaID(storeName);

    if (latestValueSchemaId == SchemaData.INVALID_VALUE_SCHEMA_ID) {
      latestValueSchemaId = schemaData.getMaxValueSchemaId();
    }

    if (latestValueSchemaId == SchemaData.INVALID_VALUE_SCHEMA_ID) {
      throw new VeniceException(storeName + " doesn't have latest schema!");
    }
    return schemaData.getValueSchema(latestValueSchemaId);
  }

  @Override
  public SchemaEntry getSupersetSchema(String storeName) {
    SchemaData schemaData = getSchemaDataFromReadThroughCache(storeName);

    int supersetSchemaID = getSupersetSchemaID(storeName);
    return schemaData.getValueSchema(supersetSchemaID);
  }

  private int getSupersetSchemaID(String storeName) {
    Store store = getStoreOrThrow(storeName);
    return store.getLatestSuperSetValueSchemaId();
  }

  @Override
  public GeneratedSchemaID getDerivedSchemaId(String storeName, String derivedSchemaStr) {
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

  public RmdSchemaEntry getReplicationMetadataSchema(
      String storeName,
      int valueSchemaId,
      int replicationMetadataVersionId) {
    throw new VeniceException("Function: getReplicationMetadataSchema is not supported!");
  }

  @Override
  public Collection<RmdSchemaEntry> getReplicationMetadataSchemas(String storeName) {
    throw new VeniceException("Function: getReplicationMetadataSchemas is not supported!");
  }

  /**
   * This method will be triggered periodically to keep the store/schema information up-to-date.
   */
  @Override
  public void refresh() {
    LOGGER.debug("Refresh started for {}", getClass().getSimpleName());
    for (String storeName: subscribedStoreMap.keySet()) {
      try {
        refreshOneStore(storeName);
      } catch (Exception e) {
        // Catch all exceptions here so the scheduled periodic refresh doesn't break and transient errors can be
        // retried.
        LOGGER.warn("Caught an exception when trying to refresh {}", getClass().getSimpleName(), e);
      }
    }
    LOGGER.debug("Refresh finished for {}", getClass().getSimpleName());
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

  protected abstract StoreMetaValue getStoreMetaValue(String storeName, StoreMetaKey key);

  // Helper function with common code for retrieving StoreConfig from meta system store.
  protected StoreConfig getStoreConfigFromMetaSystemStore(String storeName) {
    StoreClusterConfig clusterConfig = getStoreMetaValue(
        storeName,
        MetaStoreDataType.STORE_CLUSTER_CONFIG
            .getStoreMetaKey(Collections.singletonMap(KEY_STRING_STORE_NAME, storeName))).storeClusterConfig;
    return new StoreConfig(clusterConfig);
  }

  // Helper function with common code for retrieving SchemaData from meta system store.
  protected SchemaData getSchemaDataFromMetaSystemStore(String storeName) {
    SchemaData schemaData = schemaMap.get(storeName);
    SchemaEntry keySchema;
    if (schemaData == null) {
      // Retrieve the key schema and initialize SchemaData only if it's not cached yet.
      StoreMetaKey keySchemaKey = MetaStoreDataType.STORE_KEY_SCHEMAS
          .getStoreMetaKey(Collections.singletonMap(KEY_STRING_STORE_NAME, storeName));
      Map<CharSequence, CharSequence> keySchemaMap =
          getStoreMetaValue(storeName, keySchemaKey).storeKeySchemas.keySchemaMap;
      if (keySchemaMap.isEmpty()) {
        throw new VeniceException("No key schema found for store: " + storeName);
      }
      Map.Entry<CharSequence, CharSequence> keySchemaEntry = keySchemaMap.entrySet().iterator().next();
      keySchema =
          new SchemaEntry(Integer.parseInt(keySchemaEntry.getKey().toString()), keySchemaEntry.getValue().toString());
      schemaData = new SchemaData(storeName, keySchema);
    }
    StoreMetaKey valueSchemaKey = MetaStoreDataType.STORE_VALUE_SCHEMAS
        .getStoreMetaKey(Collections.singletonMap(KEY_STRING_STORE_NAME, storeName));
    Map<CharSequence, CharSequence> valueSchemaMap =
        getStoreMetaValue(storeName, valueSchemaKey).storeValueSchemas.valueSchemaMap;
    // Check the value schema string, if it's empty then try to query the other key space for individual value schema.
    for (Map.Entry<CharSequence, CharSequence> entry: valueSchemaMap.entrySet()) {
      // Check if we already have the corresponding value schema
      int valueSchemaId = Integer.parseInt(entry.getKey().toString());
      if (schemaData.getValueSchema(valueSchemaId) != null) {
        continue;
      }
      if (entry.getValue().toString().isEmpty()) {
        // The value schemas might be too large to be stored in a single K/V.
        StoreMetaKey individualValueSchemaKey =
            MetaStoreDataType.STORE_VALUE_SCHEMA.getStoreMetaKey(new HashMap<String, String>() {
              {
                put(KEY_STRING_STORE_NAME, storeName);
                put(KEY_STRING_SCHEMA_ID, entry.getKey().toString());
              }
            });
        // Empty string is not a valid value schema therefore it's safe to throw exceptions if we also cannot find it in
        // the individual value schema key space.
        String valueSchema =
            getStoreMetaValue(storeName, individualValueSchemaKey).storeValueSchema.valueSchema.toString();
        schemaData.addValueSchema(new SchemaEntry(valueSchemaId, valueSchema));
      } else {
        schemaData.addValueSchema(new SchemaEntry(valueSchemaId, entry.getValue().toString()));
      }
    }
    return schemaData;
  }

  protected Store putStore(Store newStore) {
    // Workaround to make old metadata compatible with new fields
    newStore.fixMissingFields();
    Store oldStore = subscribedStoreMap.put(newStore.getName(), newStore);
    if ((oldStore == null) || (!oldStore.equals(newStore))) {
      long previousStoreReadQuota = oldStore == null ? 0 : oldStore.getReadQuotaInCU();
      totalStoreReadQuota.addAndGet(newStore.getReadQuotaInCU() - previousStoreReadQuota);
      notifyStoreChanged(newStore);
    }
    return oldStore;
  }

  protected Store removeStore(String storeName) {
    // Remove the store name from the subscription.
    Store oldStore = subscribedStoreMap.remove(storeName);
    if (oldStore != null) {
      totalStoreReadQuota.addAndGet(-oldStore.getReadQuotaInCU());
      notifyStoreDeleted(oldStore);
    }
    removeStoreSchema(storeName);
    return oldStore;
  }

  protected void notifyStoreCreated(Store store) {
    for (StoreDataChangedListener listener: listeners) {
      try {
        listener.handleStoreCreated(store);
      } catch (Throwable e) {
        LOGGER.error("Could not handle store creation event for store: {}", store.getName(), e);
      }
    }
  }

  protected void notifyStoreDeleted(Store store) {
    for (StoreDataChangedListener listener: listeners) {
      try {
        listener.handleStoreDeleted(store);
      } catch (Throwable e) {
        LOGGER.error("Could not handle store deletion event for store: {}", store.getName(), e);
      }
    }
  }

  protected void notifyStoreChanged(Store store) {
    for (StoreDataChangedListener listener: listeners) {
      try {
        listener.handleStoreChanged(store);
      } catch (Throwable e) {
        LOGGER.error("Could not handle store updating event for store: {}", store.getName(), e);
      }
    }
  }

  protected SchemaData getAndCacheSchemaDataFromSystemStore(String storeName) {
    if (!hasStore(storeName)) {
      throw new VeniceNoStoreException(storeName);
    }
    SchemaData schemaData = getSchemaDataFromSystemStore(storeName);
    schemaMap.put(storeName, schemaData);
    return schemaData;
  }

  /**
   * @return the {@link SchemaData} associated with this store. Guaranteed to be not null or to throw.
   * @throws VeniceNoStoreException is the store is not found
   */
  private SchemaData getSchemaDataFromReadThroughCache(String storeName) throws VeniceNoStoreException {
    SchemaData schemaData = schemaMap.get(storeName);
    if (schemaData == null) {
      schemaData = getAndCacheSchemaDataFromSystemStore(storeName);
    }
    return schemaData;
  }

  protected SchemaEntry getValueSchemaInternally(String storeName, int id) {
    SchemaData schemaData = getSchemaDataFromReadThroughCache(storeName);
    if (schemaData == null) {
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
