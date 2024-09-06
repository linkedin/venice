package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.InvalidVeniceSchemaException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.schema.GeneratedSchemaID;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.utils.RetryUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.helix.zookeeper.zkclient.IZkChildListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is used to cache store schema and provide various query operations.
 * This expected user is router to support venice client schema query operations,
 * and storage node for schema validation;
 *
 * TODO:
 * 1. This class needs to periodically clear/warm up local cache since we might miss some notifications:
 *  1.` Re-connect;
 * One way to achieve to record the previous clear timestamp, and clear it again when the duration
 * exceeds some amount of time in query functions.
 * We need to reach out Helix team for this issue since it will impact Helix cache as well.
 *
 */
public class HelixReadOnlySchemaRepository implements ReadOnlySchemaRepository, StoreDataChangedListener {
  private static final Logger logger = LogManager.getLogger(HelixReadOnlySchemaRepository.class);
  public static final int VALUE_SCHEMA_STARTING_ID = 1;

  private static final Duration FORCE_REFRESH_SUPERSET_SCHEMA_MAX_DELAY_DEFAULT = Duration.ofMinutes(1);
  private static Duration FORCE_REFRESH_SUPERSET_SCHEMA_MAX_DELAY = FORCE_REFRESH_SUPERSET_SCHEMA_MAX_DELAY_DEFAULT;

  /**
   * Local cache between store name and store schema.
   */
  private final Map<String, SchemaData> schemaMap = new VeniceConcurrentHashMap<>();

  private final ZkClient zkClient;
  private final HelixSchemaAccessor accessor;
  private final CachedResourceZkStateListener zkStateListener;

  // Store repository to check store related info
  private final ReadOnlyStoreRepository storeRepository;

  // Listener to handle adding key/value schema
  private final IZkChildListener keySchemaChildListener = new KeySchemaChildListener();
  private final IZkChildListener valueSchemaChildListener = new ValueSchemaChildListener();
  private final IZkChildListener derivedSchemaChildListener = new DerivedSchemaChildListener();
  private final IZkChildListener replicationMetadataSchemaChildListener = new ReplicationMetadataSchemaChildListener();

  // Mutex for local cache
  private final ReadWriteLock schemaLock = new ReentrantReadWriteLock();

  /**
   * Package-private for use in tests. DO NOT CALL IN MAIN CODE!
   */
  static void setForceRefreshSupersetSchemaMaxDelay(Duration duration) {
    FORCE_REFRESH_SUPERSET_SCHEMA_MAX_DELAY = duration;
  }

  /**
   * Package-private for use in tests. DO NOT CALL IN MAIN CODE!
   */
  static void resetForceRefreshSupersetSchemaMaxDelay() {
    FORCE_REFRESH_SUPERSET_SCHEMA_MAX_DELAY = FORCE_REFRESH_SUPERSET_SCHEMA_MAX_DELAY_DEFAULT;
  }

  public HelixReadOnlySchemaRepository(
      ReadOnlyStoreRepository storeRepository,
      ZkClient zkClient,
      HelixAdapterSerializer adapter,
      String clusterName,
      int refreshAttemptsForZkReconnect,
      long refreshIntervalForZkReconnectInMs) {
    this.storeRepository = storeRepository;
    this.zkClient = zkClient;
    this.accessor = new HelixSchemaAccessor(
        zkClient,
        adapter,
        clusterName,
        refreshAttemptsForZkReconnect,
        refreshIntervalForZkReconnectInMs);

    storeRepository.registerStoreDataChangedListener(this);
    this.zkStateListener =
        new CachedResourceZkStateListener(this, refreshAttemptsForZkReconnect, refreshIntervalForZkReconnectInMs);
  }

  /** test-only */
  HelixReadOnlySchemaRepository(
      ReadOnlyStoreRepository storeRepository,
      ZkClient zkClient,
      HelixSchemaAccessor accessor,
      int refreshAttemptsForZkReconnect,
      long refreshIntervalForZkReconnectInMs) {
    this.storeRepository = storeRepository;
    this.zkClient = zkClient;
    this.accessor = accessor;
    storeRepository.registerStoreDataChangedListener(this);
    this.zkStateListener =
        new CachedResourceZkStateListener(this, refreshAttemptsForZkReconnect, refreshIntervalForZkReconnectInMs);
  }

  /**
   * This function will do the following steps:
   * 1. If store doesn't exist, return directly;
   * 2. If store does exist:
   * 2.1 If local cache doesn't have schema for it, fetch them from Zookeeper and setup watches if necessary;
   * 2.2 If local cache has related schema entry, return directly;
   * In this way, we can slowly fill local cache triggered by request to reduce peak qps of Zookeeper;
   *
   */
  private SchemaData getSchemaDataFromCacheOrFetch(String storeName) {
    Store store = getStoreRepository().getStoreOrThrow(storeName);
    getSchemaLock().readLock().lock();
    try {
      /**
       * This must be wrapped inside the read lock scope since it is possible
       * that some other thread could update the schema map asynchronously in between,
       * such as clearing the map during {@link #refresh()},
       * which could cause this function throw {@link VeniceNoStoreException}.
       */
      SchemaData schemaData = populateSchemaMap(storeName, store);
      if (schemaData == null) {
        throw new VeniceNoStoreException(storeName);
      }
      return schemaData;
    } finally {
      getSchemaLock().readLock().unlock();
    }
  }

  private Object doSchemaOperation(String storeName, Function<SchemaData, Object> operation) {
    SchemaData schemaData = getSchemaDataFromCacheOrFetch(storeName);
    return operation.apply(schemaData);
  }

  /**
   * @return true if the subscription was established (only if AA is enabled)
   */
  boolean maybeRegisterAndPopulateRmdSchema(Store store, SchemaData schemaData) {
    if (store.isActiveActiveReplicationEnabled()) {
      String storeName = store.getName();
      getAccessor().subscribeReplicationMetadataSchemaCreationChange(storeName, replicationMetadataSchemaChildListener);
      getAccessor().getAllReplicationMetadataSchemas(storeName).forEach(schemaData::addReplicationMetadataSchema);
      return true;
    }
    return false;
  }

  /**
   * @return true if the subscription was established (only if WC is enabled)
   */
  boolean maybeRegisterAndPopulateUpdateSchema(Store store, SchemaData schemaData) {
    if (store.isWriteComputationEnabled()) {
      String storeName = store.getName();
      getAccessor().subscribeDerivedSchemaCreationChange(storeName, derivedSchemaChildListener);
      getAccessor().getAllDerivedSchemas(storeName).forEach(schemaData::addDerivedSchema);
      return true;
    }
    return false;
  }

  SchemaEntry forceRefreshSupersetSchemaWithRetry(String storeName) {
    Store store = getStoreRepository().getStore(storeName);
    int supersetSchemaId = store.getLatestSuperSetValueSchemaId();
    AtomicReference<SchemaEntry> supersetSchemaEntry = new AtomicReference<>();
    long currentTimestamp = System.currentTimeMillis();
    List<Class<? extends Throwable>> retriableExceptions =
        Collections.singletonList(InvalidVeniceSchemaException.class);
    RetryUtils.executeWithMaxAttemptAndExponentialBackoff(() -> {
      try {
        getSchemaLock().writeLock().lock();
        SchemaData schemaData = getSchemaMap().get(storeName);
        forceRefreshSchemaData(store, schemaData);
        if (!isSupersetSchemaReadyToServe(store, schemaData, supersetSchemaId)) {
          throw new InvalidVeniceSchemaException(
              "Unable to refresh superset schema id: " + supersetSchemaId + " for store: " + store.getName());
        }
        supersetSchemaEntry.set(schemaData.getValueSchema(supersetSchemaId));
      } finally {
        getSchemaLock().writeLock().unlock();
      }
    }, 10, Duration.ofSeconds(1), FORCE_REFRESH_SUPERSET_SCHEMA_MAX_DELAY, Duration.ofMinutes(5), retriableExceptions);
    long timePassed = System.currentTimeMillis() - currentTimestamp;
    logger.info(
        "Obtain superset schema id: {} for store {} with time in milliseconds: {}.",
        supersetSchemaId,
        storeName,
        timePassed);
    return supersetSchemaEntry.get();
  }

  boolean isSupersetSchemaReadyToServe(Store store, SchemaData schemaData, int supersetSchemaId) {
    if (schemaData.getValueSchema(supersetSchemaId) == null) {
      logger.warn("Superset schema ID: {} for store: {} not found in schema cache.", supersetSchemaId, store.getName());
      return false;
    }
    if (store.isWriteComputationEnabled() && !schemaData.hasUpdateSchema(supersetSchemaId)) {
      logger.warn(
          "Update schema of superset schema ID: {} for store: {} not found in schema cache.",
          supersetSchemaId,
          store.getName());
      return false;
    }
    if (store.isActiveActiveReplicationEnabled() && !schemaData.hasRmdSchema(supersetSchemaId)) {
      logger.warn(
          "RMD schema of superset schema ID: {} for store: {} not found in schema cache.",
          supersetSchemaId,
          store.getName());
      return false;
    }
    return true;
  }

  void forceRefreshSchemaData(Store store, SchemaData schemaData) {
    String storeName = store.getName();
    getAccessor().getAllValueSchemas(storeName).forEach(schemaData::addValueSchema);
    if (store.isWriteComputationEnabled()) {
      getAccessor().getAllDerivedSchemas(storeName).forEach(schemaData::addDerivedSchema);
    }
    if (store.isActiveActiveReplicationEnabled()) {
      getAccessor().getAllReplicationMetadataSchemas(storeName).forEach(schemaData::addReplicationMetadataSchema);
    }
  }

  /**
   * This function is used to retrieve key schema for the given store.
   * If store doesn't exist, this function will return null;
   * If key schema for the given store doesn't exist, will return null;
   * Otherwise, it will return the key schema;
   *
   * Caller shouldn't modify the returned SchemeEntry
   *
   * @throws {@link com.linkedin.venice.exceptions.VeniceNoStoreException} if the store doesn't exist;
   * @return
   *    null, if key schema for the given store doesn't exist;
   *    key schema entry, otherwise;
   */
  @Override
  public SchemaEntry getKeySchema(String storeName) {
    SchemaData schemaData = getSchemaDataFromCacheOrFetch(storeName);
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

  private SchemaEntry getValueSchemaInternally(String storeName, int id) {
    SchemaData schemaData = getSchemaDataFromCacheOrFetch(storeName);
    return schemaData.getValueSchema(id);
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
    SchemaData schemaData = getSchemaDataFromCacheOrFetch(storeName);
    // Could throw SchemaParseException
    SchemaEntry valueSchema = new SchemaEntry(SchemaData.INVALID_VALUE_SCHEMA_ID, valueSchemaStr);
    return schemaData.getSchemaID(valueSchema);
  }

  @Override
  public GeneratedSchemaID getDerivedSchemaId(String storeName, String derivedSchemaStr) {
    SchemaData schemaData = getSchemaDataFromCacheOrFetch(storeName);
    return schemaData.getDerivedSchemaId(derivedSchemaStr);
  }

  @Override
  public DerivedSchemaEntry getDerivedSchema(String storeName, int valueSchemaId, int derivedSchemaId) {
    SchemaData schemaData = getSchemaDataFromCacheOrFetch(storeName);
    return schemaData.getDerivedSchema(valueSchemaId, derivedSchemaId);
  }

  /**
   * This function is used to retrieve all the value schemas for the given store.
   *
   * Caller shouldn't modify the returned SchemeEntry list.
   *
   * @throws {@link com.linkedin.venice.exceptions.VeniceNoStoreException} if the store doesn't exist;
   * @return value schema list
   */
  @Override
  public Collection<SchemaEntry> getValueSchemas(String storeName) {
    SchemaData schemaData = getSchemaDataFromCacheOrFetch(storeName);
    return schemaData.getValueSchemas();
  }

  @Override
  public Collection<DerivedSchemaEntry> getDerivedSchemas(String storeName) {
    SchemaData schemaData = getSchemaDataFromCacheOrFetch(storeName);
    return schemaData.getDerivedSchemas();
  }

  /**
   * Caller shouldn't modify the returned SchemeEntry
   */
  @Override
  public SchemaEntry getSupersetOrLatestValueSchema(String storeName) {
    SchemaData schemaData = getSchemaDataFromCacheOrFetch(storeName);
    Integer supersetSchemaID = getSupersetSchemaID(storeName);
    int latestValueSchemaId = supersetSchemaID;
    if (latestValueSchemaId == SchemaData.INVALID_VALUE_SCHEMA_ID) {
      latestValueSchemaId = schemaData.getMaxValueSchemaId();
      if (latestValueSchemaId == SchemaData.INVALID_VALUE_SCHEMA_ID) {
        throw new VeniceException(storeName + " doesn't have latest schema!");
      }
    }
    return schemaData.getValueSchema(latestValueSchemaId);
  }

  @Override
  public SchemaEntry getSupersetSchema(String storeName) {
    SchemaData schemaData = getSchemaDataFromCacheOrFetch(storeName);
    Integer supersetSchemaId = getSupersetSchemaID(storeName);
    if (supersetSchemaId == SchemaData.INVALID_VALUE_SCHEMA_ID) {
      return null;
    }
    if (isSupersetSchemaReadyToServe(getStoreRepository().getStore(storeName), schemaData, supersetSchemaId)) {
      return schemaData.getValueSchema(supersetSchemaId);
    }
    // When superset schema exist, but corresponding schema is not ready, we will force refresh the schema and retrieve
    // the update.
    return forceRefreshSupersetSchemaWithRetry(storeName);
  }

  private Integer getSupersetSchemaID(String storeName) {
    Store store = getStoreRepository().getStoreOrThrow(storeName);
    return store.getLatestSuperSetValueSchemaId();
  }

  @Override
  public DerivedSchemaEntry getLatestDerivedSchema(String storeName, int valueSchemaId) {
    return (DerivedSchemaEntry) doSchemaOperation(storeName, (schemaData -> {
      Optional<DerivedSchemaEntry> latestDerivedSchemaEntry = schemaData.getDerivedSchemas()
          .stream()
          .filter(entry -> entry.getValueSchemaID() == valueSchemaId)
          .max(Comparator.comparing(DerivedSchemaEntry::getId));

      if (!latestDerivedSchemaEntry.isPresent()) {
        throw new VeniceException(
            "Cannot find latest schema for store: " + storeName + ", value schema id: " + valueSchemaId);
      }

      return latestDerivedSchemaEntry.get();
    }));
  }

  @Override
  public RmdSchemaEntry getReplicationMetadataSchema(
      String storeName,
      int valueSchemaId,
      int replicationMetadataVersionId) {
    return (RmdSchemaEntry) doSchemaOperation(
        storeName,
        ((schemaData) -> schemaData.getReplicationMetadataSchema(valueSchemaId, replicationMetadataVersionId)));
  }

  @Override
  public Collection<RmdSchemaEntry> getReplicationMetadataSchemas(String storeName) {
    return (Collection<RmdSchemaEntry>) doSchemaOperation(
        storeName,
        ((schemaData) -> schemaData.getReplicationMetadataSchemas()));
  }

  /**
   * Refer to {@link HelixReadOnlySchemaRepository#clear()}
   *
   * This function will clear the local cache/watches, and populates the schemaMap from schemaRepository.
   */
  @Override
  public void refresh() {
    // Should guard the following with write-lock as other-thread could be reading the schema from the map
    // and might throw VeniceNoStoreException.
    schemaLock.writeLock().lock();
    try {
      Set<String> storeNameSet = schemaMap.keySet();
      logger.info("Starting to refresh schema map. Initial store count: {}", storeNameSet.size());
      storeNameSet.forEach(this::removeStoreSchemaFromLocal);
      schemaMap.clear();
      zkClient.subscribeStateChanges(zkStateListener);
      List<Store> stores = storeRepository.getAllStores();
      for (Store store: stores) {
        populateSchemaMap(store.getName(), store);
      }
      logger.info("Finished refreshing schema map. Final store count: {}", stores.size());
    } finally {
      schemaLock.writeLock().unlock();
    }
  }

  /**
   * Use {@link VeniceConcurrentHashMap#computeIfAbsent} here instead of {@link #schemaLock} to avoid the complication of
   * readlock/writelock switching/degrading.
   * You can get more details from the 'CachedData' example in {@link ReentrantReadWriteLock}.
   */
  private SchemaData populateSchemaMap(String storeName, Store store) {
    return getSchemaMap().computeIfAbsent(storeName, k -> {
      // Gradually warm up
      // If the local cache doesn't have the schema entry for this store,
      // it could be added recently, and we need to add/monitor it locally

      // Fetch key schema
      // Since key schema are not mutated (not even the child zk path) there is no need to set watches
      accessor.subscribeKeySchemaCreationChange(storeName, keySchemaChildListener);
      SchemaData schemaData = new SchemaData(storeName, accessor.getKeySchema(storeName));

      // Fetch value schema
      accessor.subscribeValueSchemaCreationChange(storeName, valueSchemaChildListener);
      accessor.getAllValueSchemas(storeName).forEach(schemaData::addValueSchema);

      // Fetch derived schemas if they are existing
      boolean updateSchemaListener = maybeRegisterAndPopulateUpdateSchema(store, schemaData);
      boolean rmdSchemaListener = maybeRegisterAndPopulateRmdSchema(store, schemaData);

      logger.info(
          "Fetched and subscribed listeners for key/value{}{} schema data of store: {}.",
          updateSchemaListener ? "/update" : "",
          rmdSchemaListener ? "/RMD" : "",
          storeName);

      return schemaData;
    });
  }

  /**
   * Clear local cache and watches
   */
  @Override
  public void clear() {
    // un-subscribe is thread safe method
    zkClient.unsubscribeStateChanges(zkStateListener);
    schemaLock.writeLock().lock();
    try {
      Set<String> storeNameSet = schemaMap.values().stream().map(SchemaData::getStoreName).collect(Collectors.toSet());
      storeNameSet.forEach(this::removeStoreSchemaFromLocal);
    } finally {
      schemaLock.writeLock().unlock();
    }
  }

  /**
   * This function is used to remove schema entry for the given store from local cache,
   * and related listeners as well.
   *
   * @param storeName
   */
  private void removeStoreSchemaFromLocal(String storeName) {
    schemaLock.writeLock().lock();
    try {
      SchemaData previous = schemaMap.remove(storeName);
      if (previous == null) {
        return;
      }
      accessor.unsubscribeKeySchemaCreationChange(storeName, keySchemaChildListener);
      accessor.unsubscribeValueSchemaCreationChange(storeName, valueSchemaChildListener);
      accessor.unsubscribeDerivedSchemaCreationChanges(storeName, derivedSchemaChildListener);
      accessor.unsubscribeReplicationMetadataSchemaCreationChanges(storeName, replicationMetadataSchemaChildListener);
      logger.info(
          "Removed from local cache and unsubscribed listeners for key/value/update/RMD schemas of store: {}.",
          storeName);
    } finally {
      schemaLock.writeLock().unlock();
    }
  }

  /**
   * zkPath: /cluster-name/Stores/store-name/[key-schema|value-schema]
   */
  private String extractStoreNameFromSchemaPath(String zkPath) {
    String[] paths = zkPath.split("/");
    if (paths.length != 5) {
      return null;
    }
    return paths[3];
  }

  /**
   * Do nothing here, since we want to warm up local cache gradually.
   */
  @Override
  public void handleStoreCreated(Store store) {

  }

  /**
   * For store deletion, we need to delete the local cache entry right way,
   * otherwise the local cache may contain the stale entries for store-delete-and-add scenario.
   */
  @Override
  public void handleStoreDeleted(String storeName) {
    removeStoreSchemaFromLocal(storeName);
  }

  @Override
  public void handleStoreChanged(Store store) {
    String storeName = store.getName();
    SchemaData schemaData;
    // Keep under readlock as other threads could be updating (refresh) the map.
    schemaLock.readLock().lock();
    try {
      schemaData = populateSchemaMap(storeName, store);
    } finally {
      schemaLock.readLock().unlock();
    }
    maybeRegisterAndPopulateUpdateSchema(store, schemaData);
    maybeRegisterAndPopulateRmdSchema(store, schemaData);
  }

  private class KeySchemaChildListener extends SchemaChildListener {
    @Override
    void handleSchemaChanges(String storeName, List<String> currentChildren) {
      schemaMap.get(storeName).setKeySchema(accessor.getKeySchema(storeName));
    }
  }

  class ValueSchemaChildListener extends SchemaChildListener {
    @Override
    void handleSchemaChanges(String storeName, List<String> currentChildren) {
      SchemaData schemaData = getSchemaMap().get(storeName);
      Set<Integer> schemaSet = new HashSet<>();

      for (String id: currentChildren) {
        int schemaId = Integer.parseInt(id);

        if (schemaData.getValueSchema(Integer.parseInt(id)) == null) {
          schemaData.addValueSchema(getSchemaAccessor().getValueSchema(storeName, id));
        }
        schemaSet.add(schemaId);
      }
      for (SchemaEntry schemaEntry: schemaData.getValueSchemas()) {
        if (!schemaSet.contains(schemaEntry.getId())) {
          schemaData.deleteValueSchema(schemaEntry);
        }
      }
    }

    Map<String, SchemaData> getSchemaMap() {
      return schemaMap;
    }

    HelixSchemaAccessor getSchemaAccessor() {
      return accessor;
    }
  }

  private class DerivedSchemaChildListener extends SchemaChildListener {
    @Override
    void handleSchemaChanges(String storeName, List<String> currentChildren) {
      SchemaData schemaData = schemaMap.get(storeName);
      for (String derivedSchemaIdPairStr: currentChildren) {
        String[] ids = derivedSchemaIdPairStr.split(HelixSchemaAccessor.MULTIPART_SCHEMA_VERSION_DELIMITER);
        if (ids.length != 2) {
          throw new VeniceException(
              "unrecognized derivedSchema path format. Store: " + storeName + " path: " + derivedSchemaIdPairStr);
        }

        if (schemaData.getDerivedSchema(Integer.parseInt(ids[0]), Integer.parseInt(ids[1])) == null) {
          schemaData.addDerivedSchema(accessor.getDerivedSchema(storeName, derivedSchemaIdPairStr));
        }
      }
    }
  }

  private class ReplicationMetadataSchemaChildListener extends SchemaChildListener {
    @Override
    void handleSchemaChanges(String storeName, List<String> currentChildren) {
      SchemaData schemaData = schemaMap.get(storeName);
      for (String replicationMetadataVersionIdPairStr: currentChildren) {
        String[] ids =
            replicationMetadataVersionIdPairStr.split(HelixSchemaAccessor.MULTIPART_SCHEMA_VERSION_DELIMITER);
        if (ids.length != 2) {
          throw new VeniceException(
              "unrecognized Schema path format. Store: " + storeName + " path: " + replicationMetadataVersionIdPairStr);
        }

        if (schemaData.getReplicationMetadataSchema(Integer.parseInt(ids[0]), Integer.parseInt(ids[1])) == null) {
          schemaData.addReplicationMetadataSchema(
              accessor.getReplicationMetadataSchema(storeName, replicationMetadataVersionIdPairStr));
        }
      }
    }
  }

  private abstract class SchemaChildListener implements IZkChildListener {
    @Override
    public void handleChildChange(String parentPath, List<String> currentChildren) {
      String storeName = extractStoreNameFromSchemaPath(parentPath);
      if (storeName == null) {
        logger.error("Invalid schema path: {}.", parentPath);
        return;
      }

      if (currentChildren == null) {
        logger.info("currentChildren is null, which might be triggered by store deletion");
        return;
      }

      schemaLock.writeLock().lock();
      try {
        if (schemaMap.containsKey(storeName)) {
          handleSchemaChanges(storeName, currentChildren);
        } else {
          // Should not happen, since we will add the store entry locally when subscribe its child change
          logger.error("Local schemaMap is missing store entry: {}, which should not happen.", storeName);
        }
      } finally {
        schemaLock.writeLock().unlock();
      }
    }

    abstract void handleSchemaChanges(String storeName, List<String> currentChildren);
  }

  HelixSchemaAccessor getAccessor() {
    return accessor;
  }

  ReadOnlyStoreRepository getStoreRepository() {
    return storeRepository;
  }

  Map<String, SchemaData> getSchemaMap() {
    return schemaMap;
  }

  ReadWriteLock getSchemaLock() {
    return schemaLock;
  }
}
