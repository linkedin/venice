package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.schema.DerivedSchemaEntry;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;

import org.I0Itec.zkclient.IZkChildListener;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
  private final Logger logger = Logger.getLogger(HelixReadOnlySchemaRepository.class);

  public static final int VALUE_SCHEMA_STARTING_ID = 1;

  // Local cache between store name and store schema
  private Map<String, SchemaData> schemaMap = new VeniceConcurrentHashMap<>();

  private final ZkClient zkClient;
  private final HelixSchemaAccessor accessor;
  private final CachedResourceZkStateListener zkStateListener;

  // Store repository to check store related info
  private ReadOnlyStoreRepository storeRepository;

  // Listener to handle adding key/value schema
  private IZkChildListener keySchemaChildListener = new KeySchemaChildListener();
  private IZkChildListener valueSchemaChildListener = new ValueSchemaChildListener();
  private IZkChildListener derivedSchemaChildListener = new DerivedSchemaChildListener();

  // Mutex for local cache
  private final ReadWriteLock schemaLock = new ReentrantReadWriteLock();

  public HelixReadOnlySchemaRepository(ReadOnlyStoreRepository storeRepository, ZkClient zkClient,
      HelixAdapterSerializer adapter, String clusterName, int refreshAttemptsForZkReconnect,
      long refreshIntervalForZkReconnectInMs) {
    this.storeRepository = storeRepository;
    this.zkClient = zkClient;
    this.accessor = new HelixSchemaAccessor(zkClient, adapter, clusterName,
        refreshAttemptsForZkReconnect, refreshIntervalForZkReconnectInMs);

    storeRepository.registerStoreDataChangedListener(this);
    zkStateListener =
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
  private void fetchStoreSchemaIfNotInCache(String storeName) {
    if (!storeRepository.hasStore(storeName)) {
      throw new VeniceNoStoreException(storeName);
    }
    if (!schemaMap.containsKey(storeName)) {
      populateSchemaMap(storeName);
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
    schemaLock.readLock().lock();
    try {
      /**
       * {@link #fetchStoreSchemaIfNotInCache(String)} must be wrapped inside the read lock scope since it is possible
       * that some other thread could update the schema map asynchronously in between,
       * such as clearing the map during {@link #refresh()},
       * which could cause this function throw {@link VeniceNoStoreException}.
       */
      fetchStoreSchemaIfNotInCache(storeName);
      SchemaData schemaData = schemaMap.get(storeName);
      if (null == schemaData) {
        throw new VeniceNoStoreException(storeName);
      }
      SchemaEntry keySchema = schemaData.getKeySchema();

      return keySchema;
    } finally {
      schemaLock.readLock().unlock();
    }
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
    SchemaEntry valueSchema = getValueSchemaInternally(storeName, id);
    return valueSchema;
  }

  private SchemaEntry getValueSchemaInternally(String storeName, int id) {
    schemaLock.readLock().lock();
    try {
      /**
       * {@link #fetchStoreSchemaIfNotInCache(String)} must be wrapped inside the read lock scope since it is possible
       * that some other thread could update the schema map asynchronously in between,
       * such as clearing the map during {@link #refresh()},
       * which could cause this function throw {@link VeniceNoStoreException}.
       */
      fetchStoreSchemaIfNotInCache(storeName);
      SchemaData schemaData = schemaMap.get(storeName);
      if (null == schemaData) {
        throw new VeniceNoStoreException(storeName);
      }
      SchemaEntry valueSchema = schemaData.getValueSchema(id);
      return valueSchema;
    } finally {
      schemaLock.readLock().unlock();
    }
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
    schemaLock.readLock().lock();
    try {
      /**
       * {@link #fetchStoreSchemaIfNotInCache(String)} must be wrapped inside the read lock scope since it is possible
       * that some other thread could update the schema map asynchronously in between,
       * such as clearing the map during {@link #refresh()},
       * which could cause this function throw {@link VeniceNoStoreException}.
       */
      fetchStoreSchemaIfNotInCache(storeName);
      SchemaData schemaData = schemaMap.get(storeName);
      if (null == schemaData) {
        throw new VeniceNoStoreException(storeName);
      }
      // Could throw SchemaParseException
      SchemaEntry valueSchema = new SchemaEntry(SchemaData.INVALID_VALUE_SCHEMA_ID, valueSchemaStr);
      return schemaData.getSchemaID(valueSchema);
    } finally {
      schemaLock.readLock().unlock();
    }
  }

  @Override
  public Pair<Integer, Integer> getDerivedSchemaId(String storeName, String derivedSchemaStr) {
    schemaLock.readLock().lock();
    try {
      /**
       * {@link #fetchStoreSchemaIfNotInCache(String)} must be wrapped inside the read lock scope since it is possible
       * that some other thread could update the schema map asynchronously in between,
       * such as clearing the map during {@link #refresh()},
       * which could cause this function throw {@link VeniceNoStoreException}.
       */
      fetchStoreSchemaIfNotInCache(storeName);
      SchemaData schemaData = schemaMap.get(storeName);
      if (null == schemaData) {
        throw new VeniceNoStoreException(storeName);
      }
      DerivedSchemaEntry derivedSchemaEntry =
          new DerivedSchemaEntry(SchemaData.UNKNOWN_SCHEMA_ID, SchemaData.UNKNOWN_SCHEMA_ID, derivedSchemaStr);
      return schemaData.getDerivedSchemaId(derivedSchemaEntry);
    } finally {
      schemaLock.readLock().unlock();
    }
  }

  @Override
  public DerivedSchemaEntry getDerivedSchema(String storeName, int valueSchemaId, int derivedSchemaId) {
    schemaLock.readLock().lock();
    try {
      fetchStoreSchemaIfNotInCache(storeName);
      SchemaData schemaData = schemaMap.get(storeName);
      if (null == schemaData) {
        throw new VeniceNoStoreException(storeName);
      }

      return schemaData.getDerivedSchema(valueSchemaId, derivedSchemaId);
    } finally {
      schemaLock.readLock().unlock();
    }
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
    schemaLock.readLock().lock();
    try {
      /**
       * {@link #fetchStoreSchemaIfNotInCache(String)} must be wrapped inside the read lock scope since it is possible
       * that some other thread could update the schema map asynchronously in between,
       * such as clearing the map during {@link #refresh()},
       * which could cause this function throw {@link VeniceNoStoreException}.
       */
      fetchStoreSchemaIfNotInCache(storeName);
      SchemaData schemaData = schemaMap.get(storeName);
      if (null == schemaData) {
        throw new VeniceNoStoreException(storeName);
      }
      return schemaData.getValueSchemas();
    } finally {
      schemaLock.readLock().unlock();
    }
  }

  @Override
  public Collection<DerivedSchemaEntry> getDerivedSchemas(String storeName) {
    schemaLock.readLock().lock();
    try {
      /**
       * {@link #fetchStoreSchemaIfNotInCache(String)} must be wrapped inside the read lock scope since it is possible
       * that some other thread could update the schema map asynchronously in between,
       * such as clearing the map during {@link #refresh()},
       * which could cause this function throw {@link VeniceNoStoreException}.
       */
      fetchStoreSchemaIfNotInCache(storeName);
      SchemaData schemaData = schemaMap.get(storeName);
      if (null == schemaData) {
        throw new VeniceNoStoreException(storeName);
      }
      return schemaData.getDerivedSchemas();
    } finally {
      schemaLock.readLock().unlock();
    }
  }

  /**
   * Caller shouldn't modify the returned SchemeEntry
   */
  @Override
  public SchemaEntry getLatestValueSchema(String storeName) {
    schemaLock.readLock().lock();
    try {
      /**
       * {@link #fetchStoreSchemaIfNotInCache(String)} must be wrapped inside the read lock scope since it is possible
       * that some other thread could update the schema map asynchronously in between,
       * such as clearing the map during {@link #refresh()},
       * which could cause this function throw {@link VeniceNoStoreException}.
       */
      fetchStoreSchemaIfNotInCache(storeName);
      SchemaData schemaData = schemaMap.get(storeName);
      if (null == schemaData) {
        throw new VeniceNoStoreException(storeName);
      }
      Store store = storeRepository.getStore(storeName);
      int latestValueSchemaId;

      if (store.getLatestSuperSetValueSchemaId() != SchemaData.INVALID_VALUE_SCHEMA_ID) {
        latestValueSchemaId = store.getLatestSuperSetValueSchemaId();
      } else {
        latestValueSchemaId = schemaData.getMaxValueSchemaId();
      }
      if (latestValueSchemaId == SchemaData.INVALID_VALUE_SCHEMA_ID) {
        throw new VeniceException(storeName + " doesn't have latest schema!");
      }
      SchemaEntry valueSchema = schemaData.getValueSchema(latestValueSchemaId);
      return valueSchema;
    } finally {
      schemaLock.readLock().unlock();
    }
  }

  @Override
  public DerivedSchemaEntry getLatestDerivedSchema(String storeName, int valueSchemaId) {
    schemaLock.readLock().lock();
    try {
      /**
       * {@link #fetchStoreSchemaIfNotInCache(String)} must be wrapped inside the read lock scope since it is possible
       * that some other thread could update the schema map asynchronously in between,
       * such as clearing the map during {@link #refresh()},
       * which could cause this function throw {@link VeniceNoStoreException}.
       */
      fetchStoreSchemaIfNotInCache(storeName);
      SchemaData schemaData = schemaMap.get(storeName);
      if (null == schemaData) {
        throw new VeniceNoStoreException(storeName);
      }
      Optional<DerivedSchemaEntry> latestDerivedSchemaEntry = schemaData.getDerivedSchemas().stream()
          .filter(entry -> entry.getValueSchemaId() == valueSchemaId)
          .max(Comparator.comparing(DerivedSchemaEntry::getId));

      if (!latestDerivedSchemaEntry.isPresent()) {
        throw new VeniceException("Cannot find latest schema for store: " + storeName
            + ", value schema id: " + valueSchemaId);
      }

      return latestDerivedSchemaEntry.get();
    } finally {
      schemaLock.readLock().unlock();
    }
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
    logger.info("Starting to refresh schema map.");
    schemaLock.writeLock().lock();
    try {
      Set<String> storeNameSet = schemaMap.keySet();
      storeNameSet.forEach(this::removeStoreSchemaFromLocal);
      schemaMap.clear();
      zkClient.subscribeStateChanges(zkStateListener);
      List<Store> stores = storeRepository.getAllStores();
      for (Store store : stores) {
        String storeName = store.getName();
        populateSchemaMap(storeName);
      }
    } finally {
      schemaLock.writeLock().unlock();
    }
    logger.info("Finished refreshing schema map.");
  }

  /**
   * Use {@link VeniceConcurrentHashMap#computeIfAbsent} here instead of {@link #schemaLock} to avoid the complication of
   * readlock/writelock switching/degrading.
   * You can get more details from the 'CachedData' example in {@link ReentrantReadWriteLock}.
   */
  private void populateSchemaMap(String storeName) {
    schemaMap.computeIfAbsent(storeName, k -> {
      // Gradually warm up
      logger.info("Try to fetch schema data for store: " + storeName);
      // If the local cache doesn't have the schema entry for this store,
      // it could be added recently, and we need to add/monitor it locally
      SchemaData schemaData = new SchemaData(storeName);

      // Fetch key schema
      // Since key schema are not mutated (not even the child zk path) there is no need to set watches
      accessor.subscribeKeySchemaCreationChange(storeName, keySchemaChildListener);
      schemaData.setKeySchema(accessor.getKeySchema(storeName));

      // Fetch value schema
      accessor.subscribeValueSchemaCreationChange(storeName, valueSchemaChildListener);
      accessor.getAllValueSchemas(storeName).forEach(schemaData::addValueSchema);

      //Fetch derived schemas if they are existing
      Store store = storeRepository.getStore(storeName);
      if (store.isWriteComputationEnabled()) {
        accessor.subscribeDerivedSchemaCreationChange(storeName, derivedSchemaChildListener);
        accessor.getAllDerivedSchemas(storeName).forEach(schemaData::addDerivedSchema);
      }
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
      Set<String> storeNameSet = schemaMap.keySet();
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
      if (!schemaMap.containsKey(storeName)) {
        return;
      }
      logger.info("Remove schema for store locally: " + storeName);
      schemaMap.remove(storeName);
      accessor.unsubscribeKeySchemaCreationChange(storeName, keySchemaChildListener);
      accessor.unsubscribeValueSchemaCreationChange(storeName, valueSchemaChildListener);
      accessor.unsubscribeDerivedSchemaCreationChanges(storeName, derivedSchemaChildListener);
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
      schemaData = schemaMap.get(storeName);
      if (null == schemaData) { // Should not happen, safety check for rare race condition.
        populateSchemaMap(storeName);
      }
    } finally {
      schemaLock.readLock().unlock();
    }

    if (store.isWriteComputationEnabled()) {
      accessor.subscribeDerivedSchemaCreationChange(storeName, derivedSchemaChildListener);
      accessor.getAllDerivedSchemas(storeName).forEach(schemaData::addDerivedSchema);
    }
  }

  private class KeySchemaChildListener extends SchemaChildListener {
    @Override
    void handleSchemaChanges(String storeName, List<String> currentChildren) {
      schemaMap.get(storeName).setKeySchema(accessor.getKeySchema(storeName));
    }
  }

  private class ValueSchemaChildListener extends SchemaChildListener {
    @Override
    void handleSchemaChanges(String storeName, List<String> currentChildren) {
      SchemaData schemaData = schemaMap.get(storeName);

      for (String id : currentChildren) {
        if (null == schemaData.getValueSchema(Integer.parseInt(id))) {
          schemaData.addValueSchema(accessor.getValueSchema(storeName, id));
        }
      }
    }
  }

  private class DerivedSchemaChildListener extends SchemaChildListener {
    @Override
    void handleSchemaChanges(String storeName, List<String> currentChildren) {
      SchemaData schemaData = schemaMap.get(storeName);
      for (String derivedSchemaIdPairStr : currentChildren) {
        String [] ids = derivedSchemaIdPairStr.split(HelixSchemaAccessor.DERIVED_SCHEMA_DELIMITER);
        if (ids.length != 2) {
          throw new VeniceException("unrecognized derivedSchema path format. Store: " + storeName
           + " path: " + derivedSchemaIdPairStr);
        }

        if (null == schemaData.getDerivedSchema(Integer.valueOf(ids[0]), Integer.valueOf(ids[1]))) {
          schemaData.addDerivedSchema(accessor.getDerivedSchema(storeName, derivedSchemaIdPairStr));
        }
      }
    }
  }

  private abstract class SchemaChildListener implements IZkChildListener {
    @Override
    public void handleChildChange(String parentPath, List<String> currentChildren) {
      String storeName = extractStoreNameFromSchemaPath(parentPath);
      if (null == storeName) {
        logger.error("Invalid schema path: " + parentPath);
        return;
      }

      if (null == currentChildren) {
        logger.info("currentChildren is null, which might be triggered by store deletion");
        return;
      }

      schemaLock.writeLock().lock();
      try {
        if (schemaMap.containsKey(storeName)) {
          handleSchemaChanges(storeName, currentChildren);
        } else {
          // Should not happen, since we will add the store entry locally when subscribe its child change
          logger.error("Local schemaMap is missing store entry: " + storeName + ", which should not happen");
        }
      } finally {
        schemaLock.writeLock().unlock();
      }
    }

    abstract void handleSchemaChanges(String storeName, List<String> currentChildren);
  }

  // For test purpose
  protected ReadWriteLock getInternalReadWriteLock() {
    return this.schemaLock;
  }
}
