package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.VeniceSerializer;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.PathResourceRegistry;
import org.I0Itec.zkclient.IZkChildListener;
import org.apache.helix.AccessOption;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.log4j.Logger;

import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
public class HelixReadOnlySchemaRepository implements ReadOnlySchemaRepository {
  private final Logger logger = Logger.getLogger(HelixReadOnlySchemaRepository.class);
  // Key schema path name
  public static final String KEY_SCHEMA_PATH = "key-schema";
  // Value schema path name
  public static final String VALUE_SCHEMA_PATH = "value-schema";
  // Key schema id, can only be '1' since Venice only maintains one single key schema per store.
  public static final String KEY_SCHEMA_ID = "1";
  // Value schema starting id
  public static final int VALUE_SCHEMA_STARTING_ID = 1;

  // Local cache between store name and store schema
  private Map<String, SchemaData> schemaMap = new ConcurrentHashMap<>();

  private ZkBaseDataAccessor<SchemaEntry> dataAccessor;

  private final ZkClient zkClient;

  private final CachedResourceZkStateListener zkStateListener;

  // Store repository to check store related info
  private ReadOnlyStoreRepository storeRepository;

  // Venice cluster name
  private String clusterName;

  // Listener to handle adding key schema
  private IZkChildListener keySchemaChildListener = new KeySchemaChildListener();

  // Listener to handle adding value schema
  private IZkChildListener valueSchemaChildListener = new ValueSchemaChildListener();

  // Mutex for local cache
  private final ReadWriteLock schemaLock = new ReentrantReadWriteLock();

  public HelixReadOnlySchemaRepository(@NotNull ReadOnlyStoreRepository storeRepository,
                                       @NotNull ZkClient zkClient,
                                       @NotNull HelixAdapterSerializer adapter,
                                       @NotNull String clusterName) {
    // Comment out store repo refresh for now, need to revisit it later.
    // Initialize local store cache
    //storeRepository.refresh();
    this.storeRepository = storeRepository;
    this.clusterName = clusterName;
    // Register schema serializer
    registerSerializerForSchema(clusterName, zkClient, adapter);
    dataAccessor = new ZkBaseDataAccessor<>(zkClient);
    this.zkClient = zkClient;

    // Register store data change listener
    storeRepository.registerStoreDataChangedListener(this);
    zkStateListener = new CachedResourceZkStateListener(this);
  }

  public static void registerSerializerForSchema(String clusterName,
                                                 ZkClient zkClient,
                                                 HelixAdapterSerializer adapter) {
    // Register schema serializer
    String keySchemaPath = getKeySchemaPath(clusterName, PathResourceRegistry.WILDCARD_MATCH_ANY);
    String valueSchemaPath = getValueSchemaPath(clusterName, PathResourceRegistry.WILDCARD_MATCH_ANY,
        PathResourceRegistry.WILDCARD_MATCH_ANY);
    VeniceSerializer<SchemaEntry> serializer = new SchemaEntrySerializer();
    adapter.registerSerializer(keySchemaPath, serializer);
    adapter.registerSerializer(valueSchemaPath, serializer);
    zkClient.setZkSerializer(adapter);
  }

  private static String getStorePath(String clusterName, String storeName) {
    StringBuilder sb = new StringBuilder(HelixUtils.getHelixClusterZkPath(clusterName));
    sb.append(HelixReadOnlyStoreRepository.STORES_PATH)
        .append("/")
        .append(storeName)
        .append("/");
    return sb.toString();
  }

  /**
   * Get absolute key schema parent path for a given store
   *
   * @param clusterName
   * @param storeName
   * @return
   */
  public static String getKeySchemaParentPath(String clusterName, String storeName) {
    return getStorePath(clusterName, storeName) + KEY_SCHEMA_PATH;
  }

  /**
   * Get absolute key schema path for a given store
   *
   * @param clusterName
   * @param storeName
   * @return
   */
  public static String getKeySchemaPath(String clusterName, String storeName) {
    return getKeySchemaParentPath(clusterName, storeName) + "/" + KEY_SCHEMA_ID;
  }

  /**
   * Get absolute value schema parent path for a given store
   *
   * @param clusterName
   * @param storeName
   * @return
   */
  public static String getValueSchemaParentPath(String clusterName, String storeName) {
    return getStorePath(clusterName, storeName) + VALUE_SCHEMA_PATH;
  }

  /**
   * Get absolute value schema path for a given store and schema id
   *
   * @param clusterName
   * @param storeName
   * @param id
   * @return
   */
  public static String getValueSchemaPath(String clusterName, String storeName, String id) {
    return getValueSchemaParentPath(clusterName, storeName) + "/" + id;
  }

  /**
   * This function will do the following steps:
   * 1. If store doesn't exist, return directly;
   * 2. If store does exist:
   *  2.1 If local cache doesn't have schema for it, fetch them from Zookeeper and setup watches if necessary;
   *  2.2 If local cache has related schema entry, return directly;
   * In this way, we can slowly fill local cache triggered by request to reduce peak qps of Zookeeper;
   *
   * @param storeName
   */
  private void fetchStoreSchemaIfNotInCache(String storeName) {
    schemaLock.writeLock().lock();
    try {
      if (!storeRepository.hasStore(storeName)) {
        // It could be a non-existed store or deleted store,
        // remove the corresponding schemas from local map
        removeStoreSchemaFromLocal(storeName);
      } else if (!schemaMap.containsKey(storeName)) {
        // Gradually warm up
        logger.info("Try to fetch schema data for store: " + storeName);
        // If the local cache doesn't have the schema entry for this store,
        // it could be added recently, and we need to add/monitor it locally
        SchemaData schemaData = new SchemaData(storeName);
        // Fetch key schema
        String keySchemaParentPath = getKeySchemaParentPath(clusterName, storeName);
        dataAccessor.subscribeChildChanges(keySchemaParentPath, keySchemaChildListener);
        logger.info("Setup watcher for path: " + keySchemaParentPath);

        SchemaEntry keySchema = dataAccessor.get(getKeySchemaPath(clusterName, storeName), null, AccessOption.PERSISTENT);
        schemaData.setKeySchema(keySchema);

        // Fetch value schema
        String valueSchemaParentPath = getValueSchemaParentPath(clusterName, storeName);
        dataAccessor.subscribeChildChanges(valueSchemaParentPath, valueSchemaChildListener);
        logger.info("Setup watcher for path: " + valueSchemaParentPath);
        List<SchemaEntry> valueSchemas = dataAccessor.getChildren(valueSchemaParentPath, null, AccessOption.PERSISTENT);
        valueSchemas.forEach(schemaData::addValueSchema);
        schemaMap.put(storeName, schemaData);
      }
    } finally {
      schemaLock.writeLock().unlock();
    }
  }

  /**
   * This function is used to retrieve key schema for the given store.
   * If store doesn't exist, this function will return null;
   * If key schema for the given store doesn't exist, will return null;
   * Otherwise, it will return a cloned version of key schema;
   *
   * @throws {@link com.linkedin.venice.exceptions.VeniceNoStoreException} if the store doesn't exist;
   *
   * @param storeName
   * @return
   *    null, if key schema for the given store doesn't exist;
   *    cloned key schema entry, otherwise;
   */
  @Override
  public SchemaEntry getKeySchema(String storeName) {
    fetchStoreSchemaIfNotInCache(storeName);
    schemaLock.readLock().lock();
    try {
      SchemaData schemaData = schemaMap.get(storeName);
      if (null == schemaData) {
        throw new VeniceNoStoreException(storeName);
      }
      SchemaEntry keySchema = schemaData.getKeySchema();
      if (null == keySchema) {
        return null;
      }
      // Return cloned version since the cached schema should not be changed by caller
      return keySchema.clone();
    } finally {
      schemaLock.readLock().unlock();
    }
  }

  /**
   * This function is used to retrieve the value schema for the given store and value schema id.
   *
   * @throws {@link com.linkedin.venice.exceptions.VeniceNoStoreException} if the store doesn't exist;
   *
   * @param storeName
   * @param id
   * @return
   *    null, if the schema doesn't exist;
   *    cloned value schema entry, otherwise;
   */
  @Override
  public SchemaEntry getValueSchema(String storeName, int id) {
    SchemaEntry valueSchema = getValueSchemaInternally(storeName, id);
    if (null == valueSchema) {
      return null;
    }
    return valueSchema.clone();
  }

  private SchemaEntry getValueSchemaInternally(String storeName, int id) {
    fetchStoreSchemaIfNotInCache(storeName);
    schemaLock.readLock().lock();
    try {
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
   *
   * @param storeName
   * @param id
   * @return
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
   *
   * @param storeName
   * @param valueSchemaStr
   * @return
   *    {@link com.linkedin.venice.schema.SchemaData#INVALID_VALUE_SCHEMA_ID}, if the schema doesn't exist in the given store;
   *    schema id (int), if the schema exists in the given store
   */
  @Override
  public int getValueSchemaId(String storeName, String valueSchemaStr) {
    fetchStoreSchemaIfNotInCache(storeName);
    schemaLock.readLock().lock();
    try {
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

  /**
   * This function is used to retrieve all the value schemas for the given store.
   *
   * @throws {@link com.linkedin.venice.exceptions.VeniceNoStoreException} if the store doesn't exist;
   *
   * @param storeName
   * @return
   *    cloned list
   */
  @Override
  public Collection<SchemaEntry> getValueSchemas(String storeName) {
    fetchStoreSchemaIfNotInCache(storeName);
    schemaLock.readLock().lock();
    try {
      SchemaData schemaData = schemaMap.get(storeName);
      if (null == schemaData) {
        throw new VeniceNoStoreException(storeName);
      }
      return schemaData.cloneValueSchemas();
    } finally {
      schemaLock.readLock().unlock();
    }
  }

  /**
   * Refer to {@link HelixReadOnlySchemaRepository#clear()}
   *
   * This function will only clear the local cache/watches,
   * and the query operations will warm up local cache gradually.
   */
  @Override
  public void refresh() {
    clear();
    // subscribe is thread safe method.
    zkClient.subscribeStateChanges(zkStateListener);
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
      dataAccessor.unsubscribeChildChanges(getKeySchemaParentPath(clusterName, storeName), keySchemaChildListener);
      dataAccessor.unsubscribeChildChanges(getValueSchemaParentPath(clusterName, storeName), valueSchemaChildListener);
    } finally {
      schemaLock.writeLock().unlock();
    }
  }

  /**
   * zkPath: /cluster-name/Stores/store-name/[key-schema|value-schema]
   *
   * @param zkPath
   * @return
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
   *
   * @param store
   */
  @Override
  public void handleStoreCreated(Store store) {
  }

  /**
   * For store deletion, we need to delete the local cache entry right way,
   * otherwise the local cache may contain the stale entries for store-delete-and-add scenario.
   *
   * @param storeName
   */
  @Override
  public void handleStoreDeleted(String storeName) {
    removeStoreSchemaFromLocal(storeName);
  }

  private class KeySchemaChildListener implements IZkChildListener {
    /**
     * Children change listener, which is mostly for key schema setup.
     * This function won't handle key schema deletion, theoretically,
     * key schema can only be removed when the store is fully removed.
     * When the store is fully removed, it will remove everything under store folder,
     * and we don't do anything here.
     *
     * @param parentPath
     * @param currentChildren
     * @throws Exception
     */
    @Override
    public void handleChildChange(String parentPath, List<String> currentChildren) throws Exception {
      String storeName = extractStoreNameFromSchemaPath(parentPath);
      if (null == storeName) {
        logger.error("Invalid key schema path: " + parentPath);
        return;
      }
      if (null == currentChildren) {
        logger.info("currentChildren is null, which might be triggered by store deletion");
        return;
      }
      SchemaEntry keySchema = dataAccessor.get(getKeySchemaPath(clusterName, storeName), null, AccessOption.PERSISTENT);
      schemaLock.writeLock().lock();
      try {
        if (schemaMap.containsKey(storeName)) {
          SchemaData schemaData = schemaMap.get(storeName);
          schemaData.setKeySchema(keySchema);
        } else {
          // Should not happen, since we will add the store entry locally when subscribe its child change
          logger.error("Local schemaMap is missing store entry: " + storeName + ", which should not happen");
        }
      } finally {
        schemaLock.writeLock().unlock();
      }
    }
  }

  private class ValueSchemaChildListener implements IZkChildListener {
    /**
     * Children change listener, which is mostly for adding value schema.
     * This function won't handle value schema deletion, theoretically,
     * value schema can only be removed when the store is fully removed.
     * When the store is fully removed, it will remove everything under store folder,
     * and we don't do anything here.
     *
     * @param parentPath
     * @param currentChildren
     * @throws Exception
     */
    @Override
    public void handleChildChange(String parentPath, List<String> currentChildren) throws Exception {
      String storeName = extractStoreNameFromSchemaPath(parentPath);
      if (null == storeName) {
        logger.error("Invalid value schema path: " + parentPath);
        return;
      }
      if (null == currentChildren) {
        logger.info("currentChildren is null, which might be triggered by store deletion");
        return;
      }
      schemaLock.writeLock().lock();
      try {
        // Value schema can only be added, no deletion
        // Store deletion will be handled by every lookup, which will
        // remove the corresponding SchemaData entry
        if (schemaMap.containsKey(storeName)) {
          // Get new added value schema
          SchemaData schemaData = schemaMap.get(storeName);
          List<String> addedSchemaPath = new ArrayList();
          for (String id : currentChildren) {
            int valueSchemaId = Integer.parseInt(id);
            if (null == schemaData.getValueSchema(valueSchemaId)) {
              addedSchemaPath.add(getValueSchemaPath(clusterName, storeName, id));
            }
          }
          if (!addedSchemaPath.isEmpty()) {
            List<SchemaEntry> addedSchema = dataAccessor.get(addedSchemaPath, null, AccessOption.PERSISTENT);
            addedSchema.forEach(schemaData::addValueSchema);
          }
        } else {
          logger.error("Local schemaMap is missing store entry: " + storeName + ", which should not happen");
        }
      } finally {
        schemaLock.writeLock().unlock();
      }
    }
  }
}
