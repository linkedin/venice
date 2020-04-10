package com.linkedin.venice.storage;

import com.linkedin.venice.config.VeniceServerConfig;
import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.server.PartitionAssignmentRepository;
import com.linkedin.venice.server.StorageEngineRepository;
import com.linkedin.venice.server.VeniceConfigLoader;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.stats.AggVersionedBdbStorageEngineStats;
import com.linkedin.venice.stats.AggVersionedStorageEngineStats;
import com.linkedin.venice.stats.RocksDBMemoryStats;
import com.linkedin.venice.stats.RocksDBStats;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.StorageEngineFactory;
import com.linkedin.venice.store.bdb.BdbStorageEngineFactory;
import com.linkedin.venice.store.blackhole.BlackHoleStorageEngineFactory;
import com.linkedin.venice.store.memory.InMemoryStorageEngineFactory;
import com.linkedin.venice.store.rocksdb.RocksDBStorageEngineFactory;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Optional;
import java.util.Set;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.function.Consumer;
import org.rocksdb.Statistics;

import static com.linkedin.venice.meta.PersistenceType.*;


/**
 * Storage interface to Venice Server. Manages creation and deletion of of Storage engines
 * and Partitions.
 *
 * Use StorageEngineRepository, if read only access is desired for the Storage Engines.
 */
public class StorageService extends AbstractVeniceService {
  private static final Logger logger = Logger.getLogger(StorageService.class);

  private final StorageEngineRepository storageEngineRepository;
  private final VeniceConfigLoader configLoader;
  private final VeniceServerConfig serverConfig;

  private final Map<PersistenceType, StorageEngineFactory> persistenceTypeToStorageEngineFactoryMap;
  private final PartitionAssignmentRepository partitionAssignmentRepository;
  private final Consumer<String> storeVersionStateDeleter;

  private final AggVersionedStorageEngineStats aggVersionedStorageEngineStats;

  private final RocksDBMemoryStats rocksDBMemoryStats;

  public StorageService(VeniceConfigLoader configLoader, Consumer<String> storeVersionStateDeleter,
      AggVersionedBdbStorageEngineStats bdbStorageEngineStats, AggVersionedStorageEngineStats storageEngineStats, RocksDBMemoryStats rocksDBMemoryStats) {
    this.configLoader = configLoader;
    this.serverConfig = configLoader.getVeniceServerConfig();
    this.storageEngineRepository = new StorageEngineRepository();
    if (bdbStorageEngineStats != null) {
      this.storageEngineRepository.setAggBdbStorageEngineStats(bdbStorageEngineStats);
    }
    this.persistenceTypeToStorageEngineFactoryMap = new HashMap<>();
    this.partitionAssignmentRepository = new PartitionAssignmentRepository();
    this.storeVersionStateDeleter = storeVersionStateDeleter;
    this.aggVersionedStorageEngineStats = storageEngineStats;
    this.rocksDBMemoryStats = rocksDBMemoryStats;
    initInternalStorageEngineFactories();
    // Restore all the stores persisted previously
    restoreAllStores(configLoader);
  }

  public StorageService(VeniceConfigLoader configLoader, AggVersionedStorageEngineStats storageEngineStats) {
    this(configLoader, s -> {}, null, storageEngineStats, null);
  }

  /**
   * Initialize all the internal storage engine factories.
   * Please add it here if you want to add more.
   */
  private void initInternalStorageEngineFactories() {
    persistenceTypeToStorageEngineFactoryMap.put(BDB, new BdbStorageEngineFactory(serverConfig));
    persistenceTypeToStorageEngineFactoryMap.put(IN_MEMORY, new InMemoryStorageEngineFactory(serverConfig));
    persistenceTypeToStorageEngineFactoryMap.put(ROCKS_DB, new RocksDBStorageEngineFactory(serverConfig, rocksDBMemoryStats));
    persistenceTypeToStorageEngineFactoryMap.put(BLACK_HOLE, new BlackHoleStorageEngineFactory());
  }

  private void restoreAllStores(VeniceConfigLoader configLoader) {
    logger.info("Start restoring all the stores persisted previously");
    for (Map.Entry<PersistenceType, StorageEngineFactory> entry : persistenceTypeToStorageEngineFactoryMap.entrySet()) {
      PersistenceType pType = entry.getKey();
      StorageEngineFactory factory = entry.getValue();
      logger.info("Start restoring all the stores with type: " + pType);
      Set<String> storeNames = factory.getPersistedStoreNames();
      for (String storeName : storeNames) {
        logger.info("Start restoring store: " + storeName + " with type: " + pType);
        /**
         * Setup store-level persistence type based on current database setup.
         */
        VeniceStoreConfig storeConfig = configLoader.getStoreConfig(storeName, pType);
        AbstractStorageEngine storageEngine = openStore(storeConfig);
        Set<Integer> partitionIds = storageEngine.getPartitionIds();
        for (Integer partitionId : partitionIds) {
          partitionAssignmentRepository.addPartition(storeName, partitionId);
        }
        logger.info("Loaded the following partitions: " + Arrays.toString(partitionIds.toArray()) + ", for store: " + storeName);
        logger.info("Done restoring store: " + storeName + " with type: " + pType);
      }
      logger.info("Done restoring all the stores with type: " + pType);
    }
    logger.info("Done restoring all the stores persisted previously");
  }

  // TODO Later change to Guice instead of Java reflections
  // This method can also be called from an admin service to add new store.

  public synchronized AbstractStorageEngine openStoreForNewPartition(VeniceStoreConfig storeConfig, int partitionId) {
    partitionAssignmentRepository.addPartition(storeConfig.getStoreName(), partitionId);

    AbstractStorageEngine engine = openStore(storeConfig);
    if (!engine.containsPartition(partitionId)) {
      engine.addStoragePartition(partitionId);
    }
    return engine;
  }

  /**
   * This method should ideally be Private, but marked as public for validating the result.
   *
   * @param storeConfig StoreConfig of the store.
   * @return Factory corresponding to the store.
   */
  public StorageEngineFactory getInternalStorageEngineFactory(VeniceStoreConfig storeConfig) {
    PersistenceType persistenceType = storeConfig.getStorePersistenceType();
    // Instantiate the factory for this persistence type if not already present
    if (persistenceTypeToStorageEngineFactoryMap.containsKey(persistenceType)) {
      return persistenceTypeToStorageEngineFactoryMap.get(persistenceType);
    }

    throw new VeniceException("Unrecognized persistence type " + persistenceType);
  }

  public Optional<Statistics> getRocksDBAggregatedStatistics() {
    if (persistenceTypeToStorageEngineFactoryMap.containsKey(ROCKS_DB)) {
      return ((RocksDBStorageEngineFactory)persistenceTypeToStorageEngineFactoryMap.get(ROCKS_DB)).getAggStatistics();
    }
    return Optional.empty();
  }


  /**
   * Creates a StorageEngineFactory for the persistence type if not already present.
   * Creates a new storage engine for the given store in the factory and registers the storage engine with the store repository.
   *
   * @param storeConfig   The store specific properties
   * @return StorageEngine that was created for the given store definition.
   */
  private synchronized AbstractStorageEngine openStore(VeniceStoreConfig storeConfig) {
    String storeName = storeConfig.getStoreName();
    AbstractStorageEngine engine = storageEngineRepository.getLocalStorageEngine(storeName);
    if (engine != null) {
      return engine;
    }

    /**
     * For new store, it will use the storage engine configured in host level if it is not known.
     */
    if (!storeConfig.isStorePersistenceTypeKnown()) {
      storeConfig.setStorePersistenceType(storeConfig.getPersistenceType());
    }

    logger.info("Creating/Opening Storage Engine " + storeName + " with type: " + storeConfig.getStorePersistenceType());
    StorageEngineFactory factory = getInternalStorageEngineFactory(storeConfig);
    engine = factory.getStore(storeConfig);
    storageEngineRepository.addLocalStorageEngine(engine);
    // Setup storage engine stats
    aggVersionedStorageEngineStats.setStorageEngine(storeName, engine);

    return engine;
  }

  /**
   * Removes the Store, Partition from the Storage service.
   */
  public synchronized void dropStorePartition(VeniceStoreConfig storeConfig, int partitionId) {
    String storeName = storeConfig.getStoreName(); // This is the Kafka topic name

    partitionAssignmentRepository.dropPartition(storeName , partitionId);
    AbstractStorageEngine storageEngine = storageEngineRepository.getLocalStorageEngine(storeName);
    if (storageEngine == null) {
      logger.info(storeName + " Store could not be located, ignoring the remove partition message.");
      return;
    }
    storageEngine.dropPartition(partitionId);
    Set<Integer> assignedPartitions = storageEngine.getPartitionIds();
    storeConfig.setStorePersistenceType(storageEngine.getType());
    logger.info("Dropped Partition " + partitionId + " Store " + storeName + " Remaining " + Arrays.toString(assignedPartitions.toArray()));

    if (assignedPartitions.isEmpty()) {
      // Drop the directory completely.
      StorageEngineFactory factory = getInternalStorageEngineFactory(storeConfig);
      factory.removeStorageEngine(storageEngine);

      // Clean up the state
      storageEngineRepository.removeLocalStorageEngine(storeName);

      // Clean up the metadata. We dont need to do this here if the config is enabled, as dropPartition does the offset cleanup also
      if (!serverConfig.isRocksDBOffsetMetadataEnabled()) {
        storeVersionStateDeleter.accept(storeName);
      }
    }
  }

  public synchronized void removeStorageEngine(String kafkaTopicName) {
    AbstractStorageEngine<?> storageEngine = getStorageEngineRepository().getLocalStorageEngine(kafkaTopicName);
    VeniceStoreConfig storeConfig = configLoader.getStoreConfig(kafkaTopicName);
    for (Integer partitionId : storageEngine.getPartitionIds()) {
      dropStorePartition(storeConfig, partitionId);
    }
  }

  public void cleanupAllStores(VeniceConfigLoader configLoader) {
    // Load local storage and delete them safely.
    // TODO Just clean the data dir in case loading and deleting is too slow.
    restoreAllStores(configLoader);
    logger.info("Start cleaning up all the stores persisted previously");
    storageEngineRepository.getAllLocalStorageEngines().stream().forEach(storageEngine -> {
      String storeName = storageEngine.getName();
      logger.info("Start deleting store: " + storeName);
      Set<Integer> partitionIds = storageEngine.getPartitionIds();
      for (Integer partitionId : partitionIds) {
        dropStorePartition(configLoader.getStoreConfig(storeName), partitionId);
      }
      logger.info("Deleted store: " + storeName);
    });
    logger.info("Done cleaning up all the stores persisted previously");
  }

  public StorageEngineRepository getStorageEngineRepository() {
    return storageEngineRepository;
  }

  @Override
  public boolean startInner() throws Exception {
    // After Storage Node starts, Helix controller initiates the state transition for the Stores that
    // should be consumed/served by the router.

    // There is no async process in this function, so we are completely finished with the start up process.
    return true;
  }

  @Override
  public void stopInner()
      throws VeniceException {
    VeniceException lastException = null;
    try {
      this.storageEngineRepository.close();
    } catch (VeniceException e) {
      lastException = e;
    }

    /*Close all storage engine factories */
    for (Map.Entry<PersistenceType, StorageEngineFactory> storageEngineFactory : persistenceTypeToStorageEngineFactoryMap.entrySet()) {
      PersistenceType factoryType =  storageEngineFactory.getKey();
      logger.info("Closing " + factoryType + " storage engine factory");
      try {
        storageEngineFactory.getValue().close();
      } catch (VeniceException e) {
        logger.error("Error closing " + factoryType, e);
        lastException = e;
      }
    }

    if (lastException != null) {
      throw lastException;
    }
  }
}
