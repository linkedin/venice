package com.linkedin.davinci.storage;

import static com.linkedin.venice.meta.PersistenceType.BLACK_HOLE;
import static com.linkedin.venice.meta.PersistenceType.IN_MEMORY;
import static com.linkedin.venice.meta.PersistenceType.ROCKS_DB;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.stats.AggVersionedStorageEngineStats;
import com.linkedin.davinci.stats.RocksDBMemoryStats;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.DelegatingStorageEngine;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.davinci.store.StorageEngineFactory;
import com.linkedin.davinci.store.blackhole.BlackHoleStorageEngineFactory;
import com.linkedin.davinci.store.memory.InMemoryStorageEngineFactory;
import com.linkedin.davinci.store.rocksdb.RocksDBStorageEngineFactory;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.helix.SafeHelixDataAccessor;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.ExceptionUtils;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.ReferenceCounted;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.IdealState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.RocksDBException;
import org.rocksdb.Statistics;


/**
 * Storage interface to Venice Server, Da Vinci and Isolated Ingestion Service. Manages creation and deletion of storage
 * engines and partitions.
 * Use StorageEngineRepository, if read only access is desired for the Storage Engines.
 */
public class StorageService extends AbstractVeniceService {
  private static final Logger LOGGER = LogManager.getLogger(StorageService.class);

  private final StorageEngineRepository storageEngineRepository;
  private final VeniceConfigLoader configLoader;
  private final VeniceServerConfig serverConfig;
  private final Map<PersistenceType, StorageEngineFactory> persistenceTypeToStorageEngineFactoryMap;
  private final AggVersionedStorageEngineStats aggVersionedStorageEngineStats;
  private final RocksDBMemoryStats rocksDBMemoryStats;
  private final InternalAvroSpecificSerializer<StoreVersionState> storeVersionStateSerializer;
  private final InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer;
  private final ReadOnlyStoreRepository storeRepository;
  private final Map<String, ReferenceCounted<DelegatingStorageEngine>> storageEngines = new VeniceConcurrentHashMap<>();

  /**
   * Allocates a new {@code StorageService} object.
   * @param configLoader a config loader to load configs related to cluster and server.
   * @param storageEngineStats storage engine related stats.
   * @param rocksDBMemoryStats RocksDB memory consumption stats.
   * @param storeVersionStateSerializer serializer for translating a store-version level state into avro-format.
   * @param partitionStateSerializer serializer for translating a partition state into avro-format.
   * @param storeRepository supports readonly operations to access stores
   * @param restoreDataPartitions indicates if store data needs to be restored.
   * @param restoreMetadataPartitions indicates if meta data needs to be restored.
   * @param checkWhetherStorageEngineShouldBeKeptOrNot check whether the local storage engine should be kept or not.
   */
  StorageService(
      VeniceConfigLoader configLoader,
      AggVersionedStorageEngineStats storageEngineStats,
      RocksDBMemoryStats rocksDBMemoryStats,
      InternalAvroSpecificSerializer<StoreVersionState> storeVersionStateSerializer,
      InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer,
      ReadOnlyStoreRepository storeRepository,
      boolean restoreDataPartitions,
      boolean restoreMetadataPartitions,
      Function<String, Boolean> checkWhetherStorageEngineShouldBeKeptOrNot,
      Optional<Map<PersistenceType, StorageEngineFactory>> persistenceTypeToStorageEngineFactoryMapOptional) {
    String dataPath = configLoader.getVeniceServerConfig().getDataBasePath();
    if (!Utils.directoryExists(dataPath)) {
      if (!configLoader.getVeniceServerConfig().isAutoCreateDataPath()) {
        throw new VeniceException(
            "Data directory '" + dataPath + "' does not exist and " + ConfigKeys.AUTOCREATE_DATA_PATH
                + " is disabled.");
      }

      File dataDir = new File(dataPath);
      LOGGER.info("Creating data directory {}", dataDir.getAbsolutePath());
      dataDir.mkdirs();
    }

    this.configLoader = configLoader;
    this.serverConfig = configLoader.getVeniceServerConfig();
    this.storageEngineRepository = new StorageEngineRepository();

    this.aggVersionedStorageEngineStats = storageEngineStats;
    this.rocksDBMemoryStats = rocksDBMemoryStats;
    this.storeVersionStateSerializer = storeVersionStateSerializer;
    this.partitionStateSerializer = partitionStateSerializer;
    this.storeRepository = storeRepository;
    if (persistenceTypeToStorageEngineFactoryMapOptional.isPresent()) {
      this.persistenceTypeToStorageEngineFactoryMap = persistenceTypeToStorageEngineFactoryMapOptional.get();
    } else {
      this.persistenceTypeToStorageEngineFactoryMap = new HashMap<>();
      initInternalStorageEngineFactories();
    }
    if (restoreDataPartitions || restoreMetadataPartitions) {
      restoreAllStores(
          configLoader,
          restoreDataPartitions,
          restoreMetadataPartitions,
          checkWhetherStorageEngineShouldBeKeptOrNot);
    }
  }

  public StorageService(
      VeniceConfigLoader configLoader,
      AggVersionedStorageEngineStats storageEngineStats,
      RocksDBMemoryStats rocksDBMemoryStats,
      InternalAvroSpecificSerializer<StoreVersionState> storeVersionStateSerializer,
      InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer,
      ReadOnlyStoreRepository storeRepository,
      boolean restoreDataPartitions,
      boolean restoreMetadataPartitions,
      Function<String, Boolean> checkWhetherStorageEngineShouldBeKeptOrNot) {
    this(
        configLoader,
        storageEngineStats,
        rocksDBMemoryStats,
        storeVersionStateSerializer,
        partitionStateSerializer,
        storeRepository,
        restoreDataPartitions,
        restoreMetadataPartitions,
        checkWhetherStorageEngineShouldBeKeptOrNot,
        Optional.empty());
  }

  public StorageService(
      VeniceConfigLoader configLoader,
      AggVersionedStorageEngineStats storageEngineStats,
      RocksDBMemoryStats rocksDBMemoryStats,
      InternalAvroSpecificSerializer<StoreVersionState> storeVersionStateSerializer,
      InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer,
      ReadOnlyStoreRepository storeRepository,
      boolean restoreDataPartitions,
      boolean restoreMetadataPartitions) {
    this(
        configLoader,
        storageEngineStats,
        rocksDBMemoryStats,
        storeVersionStateSerializer,
        partitionStateSerializer,
        storeRepository,
        restoreDataPartitions,
        restoreMetadataPartitions,
        s -> true);
  }

  /**
   * @see #StorageService(VeniceConfigLoader, AggVersionedStorageEngineStats, RocksDBMemoryStats, InternalAvroSpecificSerializer, InternalAvroSpecificSerializer, ReadOnlyStoreRepository)
   */
  public StorageService(
      VeniceConfigLoader configLoader,
      AggVersionedStorageEngineStats storageEngineStats,
      RocksDBMemoryStats rocksDBMemoryStats,
      InternalAvroSpecificSerializer<StoreVersionState> storeVersionStateSerializer,
      InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer,
      ReadOnlyStoreRepository storeRepository) {
    this(
        configLoader,
        storageEngineStats,
        rocksDBMemoryStats,
        storeVersionStateSerializer,
        partitionStateSerializer,
        storeRepository,
        true,
        true);
  }

  public static boolean isMetadataPartition(int partitionId) {
    return partitionId == AbstractStorageEngine.METADATA_PARTITION_ID;
  }

  /**
   * Initialize all the internal storage engine factories.
   * Please add it here if you want to add more.
   */
  private void initInternalStorageEngineFactories() {
    persistenceTypeToStorageEngineFactoryMap.put(IN_MEMORY, new InMemoryStorageEngineFactory(serverConfig));
    persistenceTypeToStorageEngineFactoryMap.put(
        ROCKS_DB,
        new RocksDBStorageEngineFactory(
            serverConfig,
            rocksDBMemoryStats,
            storeVersionStateSerializer,
            partitionStateSerializer));
    persistenceTypeToStorageEngineFactoryMap.put(BLACK_HOLE, new BlackHoleStorageEngineFactory());
  }

  static void deleteStorageEngineOnRocksDBError(
      String storageEngineName,
      ReadOnlyStoreRepository storeRepository,
      StorageEngineFactory factory) {
    String storeName = Version.parseStoreFromKafkaTopicName(storageEngineName);
    Store store;
    boolean doDeleteStorageEngine;
    try {
      int versionNumber = Version.parseVersionFromKafkaTopicName(storageEngineName);
      store = storeRepository.getStoreOrThrow(storeName);
      doDeleteStorageEngine = store.getVersion(versionNumber) == null || versionNumber < store.getCurrentVersion();
    } catch (VeniceNoStoreException e) {
      // The store does not exist in Venice anymore, so it will be deleted.
      doDeleteStorageEngine = true;
    }

    if (doDeleteStorageEngine) {
      LOGGER.info("Storage for {} does not exist, will delete it.", storageEngineName);
      factory.removeStorageEngine(storageEngineName);
    }
  }

  private void restoreAllStores(
      VeniceConfigLoader configLoader,
      boolean restoreDataPartitions,
      boolean restoreMetadataPartitions,
      Function<String, Boolean> checkWhetherStorageEngineShouldBeKeptOrNot) {
    LOGGER.info("Start restoring all the stores persisted previously");
    for (Map.Entry<PersistenceType, StorageEngineFactory> entry: persistenceTypeToStorageEngineFactoryMap.entrySet()) {
      PersistenceType pType = entry.getKey();
      StorageEngineFactory factory = entry.getValue();
      LOGGER.info("Start restoring all the stores with type: {}", pType);
      Set<String> storeNames = factory.getPersistedStoreNames();
      for (String storeName: storeNames) {
        LOGGER.info("Start restoring store: {} with type: {}", storeName, pType);
        /**
         * Setup store-level persistence type based on current database setup.
         */
        VeniceStoreVersionConfig storeConfig = configLoader.getStoreConfig(storeName, pType);
        // Load the metadata & data restore settings from config loader.
        storeConfig.setRestoreDataPartitions(restoreDataPartitions);
        storeConfig.setRestoreMetadataPartition(restoreMetadataPartitions);
        StorageEngine storageEngine;

        if (checkWhetherStorageEngineShouldBeKeptOrNot.apply(storeName)) {
          try {
            storageEngine = openStore(storeConfig, () -> null);
          } catch (Exception e) {
            if (ExceptionUtils.recursiveClassEquals(e, RocksDBException.class)) {
              LOGGER.warn("Encountered RocksDB error while opening store: {}", storeName, e);
              // if store version does not exist, clean up the resources.
              deleteStorageEngineOnRocksDBError(storeName, storeRepository, factory);
              continue;
            }
            LOGGER.error("Could not load the following store : " + storeName, e);
            aggVersionedStorageEngineStats.recordRocksDBOpenFailure(storeName);
            throw new VeniceException("Error caught during opening store " + storeName, e);
          }

          Set<Integer> partitionIds = storageEngine.getPartitionIds();
          LOGGER.info(
              "Loaded the following partitions: {}, for store: {}",
              Arrays.toString(partitionIds.toArray()),
              storeName);
          LOGGER.info("Done restoring store: {} with type: {}", storeName, pType);
        } else {
          LOGGER.info("Starting deleting local storage engine: {} with type: {}", storeName, pType);
          factory.removeStorageEngine(storeName);
          LOGGER.info("Done deleting local storage engine: {} with type: {}", storeName, pType);
        }
      }
      LOGGER.info("Done restoring all the stores with type: {}", pType);
    }
    LOGGER.info("Done restoring all the stores persisted previously");
  }

  public synchronized StorageEngine openStoreForNewPartition(
      VeniceStoreVersionConfig storeConfig,
      int partitionId,
      Supplier<StoreVersionState> initialStoreVersionStateSupplier) {
    LOGGER.info("Opening store for {} partition {}", storeConfig.getStoreVersionName(), partitionId);
    StorageEngine engine = openStore(storeConfig, initialStoreVersionStateSupplier);
    engine.addStoragePartitionIfAbsent(partitionId);
    LOGGER.info("Opened store for {} partition {}", storeConfig.getStoreVersionName(), partitionId);
    return engine;
  }

  public BiConsumer<String, StoreVersionState> getStoreVersionStateSyncer() {
    return (storeVersionName, storeVersionState) -> {
      StorageEngine storageEngine = storageEngineRepository.getLocalStorageEngine(storeVersionName);
      if (storageEngine != null) {
        storageEngine.updateStoreVersionStateCache(storeVersionState);
      }
    };
  }

  /**
   * This method should ideally be Private, but marked as public for validating the result.
   *
   * @param storeConfig StoreConfig of the store.
   * @return Factory corresponding to the store.
   */
  public StorageEngineFactory getInternalStorageEngineFactory(VeniceStoreVersionConfig storeConfig) {
    PersistenceType persistenceType = storeConfig.getStorePersistenceType();
    // Instantiate the factory for this persistence type if not already present
    if (persistenceTypeToStorageEngineFactoryMap.containsKey(persistenceType)) {
      return persistenceTypeToStorageEngineFactoryMap.get(persistenceType);
    }

    throw new VeniceException("Unrecognized persistence type " + persistenceType);
  }

  public Optional<Statistics> getRocksDBAggregatedStatistics() {
    if (persistenceTypeToStorageEngineFactoryMap.containsKey(ROCKS_DB)) {
      return ((RocksDBStorageEngineFactory) persistenceTypeToStorageEngineFactoryMap.get(ROCKS_DB)).getAggStatistics();
    }
    return Optional.empty();
  }

  /**
   * Creates a StorageEngineFactory for the persistence type if not already present.
   * Creates a new storage engine for the given store in the factory and registers the storage engine with the store repository.
   *
   * @param storeConfig   The store specific properties
   * @param initialStoreVersionStateSupplier invoked to initialize the SVS when a brand-new storage engine is created
   * @return StorageEngine that was created for the given store definition.
   */
  public synchronized StorageEngine openStore(
      VeniceStoreVersionConfig storeConfig,
      Supplier<StoreVersionState> initialStoreVersionStateSupplier) {
    String topicName = storeConfig.getStoreVersionName();
    StorageEngine engine = storageEngineRepository.getLocalStorageEngine(topicName);
    if (engine != null) {
      return engine;
    }

    long startTimeInBuildingNewEngine = System.nanoTime();
    /**
     * For new store, it will use the storage engine configured in host level if it is not known.
     */
    if (!storeConfig.isStorePersistenceTypeKnown()) {
      storeConfig.setStorePersistenceType(storeConfig.getPersistenceType());
    }

    LOGGER.info("Creating/Opening Storage Engine {} with type: {}", topicName, storeConfig.getStorePersistenceType());
    StorageEngineFactory factory = getInternalStorageEngineFactory(storeConfig);
    StorageEngine newEngine =
        factory.getStorageEngine(storeConfig, isReplicationMetadataEnabled(topicName, factory.getPersistenceType()));
    newEngine.updateStoreVersionStateCache(initialStoreVersionStateSupplier.get());

    // Let's check if a previous incarnation of the storage engine existed earlier
    ReferenceCounted<DelegatingStorageEngine> refCountedStorageEngine =
        this.storageEngines.compute(topicName, (k, v) -> {
          if (v != null) {
            v.get().setDelegate(newEngine);
            LOGGER.info(
                "Injected newly created storage engine into existing ref-counted delegating storage engine for {}.",
                k);
          }
          return v;
        });
    DelegatingStorageEngine delegatingStorageEngine =
        refCountedStorageEngine == null ? new DelegatingStorageEngine(newEngine) : refCountedStorageEngine.get();

    storageEngineRepository.addLocalStorageEngine(delegatingStorageEngine);
    // Setup storage engine stats
    aggVersionedStorageEngineStats.setStorageEngine(topicName, delegatingStorageEngine);

    LOGGER.info(
        "time spent on creating new storage Engine for store {}: {} ms",
        topicName,
        LatencyUtils.getElapsedTimeFromNSToMS(startTimeInBuildingNewEngine));
    return delegatingStorageEngine;
  }

  public synchronized void checkWhetherStoragePartitionsShouldBeKeptOrNot(SafeHelixManager manager) {
    if (!serverConfig.isDeleteUnassignedPartitionsOnStartupEnabled()) {
      return;
    }

    if (manager == null) {
      return;
    }
    for (StorageEngine storageEngine: getStorageEngineRepository().getAllLocalStorageEngines()) {
      String storeName = storageEngine.getStoreVersionName();
      Set<Integer> storageEnginePartitionIds = new HashSet<>(storageEngine.getPartitionIds());
      String instanceHostName = manager.getInstanceName();
      PropertyKey.Builder propertyKeyBuilder =
          new PropertyKey.Builder(configLoader.getVeniceClusterConfig().getClusterName());
      SafeHelixDataAccessor helixDataAccessor = manager.getHelixDataAccessor();
      IdealState idealState = helixDataAccessor.getProperty(propertyKeyBuilder.idealStates(storeName));
      /**
       * In a helix ideal state, helix maintains information in listfields and mapfields.  The mapfields inform which
       * messages needs to be sent based on the current LIVEINSTANCES, and will therefore be reflective of what
       * nodes are available to receive messages.  The listfields however will contain the assignment, and will only
       * change if a rebalance is calculated.  For this clean up code, we rely on entries in the listfield based
       * on which state transitions we're anticipating to receive.
       */
      if (idealState != null) {
        Map<String, List<String>> listFields = idealState.getRecord().getListFields();
        for (Integer partitionId: storageEnginePartitionIds) {
          if (isMetadataPartition(partitionId)) {
            continue;
          }
          String partitionDbName = storeName + "_" + partitionId;
          List<String> hostNames = listFields.get(partitionDbName);
          if (hostNames == null || !hostNames.contains(instanceHostName)) {
            LOGGER.info(
                "the following partition is not assigned to the current host {} and is being dropped from storage engine {}: {}",
                instanceHostName,
                storeName,
                String.valueOf(partitionId));
            storageEngine.dropPartition(partitionId);
          }
        }
        if (storageEngine.getPartitionIds().isEmpty()) {
          LOGGER.info("removing the storage engine {}, which has no partitions", storeName);
          removeStorageEngine(storeName);
        }
      } else {
        LOGGER.info("removing the storage engine {} as the ideal state is null", storeName);
        removeStorageEngine(storeName);
      }
    }
  }

  /**
   * Drops the partition of the specified store version in the storage service. When all data partitions are dropped,
   * it will also drop the storage engine of the specific store version.
   * @param storeConfig config of the store version.
   * @param partition partition ID to be dropped.
   */
  public synchronized void dropStorePartition(VeniceStoreVersionConfig storeConfig, int partition) {
    dropStorePartition(storeConfig, partition, true);
  }

  /**
   * Drops the partition of the specified store version in the storage service.
   *
   * @param storeConfig              config of the store version.
   * @param partition                partition ID to be dropped.
   * @param removeEmptyStorageEngine Whether to delete the storage engine when there is no remaining data partition.
   */
  public synchronized void dropStorePartition(
      VeniceStoreVersionConfig storeConfig,
      int partition,
      boolean removeEmptyStorageEngine) {
    String kafkaTopic = storeConfig.getStoreVersionName();
    StorageEngine storageEngine = storageEngineRepository.getLocalStorageEngine(kafkaTopic);
    if (storageEngine == null) {
      LOGGER.warn("Storage engine {} does not exist, directly deleting DB files.", kafkaTopic);
      removeStoragePartition(kafkaTopic, partition);
      return;
    }
    storageEngine.dropPartition(partition);
    Set<Integer> remainingPartitions = storageEngine.getPartitionIds();
    LOGGER.info("Dropped partition {} of {}, remaining partitions={}", partition, kafkaTopic, remainingPartitions);

    if (remainingPartitions.isEmpty() && removeEmptyStorageEngine) {
      removeStorageEngine(kafkaTopic);
    }
  }

  void removeStoragePartition(String kafkaTopic, int partition) {
    for (StorageEngineFactory factory: persistenceTypeToStorageEngineFactoryMap.values()) {
      factory.removeStorageEnginePartition(kafkaTopic, partition);
    }
  }

  public synchronized void closeStorePartition(VeniceStoreVersionConfig storeConfig, int partition) {
    String kafkaTopic = storeConfig.getStoreVersionName();
    StorageEngine storageEngine = storageEngineRepository.getLocalStorageEngine(kafkaTopic);
    if (storageEngine == null) {
      LOGGER.warn("Storage engine {} does not exist, ignoring close partition request.", kafkaTopic);
      return;
    }
    storageEngine.closePartition(partition);
  }

  public synchronized void removeStorageEngine(String kafkaTopic) {
    StorageEngine<? extends com.linkedin.davinci.store.AbstractStoragePartition> storageEngine =
        getStorageEngineRepository().removeLocalStorageEngine(kafkaTopic);
    if (storageEngine == null) {
      LOGGER.warn("Storage engine {} does not exist, ignoring remove request.", kafkaTopic);
      return;
    }
    storageEngine.drop();

    VeniceStoreVersionConfig storeConfig = configLoader.getStoreConfig(kafkaTopic);
    storeConfig.setStorePersistenceType(storageEngine.getType());

    StorageEngineFactory factory = getInternalStorageEngineFactory(storeConfig);
    factory.removeStorageEngine(storageEngine);
  }

  /**
   * This function is used to forcely clean up all the databases belonging to {@param kafkaTopic}.
   * This function will only be used when the {@link #removeStorageEngine(String)} function can't
   * handle some edge case, such as some partitions are lingering, which are not visible to the corresponding
   * {@link StorageEngine}
   */
  public synchronized void forceStorageEngineCleanup(String kafkaTopic) {
    persistenceTypeToStorageEngineFactoryMap.values().forEach(factory -> factory.removeStorageEngine(kafkaTopic));
  }

  public synchronized void closeStorageEngine(String kafkaTopic) {
    StorageEngine<? extends com.linkedin.davinci.store.AbstractStoragePartition> storageEngine =
        getStorageEngineRepository().removeLocalStorageEngine(kafkaTopic);
    if (storageEngine == null) {
      LOGGER.warn("Storage engine {} does not exist, ignoring close request.", kafkaTopic);
      return;
    }
    storageEngine.close();

    VeniceStoreVersionConfig storeConfig = configLoader.getStoreConfig(kafkaTopic);
    storeConfig.setStorePersistenceType(storageEngine.getType());

    StorageEngineFactory factory = getInternalStorageEngineFactory(storeConfig);
    factory.closeStorageEngine(storageEngine);
  }

  public void cleanupAllStores(VeniceConfigLoader configLoader) {
    // Load local storage and delete them safely.
    // TODO Just clean the data dir in case loading and deleting is too slow.
    restoreAllStores(configLoader, true, true, s -> true);
    LOGGER.info("Start cleaning up all the stores persisted previously");
    storageEngineRepository.getAllLocalStorageEngines().stream().forEach(storageEngine -> {
      String storeName = storageEngine.getStoreVersionName();
      LOGGER.info("Start deleting store: {}", storeName);
      Set<Integer> partitionIds = storageEngine.getPartitionIds();
      for (Integer partitionId: partitionIds) {
        dropStorePartition(configLoader.getStoreConfig(storeName), partitionId);
      }
      LOGGER.info("Deleted store: {}", storeName);
    });
    LOGGER.info("Done cleaning up all the stores persisted previously");
  }

  public List<Integer> getUserPartitions(String kafkaTopicName) {
    StorageEngine storageEngine = storageEngineRepository.getLocalStorageEngine(kafkaTopicName);
    if (storageEngine == null) {
      LOGGER.warn("Local storage engine does not exist for topic: {}", kafkaTopicName);
      return Collections.emptyList();
    }
    return new ArrayList<Integer>(storageEngine.getPartitionIds());
  }

  public void closeAllStorageEngines() {
    LOGGER.info(
        "Storage service has {} storage engines before cleanup.",
        storageEngineRepository.getAllLocalStorageEngines().size());
    for (StorageEngine storageEngine: storageEngineRepository.getAllLocalStorageEngines()) {
      closeStorageEngine(storageEngine.getStoreVersionName());
    }
    LOGGER.info(
        "Storage service has {} storage engines after cleanup.",
        storageEngineRepository.getAllLocalStorageEngines().size());
  }

  public StorageEngineRepository getStorageEngineRepository() {
    return storageEngineRepository;
  }

  public StorageEngine getStorageEngine(String kafkaTopic) {
    return getStorageEngineRepository().getLocalStorageEngine(kafkaTopic);
  }

  /**
   * This function is for code paths which need to tie themselves to the lifecycle of the storage engine. When the
   * caller's own lifecycle ends, it should call {@link ReferenceCounted#release()} on the instance returned by this
   * function, to indicate that it no longer depends on it.
   */
  public ReferenceCounted<? extends StorageEngine> getRefCountedStorageEngine(String storeVersionName) {
    return this.storageEngines.compute(storeVersionName, (k, v) -> {
      if (v != null) {
        v.retain();
        return v;
      }
      DelegatingStorageEngine storageEngine = getStorageEngineRepository().getDelegatingStorageEngine(k);
      if (storageEngine == null) {
        throw new IllegalStateException("Did not find a storage engine for: " + k);
      }
      return new ReferenceCounted<>(storageEngine, se -> this.storageEngines.remove(k));
    });
  }

  public Map<String, Set<Integer>> getStoreAndUserPartitionsMapping() {
    Map<String, Set<Integer>> storePartitionMapping = new HashMap<>();
    for (StorageEngine engine: storageEngineRepository.getAllLocalStorageEngines()) {
      String storeName = engine.getStoreVersionName();
      Set<Integer> partitionIdSet = new HashSet<>();
      /**
       * The reason to use {@link StorageEngine#getPersistedPartitionIds()} here is that
       * Isolated process doesn't preload all the on-disk databases at startup time.
       */
      ((Set<Integer>) engine.getPersistedPartitionIds()).stream().forEach(partitionId -> {
        if (!isMetadataPartition(partitionId)) {
          partitionIdSet.add(partitionId);
        }
        storePartitionMapping.put(storeName, partitionIdSet);
      });
    }
    return storePartitionMapping;
  }

  @Override
  public boolean startInner() throws Exception {
    // After Storage Node starts, Helix controller initiates the state transition for the Stores that
    // should be consumed/served by the router.

    // There is no async process in this function, so we are completely finished with the start up process.
    return true;
  }

  @Override
  public void stopInner() throws VeniceException {
    VeniceException lastException = null;
    try {
      this.storageEngineRepository.close();
    } catch (VeniceException e) {
      lastException = e;
    }

    /*Close all storage engine factories */
    for (Map.Entry<PersistenceType, StorageEngineFactory> storageEngineFactory: persistenceTypeToStorageEngineFactoryMap
        .entrySet()) {
      PersistenceType factoryType = storageEngineFactory.getKey();
      LOGGER.info("Closing {} storage engine factory", factoryType);
      try {
        storageEngineFactory.getValue().close();
      } catch (VeniceException e) {
        LOGGER.error("Error closing " + factoryType, e);
        lastException = e;
      }
    }

    if (lastException != null) {
      throw lastException;
    }
  }

  private boolean isReplicationMetadataEnabled(String topicName, PersistenceType persistenceType) {
    // Replication metadata will only be used in Server as Da Vinci will never become LEADER.
    if (serverConfig.isDaVinciClient() || !Objects.equals(persistenceType, ROCKS_DB)) {
      return false;
    }
    String storeName;
    int versionNum;
    try {
      storeName = Version.parseStoreFromVersionTopic(topicName);
      versionNum = Version.parseVersionFromKafkaTopicName(topicName);
    } catch (IllegalArgumentException e) {
      /**
       * Adding this try-catch block to return false if passed in storeName does not contain a version number.
       * Our storage engine constructor does not check whether the passed in storeName contains a valid version number.
       * In our test suite, we wrote some tests that only specify store name but not version number. For these tests,
       * we should return false as they are aiming at other features and not for this version-level config testing.
       */
      return false;
    }
    try {
      Version version = storeRepository.getStoreOrThrow(storeName).getVersion(versionNum);
      if (version != null) {
        return version.isActiveActiveReplicationEnabled();
      } else {
        LOGGER.warn("Version {} of store {} does not exist in storeRepository.", versionNum, storeName);
        return false;
      }
    } catch (VeniceNoStoreException e) {
      LOGGER.warn("Store {} does not exist in storeRepository.", storeName);
      return false;
    }
  }
}
