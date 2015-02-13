package com.linkedin.venice.storage;

import com.linkedin.venice.config.VeniceServerConfig;
import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.server.PartitionNodeAssignmentRepository;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.server.VeniceConfigService;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.StorageEngineFactory;
import com.linkedin.venice.store.Store;
import com.linkedin.venice.utils.ReflectUtils;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


/**
 * Storage interface to Venice Server. Manages creation and deletion of of Storage engines and maintenance of StoreRepository
 */
public class StorageService extends AbstractVeniceService {

  private static final Logger logger = Logger.getLogger(StorageService.class.getName());

  private final StoreRepository storeRepository;
  private final VeniceConfigService veniceConfigService;
  private final ConcurrentMap<String, StorageEngineFactory> persistenceTypeToStorageEngineFactoryMap;
  private final PartitionNodeAssignmentRepository partitionNodeAssignmentRepository;

  public StorageService(StoreRepository storeRepository, VeniceConfigService veniceConfigService,
      PartitionNodeAssignmentRepository partitionNodeAssignmentRepository) {
    super("storage-service");
    this.storeRepository = storeRepository;
    this.veniceConfigService = veniceConfigService;
    this.persistenceTypeToStorageEngineFactoryMap = new ConcurrentHashMap<String, StorageEngineFactory>();
    this.partitionNodeAssignmentRepository = partitionNodeAssignmentRepository;
  }

  // TODO Later change to Guice instead of Java reflections
  // This method can also be called from an admin service to add new store.

  /**
   * Creates a StorageEngineFactory for the persistence type if not already present.
   * Creates a new storage engine for the given store in the factory and registers the storage engine with the store repository.
   *
   * @param storeDefinition   The store specific properties
   * @return StorageEngine that was created for the given store definition.
   */
  public AbstractStorageEngine openStore(VeniceStoreConfig storeDefinition)
      throws Exception {
    String persistenceType = storeDefinition.getPersistenceType();
    AbstractStorageEngine engine = null;
    StorageEngineFactory factory = null;

    // Instantiate the factory for this persistence type if not already present
    if (!persistenceTypeToStorageEngineFactoryMap.containsKey(persistenceType)) {
      String storageFactoryClassName = storeDefinition.getStorageEngineFactoryClassName();
      try {
        Class<?> factoryClass = ReflectUtils.loadClass(storageFactoryClassName);
        factory = (StorageEngineFactory) ReflectUtils
            .callConstructor(factoryClass, new Class<?>[]{VeniceServerConfig.class, PartitionNodeAssignmentRepository.class},
                new Object[]{storeDefinition, partitionNodeAssignmentRepository});
        persistenceTypeToStorageEngineFactoryMap.putIfAbsent(persistenceType, factory);
      } catch (IllegalStateException e) {
        logger.error("Error loading storage engine factory '" + storageFactoryClassName + "'.", e);
        throw e;
      }
    }

    factory = persistenceTypeToStorageEngineFactoryMap.get(persistenceType);

    if (factory != null) {
      engine = factory.getStore(storeDefinition);
      try {
        registerEngine(engine);
      } catch (VeniceException e) {
        logger.error("Failed to register storage engine for  store: " + engine.getName(), e);
        removeEngine(engine);
      }
    } else {
      logger.error("Failed to get factory from the StorageEngineFactoryMap for perisitence type: " + persistenceType);
    }
    return engine;
  }

  /**
   * Adds the storage engine to the store repository.
   *
   * @param engine  StorageEngine to add
   * @throws VeniceException
   */
  public void registerEngine(AbstractStorageEngine engine)
      throws VeniceException {
    storeRepository.addLocalStorageEngine(engine);
  }

  /**
   * Removes the StorageEngine from the store repository
   *
   * @param engine StorageEngine to remove
   */
  public void removeEngine(AbstractStorageEngine engine) {
    storeRepository.removeLocalStorageEngine(engine.getName());
  }

  @Override
  public void startInner()
      throws Exception {
    logger.info("Initializing stores:");

    /*Loop through the stores. Create the Factory if needed, open the storage engine and
     register it with the Store Repository*/
    for (Map.Entry<String, VeniceStoreConfig> entry : veniceConfigService.getAllStoreConfigs().entrySet()) {
      openStore(entry.getValue());
    }

    logger.info("All stores initialized");
  }

  @Override
  public void stopInner()
      throws VeniceException {
    VeniceException lastException = null;
      /* This will also close the storage engines */
    for (Store store : this.storeRepository.getAllLocalStorageEngines()) {
      logger.info("Closing storage engine for " + store.getName());
      try {
        store.close();
      } catch (VeniceException e) {
        logger.error(e);
        lastException = e;
      }
    }
    logger.info("All stores closed.");

    /*Close all storage engine factories */
    for (Map.Entry<String, StorageEngineFactory> storageEngineFactory : persistenceTypeToStorageEngineFactoryMap.entrySet()) {
      logger.info("Closing " + storageEngineFactory.getKey() + " storage engine factory");
      try {
        storageEngineFactory.getValue().close();
      } catch (VeniceException e) {
        logger.error(e);
        lastException = e;
      }
    }

    if (lastException != null) {
      throw lastException;
    }
  }
}
