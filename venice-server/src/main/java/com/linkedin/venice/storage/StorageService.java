package com.linkedin.venice.storage;

import com.linkedin.venice.server.PartitionNodeAssignmentRepository;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.server.VeniceConfig;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.StorageEngineFactory;
import com.linkedin.venice.store.Store;
import com.linkedin.venice.utils.ReflectUtils;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.log4j.Logger;


/**
 * Storage interface to Venice Server. Manages creation and deletion of of Storage engines and maintenance of StoreRepository
 */
public class StorageService extends AbstractVeniceService {

  private static final Logger logger = Logger.getLogger(StorageService.class.getName());

  private final StoreRepository storeRepository;
  private final VeniceConfig veniceConfig;
  private final ConcurrentMap<String, StorageEngineFactory> storeToStorageEngineFactoryMap;
  private final ConcurrentMap<String, Properties> storeNameToStoreConfigsMap;
  private final PartitionNodeAssignmentRepository partitionNodeAssignmentRepository;

  public StorageService(StoreRepository storeRepository, VeniceConfig veniceConfig,
      ConcurrentMap<String, Properties> storeNameToStoreConfigsMap,
      PartitionNodeAssignmentRepository partitionNodeAssignmentRepository) {
    super("storage-service");
    this.storeRepository = storeRepository;
    this.veniceConfig = veniceConfig;
    this.storeToStorageEngineFactoryMap = new ConcurrentHashMap<String, StorageEngineFactory>();
    this.storeNameToStoreConfigsMap = storeNameToStoreConfigsMap;
    this.partitionNodeAssignmentRepository = partitionNodeAssignmentRepository;
  }

  //TODO later change properties into StoreDefinition class
  // TODO Later change to Guice instead of Java reflections
  // This method can also be called from an admin service to add new store.

  /**
   * Creates a StorageEngineFactory for the persistence type if not already present.
   * Creates a new storage engine for the given store in the factory and registers the storage engine with the store repository.
   *
   * @param storeDefinition   The store specific properties
   * @return StorageEngine that was created for the given store definition.
   */
  public AbstractStorageEngine openStore(Properties storeDefinition) {
    String persistenceType = storeDefinition.getProperty("persistence.type");
    AbstractStorageEngine engine = null;
    StorageEngineFactory factory = null;

    // Instantiate the factory for this persistence type if not already present
    if (!storeToStorageEngineFactoryMap.containsKey(persistenceType)) {
      String storageFactoryClassName = veniceConfig.getStorageEngineFactoryClassName(persistenceType);
      if (storageFactoryClassName != null) {
        try {
          Class<?> factoryClass = ReflectUtils.loadClass(storageFactoryClassName);
          factory = (StorageEngineFactory) ReflectUtils
              .callConstructor(factoryClass, new Class<?>[]{VeniceConfig.class},
                  new Object[]{veniceConfig, partitionNodeAssignmentRepository});
          storeToStorageEngineFactoryMap.putIfAbsent(persistenceType, factory);
        } catch (IllegalStateException e) {
          logger.error("Error loading storage engine factory '" + storageFactoryClassName + "'.", e);
        }
      } else {
        // TODO throw / handle exception . This is not a known persistence type.
      }
    }

    factory = storeToStorageEngineFactoryMap.get(persistenceType);

    if (factory != null) {
      engine = factory.getStore(storeDefinition);
      try {
        registerEngine(engine);
      } catch (Exception e) {
        // TODO log error
        removeEngine(engine);
        // throw new exception
      }
    } else {
      // TODO log error
    }
    return engine;
  }

  /**
   * Adds the storage engine to the store repository.
   *
   * @param engine  StorageEngine to add
   * @throws Exception
   */
  public void registerEngine(AbstractStorageEngine engine)
      throws Exception {
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
    for (Map.Entry<String, Properties> entry : storeNameToStoreConfigsMap.entrySet()) {
      openStore(entry.getValue());
    }

    logger.info("All stores initialized");
  }

  @Override
  public void stopInner()
      throws Exception {
    Exception lastException = null;
      /* This will also close the storage engines */
    for (Store store : this.storeRepository.getAllLocalStorageEngines()) {
      logger.info("Closing storage engine for " + store.getName());
      try {
        store.close();
      } catch (Exception e) {
        logger.error(e);
        lastException = e;
      }
    }
    logger.info("All stores closed.");

    /*Close all storage engine factories */
    for (Map.Entry<String, StorageEngineFactory> storageEngineFactory : storeToStorageEngineFactoryMap.entrySet()) {
      logger.info("Closing " + storageEngineFactory.getKey() + " storage engine factory");
      try {
        storageEngineFactory.getValue().close();
      } catch (Exception e) {
        logger.error(e);
        lastException = e;
      }
    }

    if (lastException != null) {
      throw lastException;
    }
  }
}
