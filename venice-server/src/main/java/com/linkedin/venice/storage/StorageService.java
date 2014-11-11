package com.linkedin.venice.storage;

import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.server.VeniceConfig;
import com.linkedin.venice.service.AbstractService;
import com.linkedin.venice.store.StorageEngine;
import com.linkedin.venice.store.StorageEngineFactory;
import com.linkedin.venice.store.Store;
import com.linkedin.venice.utils.ReflectUtils;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.io.FilenameFilter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.log4j.Logger;


public class StorageService extends AbstractService {

  private static final Logger logger = Logger.getLogger(StorageService.class.getName());

  private final StoreRepository storeRepository;
  private final VeniceConfig veniceConfig;
  private final ConcurrentMap<String, StorageEngineFactory> storageEngineFactoryMap;
  private final ConcurrentMap<String, Properties> storeDefinitionsMap;

  public StorageService(StoreRepository storeRepository, VeniceConfig veniceConfig) {
    super("storage-service");
    this.storeRepository = storeRepository;
    this.veniceConfig = veniceConfig;
    this.storageEngineFactoryMap = new ConcurrentHashMap<String, StorageEngineFactory>();
    this.storeDefinitionsMap = new ConcurrentHashMap<String, Properties>();
  }

  /**
   * Go over the list of configured stores and initialize
   * 1. store definitions map
   */
  private void initStoreDefinitions() {
    File storeDefinitionsDir =
        new File(veniceConfig.getConfigDirAbsolutePath() + File.separator + veniceConfig.STORE_DEFINITIONS_DIR_NAME);
    if (!Utils.isReadableDir(storeDefinitionsDir)) {
      logger.error(
          "Either the " + VeniceConfig.STORE_DEFINITIONS_DIR_NAME + " directory does not exist or is not readable.");
      // TODO throw exception and stop
    }

    // Get all .properties file in STORES directory
    List<File> storeConfigurationFiles = Arrays.asList(storeDefinitionsDir.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith(".properties");
      }
    }));

    //parse the properties for each store
    for (File storeDef : storeConfigurationFiles) {
      try {
        Properties prop = Utils.parseProperties(storeDef);
        storeDefinitionsMap.putIfAbsent(prop.getProperty("name"), prop);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  //TODO later change properties into StoreDefinition class
  // TODO Later change to Guice instead of Java reflections
  // This method can also be called from an admin service to add new store.
  public StorageEngine openStore(Properties storeDefinition) {
    String persistenceType = storeDefinition.getProperty("persistence.type");
    StorageEngine engine = null;
    StorageEngineFactory factory = null;

    // Instantiate the factory for this persistence type if not already present
    if (!storageEngineFactoryMap.containsKey(persistenceType)) {
      String storageFactoryClassName = veniceConfig.getStorageEngineFactoryClassName(persistenceType);
      if (storageFactoryClassName != null) {
        try {
          Class<?> factoryClass = ReflectUtils.loadClass(storageFactoryClassName);
          factory = (StorageEngineFactory) ReflectUtils
              .callConstructor(factoryClass, new Class<?>[]{VeniceConfig.class}, new Object[]{veniceConfig});
          storageEngineFactoryMap.putIfAbsent(persistenceType, factory);
        } catch (IllegalStateException e) {
          logger.error("Error loading storage engine factory '" + storageFactoryClassName + "'.", e);
        }
      } else {
        // TODO throw / handle exception . This is not a known persistence type.
      }
    }

    factory = storageEngineFactoryMap.get(persistenceType);

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

  public void registerEngine(StorageEngine engine)
      throws Exception {
    storeRepository.addLocalStorageEngine(engine);
    storeRepository.addLocalStore(engine);
  }

  public void removeEngine(StorageEngine engine) {
    storeRepository.removeLocalStore(engine.getName());
    storeRepository.removeLocalStorageEngine(engine.getName());
  }

  @Override
  public void startInner()
      throws Exception {

    //initialize all store definitions
    initStoreDefinitions();

    logger.info("Initializing stores:");

    /*Loop through the stores. Create the Factory if needed, open the storage engine and
     register it with the Store Repository*/
    for (Map.Entry<String, Properties> entry : storeDefinitionsMap.entrySet()) {
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
    for (Map.Entry<String, StorageEngineFactory> storageEngineFactory : storageEngineFactoryMap.entrySet()) {
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
