package com.linkedin.venice.server;

import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.storage.StorageService;
import com.linkedin.venice.store.AbstractStorageEngine;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.log4j.Logger;


/**
 *  A wrapper class that holds all the server's storage engines
 *
 *  TODO 1. Later need to add stats and monitoring
 */
public class StoreRepository {

  private static final Logger logger = Logger.getLogger(StoreRepository.class.getName());

  /**
   *   Local storage engine for this node. This is lowest level persistence abstraction, these StorageEngines provide an iterator over their values.
   */
  private final ConcurrentMap<String, AbstractStorageEngine> localStorageEngines;


  //The parent storage service that creates this repository should pass in a reference to itself for creation of new stores.
  private StorageService storageService = null;

  public StoreRepository() {
    this.localStorageEngines = new ConcurrentHashMap<String, AbstractStorageEngine>();
  }

  public void setStorageService(StorageService storageService){
    this.storageService = storageService;
  }

  /*
  Usual CRUD operations on map of Local Storage Engines
   */
  public boolean hasLocalStorageEngine(String name) {
    return this.localStorageEngines.containsKey(name);
  }

  public synchronized AbstractStorageEngine getOrCreateLocalStorageEngine(VeniceStoreConfig storeConfig, int partition){
    if (!localStorageEngines.containsKey(storeConfig.getStoreName())){
      try {
        AbstractStorageEngine newEngine = storageService.openStoreForNewPartition(storeConfig, partition);
        localStorageEngines.put(storeConfig.getStoreName(), newEngine);
      } catch (Exception e) {
        logger.error("Failed to create a new store: " + storeConfig.getStoreName());
        throw new RuntimeException(e);
      }
    }
    return localStorageEngines.get(storeConfig.getStoreName());
  }

  public AbstractStorageEngine getLocalStorageEngine(String storeName) {
    return localStorageEngines.get(storeName);
  }

  public AbstractStorageEngine removeLocalStorageEngine(String storeName) {
    return localStorageEngines.remove(storeName);
  }

  public synchronized void addLocalStorageEngine(AbstractStorageEngine engine)
      throws VeniceException {
    AbstractStorageEngine found = localStorageEngines.putIfAbsent(engine.getName(), engine);
    if (found != null) {
      String errorMessage = "Storage Engine '" + engine.getName() + "' has already been initialized.";
      logger.error(errorMessage);
      throw new VeniceException(errorMessage);
    }
  }

  public List<AbstractStorageEngine> getAllLocalStorageEngines() {
    return new ArrayList<AbstractStorageEngine>(localStorageEngines.values());
  }
}
