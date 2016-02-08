package com.linkedin.venice.server;

import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.storage.StorageService;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.QueryStore;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.log4j.Logger;


/**
 *  A wrapper class that holds all the server's stores, storage engines
 *
 * There are two maps -
 * 1. localStorageEngines - is the lowest level persistence - mainly used by kafka consumer tasks
 * 2. localStores - These are Store abstractions over the local storage engines. They may be used by other services like for example Venice Broker.
 *    They provide access to only QueryStore APIs.
 *
 *  TODO 1. Later need to add stats and monitoring
 *
 */
public class StoreRepository {

  private static final Logger logger = Logger.getLogger(StoreRepository.class.getName());

  /**
   * All Stores owned by this node. Note this only allows for querying the store. No write operations are allowed
   */
  private final ConcurrentMap<String, QueryStore> localStores;

  /**
   *   Local storage engine for this node. This is lowest level persistence abstraction, these StorageEngines provide an iterator over their values.
   */
  private final ConcurrentMap<String, AbstractStorageEngine> localStorageEngines;

  /**
   * The parent storage service that creates this repository should pass in a reference to itself for creation of new stores.
   */
  private StorageService storageService = null;

  public StoreRepository() {
    this.localStores = new ConcurrentHashMap<String, QueryStore>();
    this.localStorageEngines = new ConcurrentHashMap<String, AbstractStorageEngine>();
  }

  public void setStorageService(StorageService storageService){

    this.storageService = storageService;
  }

  /*
    Usual CRUD operations on map of Local Stores
   */
  public boolean hasLocalStore(String name) {
    return localStores.containsKey(name);
  }

  public QueryStore getLocalStore(String storeName) {
    return localStores.get(storeName);
  }

  private QueryStore removeLocalStore(String storeName) {
    return localStores.remove(storeName);
  }

  private void addLocalStore(QueryStore store)
      throws VeniceException {
    QueryStore found = localStores.putIfAbsent(store.getName(), store);

    if (found != null) {
      String errorMessage = "Store '" + store.getName() + "' has already been initialized.";
      logger.error(errorMessage);
      throw new VeniceException(errorMessage);
    }
  }

  public List<QueryStore> getAllLocalStores() {
    return new ArrayList<QueryStore>(localStores.values());
  }

  /*
  Usual CRUD operations on map of Local Storage Engines
   */
  public boolean hasLocalStorageEngine(String name) {
    return this.localStorageEngines.containsKey(name);
  }

  public AbstractStorageEngine getOrCreateLocalStorageEngine(VeniceStoreConfig storeConfig, int partition){
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
    this.removeLocalStore(storeName);
    return localStorageEngines.remove(storeName);
  }

  public void addLocalStorageEngine(AbstractStorageEngine engine)
      throws VeniceException {
    AbstractStorageEngine found = localStorageEngines.putIfAbsent(engine.getName(), engine);
    if (found != null) {
      String errorMessage = "Storage Engine '" + engine.getName() + "' has already been initialized.";
      logger.error(errorMessage);
      throw new VeniceException(errorMessage);
    }
    this.addLocalStore((QueryStore) engine);
  }

  public List<AbstractStorageEngine> getAllLocalStorageEngines() {
    return new ArrayList<AbstractStorageEngine>(localStorageEngines.values());
  }
}
