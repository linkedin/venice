package com.linkedin.venice.server;

import com.linkedin.venice.store.QueryStore;
import com.linkedin.venice.store.StorageEngine;
import com.linkedin.venice.store.Store;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
  private final ConcurrentMap<String, StorageEngine> localStorageEngines;

  public StoreRepository() {
    this.localStores = new ConcurrentHashMap<String, QueryStore>();
    this.localStorageEngines = new ConcurrentHashMap<String, StorageEngine>();
  }

  /*
    Usual CRUD operations on map of Local Stores
   */
  public boolean hasLocalStore(String name) {
    return this.localStores.containsKey(name);
  }

  public QueryStore getLocalStore(String storeName) {
    return this.localStores.get(storeName);
  }

  private QueryStore removeLocalStore(String storeName) {
    return this.localStores.remove(storeName);
  }

  private void addLocalStore(QueryStore store) {
    QueryStore found = this.localStores.putIfAbsent(store.getName(), store);

    if (found != null) {
      logger.error("Store '" + store.getName() + "' has already been initialized.");
      // TODO throw VeniceException
    }
  }

  public List<QueryStore> getAllLocalStores() {
    return new ArrayList<QueryStore>(this.localStores.values());
  }

  /*
  Usual CRUD operations on map of Local Storage Engines
   */
  public boolean hasLocalStorageEngine(String name) {
    return this.localStorageEngines.containsKey(name);
  }

  public StorageEngine getLocalStorageEngine(String storeName) {
    return this.localStorageEngines.get(storeName);
  }

  public StorageEngine removeLocalStorageEngine(String storeName) {
    this.removeLocalStore(storeName);
    return this.localStorageEngines.remove(storeName);
  }

  public void addLocalStorageEngine(StorageEngine engine) {
    StorageEngine found = this.localStorageEngines.putIfAbsent(engine.getName(), engine);
    if (found != null) {
      logger.error("Storage Engine '" + engine.getName() + "' has already been initialized.");
      // TODO throw VeniceException
    }
    this.addLocalStore((QueryStore) engine);
  }

  public List<StorageEngine> getAllLocalStorageEngines() {
    return new ArrayList<StorageEngine>(this.localStorageEngines.values());
  }
}
