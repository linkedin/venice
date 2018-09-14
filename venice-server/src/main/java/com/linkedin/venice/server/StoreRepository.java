package com.linkedin.venice.server;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.stats.AggVersionedBdbStorageEngineStats;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.Store;
import com.linkedin.venice.store.bdb.BdbStorageEngine;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.log4j.Logger;

import static com.linkedin.venice.meta.PersistenceType.BDB;

/**
 *  A wrapper class that holds all the server's storage engines.
 *
 *  TODO 1. Later need to add stats and monitoring
 *  TODO 2. Rename to StorageEngineRepository, as the current name is too confusing.
 */
public class StoreRepository {

  private static final Logger logger = Logger.getLogger(StoreRepository.class);

  /**
   *   Local storage engine for this node. This is lowest level persistence abstraction, these StorageEngines provide an iterator over their values.
   */
  private final ConcurrentMap<String, AbstractStorageEngine> localStorageEngines;

  private AggVersionedBdbStorageEngineStats stats = null;


  public StoreRepository() {
    this.localStorageEngines = new ConcurrentHashMap<String, AbstractStorageEngine>();
  }

  /*
  Usual CRUD operations on map of Local Storage Engines
   */
  public boolean hasLocalStorageEngine(String name) {
    return this.localStorageEngines.containsKey(name);
  }

  public AbstractStorageEngine getLocalStorageEngine(String storeName) {
    return localStorageEngines.get(storeName);
  }

  public AbstractStorageEngine removeLocalStorageEngine(String storeName) {
    AbstractStorageEngine engine = localStorageEngines.remove(storeName);
    //reset stats
    //TODO: make an abstraction atop StorageEngine Stats and get rid of
    if (stats != null && engine != null && engine.getType() == BDB) {
      stats.removeBdbEnvironment(engine.getName());
    }

    return engine;
  }

  public synchronized void addLocalStorageEngine(AbstractStorageEngine engine)
      throws VeniceException {
    AbstractStorageEngine found = localStorageEngines.putIfAbsent(engine.getName(), engine);
    if (found != null) {
      String errorMessage = "Storage Engine '" + engine.getName() + "' has already been initialized.";
      logger.error(errorMessage);
      throw new VeniceException(errorMessage);
    }

    /**
     * It's necessary to get the storage engine instance from the map again,
     * because `putIfAbsent()` method return "the previous value associated with the specified key,
     * or{@code null} if there was no mapping for the key";
     * Without getting storage engine from the map, the condition check below would always fail
     * and the bdb environment in versioned stats would never be set
     */
    found = localStorageEngines.getOrDefault(engine.getName(), null);
    if (stats != null && found != null && found.getType() == BDB) {
      stats.setBdbEnvironment(engine.getName(), ((BdbStorageEngine) engine).getBdbEnvironment());
    }
  }

  public List<AbstractStorageEngine> getAllLocalStorageEngines() {
    return new ArrayList<>(localStorageEngines.values());
  }

  public synchronized void setAggBdbStorageEngineStats(AggVersionedBdbStorageEngineStats stats) {
    this.stats = stats;

    //load all existing bdb engine environments
    for (AbstractStorageEngine engine : getAllLocalStorageEngines()) {
      if (engine.getType() == BDB) {
        this.stats.setBdbEnvironment(engine.getName(), ((BdbStorageEngine) engine).getBdbEnvironment());
      }
    }
  }

  public void close() {
    VeniceException lastException = null;
    for (Store store : localStorageEngines.values()) {
      String storeName = store.getName();
      logger.info("Closing storage engine for " + storeName );
      try {
        store.close();
      } catch (VeniceException e) {
        logger.error("Error closing storage engine for store" + storeName , e);
        lastException = e;
      }
    }

    if(lastException != null) {
      throw lastException;
    }
    logger.info("All stores closed.");
  }
}
