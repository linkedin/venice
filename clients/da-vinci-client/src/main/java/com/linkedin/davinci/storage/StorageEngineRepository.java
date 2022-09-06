package com.linkedin.davinci.storage;

import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.venice.exceptions.VeniceException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 *  A wrapper class that holds all the server's storage engines.
 *
 *  TODO 1. Later need to add stats and monitoring
 */
public class StorageEngineRepository {
  private static final Logger logger = LogManager.getLogger(StorageEngineRepository.class);

  /**
   *   Local storage engine for this node. This is lowest level persistence abstraction, these StorageEngines provide an iterator over their values.
   */
  private final ConcurrentMap<String, AbstractStorageEngine> localStorageEngines = new ConcurrentHashMap<>();

  /*
  Usual CRUD operations on map of Local Storage Engines
   */
  public boolean hasLocalStorageEngine(String name) {
    return localStorageEngines.containsKey(name);
  }

  public AbstractStorageEngine getLocalStorageEngine(String storeName) {
    return localStorageEngines.get(storeName);
  }

  public AbstractStorageEngine removeLocalStorageEngine(String storeName) {
    AbstractStorageEngine engine = localStorageEngines.remove(storeName);
    return engine;
  }

  public synchronized void addLocalStorageEngine(AbstractStorageEngine engine) {
    AbstractStorageEngine found = localStorageEngines.putIfAbsent(engine.getStoreName(), engine);
    if (found != null) {
      String errorMessage = "Storage Engine '" + engine.getStoreName() + "' has already been initialized.";
      logger.error(errorMessage);
      throw new VeniceException(errorMessage);
    }
  }

  public List<AbstractStorageEngine> getAllLocalStorageEngines() {
    return new ArrayList<>(localStorageEngines.values());
  }

  public void close() {
    VeniceException lastException = null;
    for (AbstractStorageEngine store: localStorageEngines.values()) {
      String storeName = store.getStoreName();
      logger.info("Closing storage engine for " + storeName);
      try {
        store.close();
      } catch (VeniceException e) {
        logger.error("Error closing storage engine for store" + storeName, e);
        lastException = e;
      }
    }

    if (lastException != null) {
      throw lastException;
    }
    logger.info("All stores closed.");
  }
}
