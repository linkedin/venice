package com.linkedin.davinci.storage;

import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.venice.exceptions.VeniceException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 *  A wrapper class that holds all the server's storage engines.
 */
public class StorageEngineRepository {
  private static final Logger LOGGER = LogManager.getLogger(StorageEngineRepository.class);

  /**
   *   Local storage engine for this node. This is lowest level persistence abstraction, these StorageEngines provide an iterator over their values.
   */
  private final ConcurrentMap<String, StorageEngine> localStorageEngines = new ConcurrentHashMap<>();

  public StorageEngine getLocalStorageEngine(String storeName) {
    return localStorageEngines.get(storeName);
  }

  public StorageEngine removeLocalStorageEngine(String storeName) {
    StorageEngine engine = localStorageEngines.remove(storeName);
    return engine;
  }

  public synchronized void addLocalStorageEngine(StorageEngine engine) {
    StorageEngine found = localStorageEngines.putIfAbsent(engine.getStoreVersionName(), engine);
    if (found != null) {
      String errorMessage = "Storage Engine '" + engine.getStoreVersionName() + "' has already been initialized.";
      LOGGER.error(errorMessage);
      throw new VeniceException(errorMessage);
    }
  }

  public List<StorageEngine> getAllLocalStorageEngines() {
    return new ArrayList<>(localStorageEngines.values());
  }

  public void close() {
    VeniceException lastException = null;
    for (StorageEngine store: localStorageEngines.values()) {
      String storeName = store.getStoreVersionName();
      LOGGER.info("Closing storage engine for store: {}", storeName);
      try {
        store.close();
      } catch (VeniceException e) {
        LOGGER.error("Error closing storage engine for store: {}", storeName, e);
        lastException = e;
      }
    }

    if (lastException != null) {
      throw lastException;
    }
    LOGGER.info("All stores closed.");
  }
}
