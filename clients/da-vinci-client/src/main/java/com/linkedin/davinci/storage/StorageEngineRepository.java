package com.linkedin.davinci.storage;

import com.linkedin.davinci.store.DelegatingStorageEngine;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.ArrayList;
import java.util.List;
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
  private final ConcurrentMap<String, DelegatingStorageEngine> localStorageEngines = new VeniceConcurrentHashMap<>();

  public StorageEngine getLocalStorageEngine(String storeName) {
    return getDelegatingStorageEngine(storeName);
  }

  /**
   * Package-private on purpose. Not all code paths should be made aware of the delegating nature of the storage engine.
   */
  DelegatingStorageEngine getDelegatingStorageEngine(String storeName) {
    return localStorageEngines.get(storeName);
  }

  public StorageEngine removeLocalStorageEngine(String storeName) {
    StorageEngine engine = localStorageEngines.remove(storeName);
    return engine;
  }

  /**
   * Package-private on purpose. We want to limit code paths having the ability to add state to the repository.
   *
   * @param engine a {@link DelegatingStorageEngine}
   */
  synchronized void addLocalStorageEngine(DelegatingStorageEngine engine) {
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
