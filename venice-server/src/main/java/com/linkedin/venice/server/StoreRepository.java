package com.linkedin.venice.server;

import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.storage.StorageService;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.StorageEngineFactory;
import com.linkedin.venice.store.Store;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.log4j.Logger;


/**
 *  A wrapper class that holds all the server's storage engines.
 *
 *  TODO 1. Later need to add stats and monitoring
 */
public class StoreRepository {

  private static final Logger logger = Logger.getLogger(StoreRepository.class);

  /**
   *   Local storage engine for this node. This is lowest level persistence abstraction, these StorageEngines provide an iterator over their values.
   */
  private final ConcurrentMap<String, AbstractStorageEngine> localStorageEngines;


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
    return new ArrayList<>(localStorageEngines.values());
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
