package com.linkedin.venice.meta;

import com.linkedin.venice.VeniceResource;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;


/**
 * Interface defined readonly operations to access stores.
 */
public interface ReadOnlyStoreRepository extends VeniceResource {
  /**
   * Get one store by given name from repository.
   *
   * @param name name of wanted store.
   *
   * @return Store for given name.
   */
  Store getStore(String name);

  /**
   * Whether the store exists or not.
   *
   * @param name store name
   * @return
   */
  boolean hasStore(String name);

  /**
   * Get total read quota of all stores.
   */
  long getTotalStoreReadQuota();

  /**
   * Get all stores in the current repository
   * @return
   */
  List<Store> getAllStores();

  /**
   * Register store data change listener.
   *
   * @param listener
   */
  void registerStoreDataChangedListener(StoreDataChangedListener listener);

  /**
   * Unregister store data change listener.
   * @param listener
   */
  void unregisterStoreDataChangedListener(StoreDataChangedListener listener);

  /**
   * Return the internal lock, so that {@link ReadOnlySchemaRepository} will reuse it to avoid deadlock issue
   */
  ReadWriteLock getInternalReadWriteLock();

  /**
   * Whether Router cache is enabled for the specified store
   * @return
   */
  boolean isSingleGetRouterCacheEnabled(String name);

  /**
   * Whether Router cache for batch get is enabled for the specified store
   * @param name name of the store
   * @return
   */
  boolean isBatchGetRouterCacheEnabled(String name);

  /**
   * Get batch-get limit for the specified store
   * @param name
   * @return
   */
  int getBatchGetLimit(String name);

  /**
   * Whether computation is enabled for the specified store.
   * @param name store name
   * @return
   */
  boolean isReadComputationEnabled(String name);
}
