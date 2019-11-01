package com.linkedin.venice.meta;

import com.linkedin.venice.VeniceResource;
import com.linkedin.venice.exceptions.VeniceNoStoreException;

import java.util.List;


/**
 * Interface defined readonly operations to access stores.
 */
public interface ReadOnlyStoreRepository extends VeniceResource {
  /**
   * Get one store by given name from repository.
   *
   * @param storeName name of wanted store.
   *
   * @return Store for given name.
   */
  Store getStore(String storeName);
  Store getStoreOrThrow(String storeName) throws VeniceNoStoreException;

  /**
   * Whether the store exists or not.
   *
   * @param storeName store name
   * @return
   */
  boolean hasStore(String storeName);

  /**
   * Selective refresh operation which fetches one store from ZK
   *
   * @param storeName store name
   * @return the newly refreshed store
   */
  Store refreshOneStore(String storeName);

  /**
   * Get all stores in the current repository
   * @return
   */
  List<Store> getAllStores();

  /**
   * Get total read quota of all stores.
   */
  long getTotalStoreReadQuota();

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
   * Get batch-get limit for the specified store
   * @param storeName
   * @return
   */
  int getBatchGetLimit(String storeName);

  /**
   * Whether computation is enabled for the specified store.
   * @param storeName store name
   * @return
   */
  boolean isReadComputationEnabled(String storeName);

  /**
   * Whether Router cache is enabled for the specified store
   * @return
   */
  boolean isSingleGetRouterCacheEnabled(String storeName);

  /**
   * Whether Router cache for batch get is enabled for the specified store
   * @param storeName name of the store
   * @return
   */
  boolean isBatchGetRouterCacheEnabled(String storeName);
}
