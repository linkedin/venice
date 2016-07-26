package com.linkedin.venice.meta;

import com.linkedin.venice.VeniceResource;


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
  public Store getStore(String name);

  /**
   * Whether the store exists or not.
   *
   * @param name store name
   * @return
   */
  boolean hasStore(String name);

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
}
