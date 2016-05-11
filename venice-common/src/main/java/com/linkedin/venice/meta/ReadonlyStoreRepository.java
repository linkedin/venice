package com.linkedin.venice.meta;

import com.linkedin.venice.VeniceResource;


/**
 * Interface defined readonly operations to access stores.
 */
public interface ReadonlyStoreRepository extends VeniceResource {
  /**
   * Get one store by given name from repository.
   *
   * @param name name of wanted store.
   *
   * @return Store for given name.
   */
  public Store getStore(String name);
}
