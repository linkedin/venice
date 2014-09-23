package com.linkedin.venice.storage;

import java.util.HashMap;

/**
 * Created by clfung on 9/29/14.
 */
public abstract class VeniceStorePartition {

  /**
   * Returns the id of this given partition
   */
  public abstract int getId();

  /**
   * Puts a value into the key value store
   */
  public abstract void put(String key, Object payload);


  /**
   * Gets a value from the key value store
   */
  public abstract Object get(String key);

  /**
   * Deletes a value from the key value store
   */
  public abstract void delete(String key);

}
