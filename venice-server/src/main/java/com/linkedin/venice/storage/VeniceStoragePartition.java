package com.linkedin.venice.storage;

/**
 * A generalized implementation of a storage partition.
 * All storage solutions need to extend this class.
 */
public abstract class VeniceStoragePartition {

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
