package com.linkedin.venice.store;

import com.linkedin.venice.storage.VeniceStorageException;


/**
 * The basic interface used for storage. Allows the usual CRUD operations.
 */

public interface Store {

  /**
   * @return The name of the Store
   */
  public String getName();

  /**
   * Get the value associated with the given key
   *
   * @param key The key to check for
   * @return The value associated with the key or an empty list if no values are found.
   * @throws VeniceStorageException
   */
  public byte[] get(byte[] key)
      throws VeniceStorageException;

  /**
   * Associate the value with the key in this store
   * <p/>
   * TODO: Partial put should be implemented by layers above storage. They should do the following :
   * 1. Get the current values using get
   * 2. Compute the new value based on existing and partial update
   * 3. Invoke put on the storage with the whole key and value
   *
   * @param key  The key to put
   * @param value  The value associated with the key
   * @throws VeniceStorageException
   */
  public void put(byte[] key, byte[] value)
      throws VeniceStorageException;

  /**
   * Delete entry corresponding to the given key
   *
   * @param key The key to delete
   * @throws VeniceStorageException
   */
  public void delete(byte[] key)
      throws VeniceStorageException;

  /**
   * Close the store.
   *
   * @throws VeniceStorageException If closing fails.
   */
  public void close()
      throws VeniceStorageException;
}
