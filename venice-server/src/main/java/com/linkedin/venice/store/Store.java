package com.linkedin.venice.store;

import com.linkedin.venice.storage.VeniceStorageException;
import com.linkedin.venice.utils.Utils;


/**
 * The basic interface used for storage. Allows the usual CRUD operations.
 */

public interface Store extends QueryStore {

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
}
