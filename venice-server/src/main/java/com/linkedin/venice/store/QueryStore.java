package com.linkedin.venice.store;

import com.linkedin.venice.storage.VeniceStorageException;


/**
 * The basic interface used for strictly querying storage.
 *
 * TODO: If needed add more query APIs for multi-get, filtered get, etc
 */
public interface QueryStore {
  /**
   * @return The name of the Store
   */
  public String getName();

  /**
   * Get the value associated with the given key
   *
   * @param key The key to check for
   * @return The value associated with the key or an empty list if no values are found.
   * @throws com.linkedin.venice.storage.VeniceStorageException
   */
  public byte[] get(byte[] key)
      throws VeniceStorageException;

  /**
   * Close the store.
   *
   * @throws VeniceStorageException If closing fails.
   */
  public void close()
      throws VeniceStorageException;
}
