package com.linkedin.venice.store;

import com.linkedin.venice.storage.VeniceStorageException;


/**
 * The basic interface used for strictly querying storage.
 *
 * TODO: If needed add more query APIs for multi-get, filtered get, etc
 *
 * Note -
 * 1. Each of the APIs include an additional parameter "logicalPartitionId". This should be supplied
 *    by the client or the SimpleKafkaConsumerTask and aims to avoid computing the partition id in each request.
 * 2. In future if needed, we can add a wrapper API that will compute the partiion id from the key and  will
 *    invoke the appropriate underlying APIs as necessary.
 */
public interface QueryStore {
  /**
   * @return The name of the Store
   */
  public String getName();

  /**
   * Get the value associated with the given key
   *
   * @param logicalPartitionId Id of the logical partition where this key belongs
   * @param key The key to check for
   * @return The value associated with the key or an empty list if no values are found.
   * @throws com.linkedin.venice.storage.VeniceStorageException
   */
  public byte[] get(Integer logicalPartitionId, byte[] key)
      throws Exception;

  /**
   * Close the store.
   *
   * @throws VeniceStorageException If closing fails.
   */
  public void close()
      throws VeniceStorageException;
}
