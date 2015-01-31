package com.linkedin.venice.store;

import com.linkedin.venice.storage.VeniceStorageException;


/**
 * The basic interface used for storage. Allows the usual CRUD operations.
 *
 * Note -
 * 1. Each of the APIs include an additional parameter "logicalPartitionId". This should be supplied
 *    by the client or the SimpleKafkaConsumerTask and aims to avoid computing the partition id in each request.
 * 2. In future if needed, we can add a wrapper API that will compute the partiion id from the key and  will
 *    invoke the appropriate underlying APIs as necessary.
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
   * @param logicalPartitionId Id of the logical partition where this key belongs
   * @param key  The key to put
   * @param value  The value associated with the key
   * @throws VeniceStorageException
   */
  public void put(Integer logicalPartitionId, byte[] key, byte[] value)
      throws Exception;

  /**
   * Delete entry corresponding to the given key
   *
   * @param logicalPartitionId Id of the logical partition where this key belongs
   * @param key  The key to delete
   * @throws VeniceStorageException
   */
  public void delete(Integer logicalPartitionId, byte[] key)
      throws Exception;
}
