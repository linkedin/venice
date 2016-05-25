package com.linkedin.venice.store;

import com.linkedin.venice.exceptions.PersistenceFailureException;
import com.linkedin.venice.utils.partition.iterators.AbstractCloseablePartitionEntriesIterator;
import com.linkedin.venice.utils.partition.iterators.CloseablePartitionKeysIterator;
import org.apache.log4j.Logger;


/**
 * An abstract implementation of a storage partition. This could be a database in BDB
 * environment or a concurrent hashmap incase of an inMemory implementation depending
 * on the storage-partition model.
 */
public abstract class AbstractStoragePartition {

  protected final Logger logger = Logger.getLogger(getClass());

  protected final Integer partitionId;

  public AbstractStoragePartition(Integer partitionId) { this.partitionId = partitionId; }

  /**
   * returns the id of this partition
   */
  public Integer getPartitionId() {
    return this.partitionId;
  }

  /**
   * Puts a value into the partition database
   */
  public abstract void put(byte[] key, byte[] value);

  /**
   * Get a value from the partition database
   * @param key key to be retrieved
   * @return null if the key does not exist, byte[] value if it exists.
   */
  public abstract byte[] get(byte[] key);

  /**
   * Delete a key from the partition database
   */
  public abstract void delete(byte[] key);

  /**
   * Get an iterator over pairs of entries in the partition. The key is the first
   * element in the pair and the value is the second element.
   * <p/>
   * Note that the iterator need not be threadsafe, and that it must be
   * manually closed after use.
   *
   * @return An iterator over the entries in this AbstractStoragePartition.
   */
  public abstract AbstractCloseablePartitionEntriesIterator partitionEntries();

  /**
   * /**
   * Get an iterator over keys in the partition.
   * <p/>
   * Note that the iterator need not be threadsafe, and that it must be
   * manually closed after use.
   *
   * @return An iterator over the keys in this AbstractStoragePartition.
   */
  public abstract CloseablePartitionKeysIterator partitionKeys();

  /**
   * Truncate all entries in the partition.
   *
   */
  public abstract void truncate();

  /**
   * Drop when it is not required anymore.
   */
  public abstract void drop();

  /**
   * Close the specific partition
   */
  public abstract void close();
}
