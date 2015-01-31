package com.linkedin.venice.store;

import com.linkedin.venice.utils.partition.iterators.AbstractCloseablePartitionEntriesIterator;
import com.linkedin.venice.utils.partition.iterators.CloseablePartitionKeysIterator;


/**
 * An abstract implementation of a storage partition. This could be a database in BDB
 * environment or a concurrent hashmap incase of an inMemory implementation depending
 * on the storage-partition model.
 */
public abstract class AbstractStoragePartition {
  private final Integer partitionId;

  public AbstractStoragePartition(Integer partitionId) {
    this.partitionId = partitionId;
  }

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
   */
  public abstract byte[] get(byte[] key)
      throws Exception;

  /**
   * Delete a key from the partition database
   */
  public abstract void delete(byte[] key);

  /**
   *
   * Get an iterator over pairs of entries in the partition. The key is the first
   * element in the pair and the value is the second element.
   *
   * Note that the iterator need not be threadsafe, and that it must be
   * manually closed after use.
   *
   * @return An iterator over the entries in this AbstractStoragePartition.
   */
  public abstract AbstractCloseablePartitionEntriesIterator partitionEntries();

  /**
   * /**
   * Get an iterator over keys in the partition.
   *
   * Note that the iterator need not be threadsafe, and that it must be
   * manually closed after use.
   *
   * @return An iterator over the keys in this AbstractStoragePartition.
   */
  public abstract CloseablePartitionKeysIterator partitionKeys();

  /**
   *  Truncate all entries in the partition
   */
  public abstract void truncate();
}
