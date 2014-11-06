package com.linkedin.venice.store;

import com.linkedin.venice.utils.CloseableIterator;
import com.linkedin.venice.utils.Pair;


/**
 * A base storage interface which is actually responsible for data persistence. This
 * interface implies all the usual responsibilities of a Store implementation,
 *
 * There are serval proposals for storage-partition model:
 *
 * 1. One storage engine for all stores
 *  1.1 One store uses one database, i.e. all partitions of the store will be in one database.
 *  1.2 One store uses multiple databases, i.e. one partition per database.
 * 2. Each store handled by one storage engine
 *  2.1 All partitions of the store will be handled in one database (current Voldemort implementation)
 *  2.2 One partition per database (Sudha suggests)
 * 3. Each partition handled by one storage engine (original proposal before today’s discussion, super high overhead)
 *
 * The point of having one storage engine(environment) or one database for one partition, is to simplify the complexity of rebalancing/partition migration/host swap.
 * The team agreed to take (2.2) as default storage-partition model for now, and run performance tests to see if it goes well.
 *
 *
 */
public interface StorageEngine extends Store {

  /**
   * Get an iterator over pairs of entries in the store. The key is the first
   * element in the pair and the value is the second element.
   *
   * Note that the iterator need not be threadsafe, and that it must be
   * manually closed after use.
   *
   * @return An iterator over the entries in this StorageEngine.
   */
  public CloseableIterator<Pair<byte[], byte[]>> entries();

  /**
   * Get an iterator over keys in the store.
   *
   * Note that the iterator need not be threadsafe, and that it must be
   * manually closed after use.
   *
   * @return An iterator over the keys in this StorageEngine.
   */
  public CloseableIterator<byte[]> keys();

  /**
   * Truncate all entries in the store
   */
  public void truncate();

  /**
   * A lot of storage engines support efficient methods for performing large
   * number of writes (puts/deletes) against the data source. This method puts
   * the storage engine in this batch write mode
   *
   * @return true if the storage engine took successful action to switch to
   *         'batch-write' mode
   */
  public boolean beginBatchWrites();

  /**
   *
   * @return true if the storage engine successfully returned to normal mode
   */
  public boolean endBatchWrites();
}
