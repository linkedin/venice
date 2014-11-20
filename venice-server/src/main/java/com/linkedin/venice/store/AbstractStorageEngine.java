package com.linkedin.venice.store;

import com.linkedin.venice.server.PartitionNodeAssignmentRepository;
import com.linkedin.venice.server.VeniceConfig;
import com.linkedin.venice.storage.VeniceStorageException;
import com.linkedin.venice.utils.CloseableIterator;
import com.linkedin.venice.utils.Pair;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.log4j.Logger;


/**
 * A base storage abstract class which is actually responsible for data persistence. This
 * abstract class implies all the usual responsibilities of a Store implementation,
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
 * TODO: This is just a stub code for now. I plan to populate the methods later when implementing the storage engines.
 */
public abstract class AbstractStorageEngine implements Store {
  private final String storeName;
  protected final Properties storeDef;
  protected final VeniceConfig config;
  protected final AtomicBoolean isOpen;
  protected final PartitionNodeAssignmentRepository partitionNodeAssignmentRepo;

  public AbstractStorageEngine(VeniceConfig config, Properties storeDef,
      PartitionNodeAssignmentRepository partitionNodeAssignmentRepo) {
    this.config = config;
    this.storeDef = storeDef;
    storeName = storeDef.getProperty("name");
    this.isOpen = new AtomicBoolean(true);
    this.partitionNodeAssignmentRepo = partitionNodeAssignmentRepo;
  }

  /**
   * Get store name served by this storage engine
   * @return associated storeName
   */
  public String getStoreName() {
    return this.storeName;
  }

  /**
   *
   * Get an iterator over pairs of entries in the store. The key is the first
   * element in the pair and the value is the second element.
   *
   * Note that the iterator need not be threadsafe, and that it must be
   * manually closed after use.
   *
   * @return An iterator over the entries in this AbstractStorageEngine.
   */
  public abstract CloseableIterator<Pair<byte[], byte[]>> entries();

  /**
   * /**
   * Get an iterator over keys in the store.
   *
   * Note that the iterator need not be threadsafe, and that it must be
   * manually closed after use.
   *
   * @return An iterator over the keys in this AbstractStorageEngine.
   */
  public abstract CloseableIterator<byte[]> keys();

  /**
   * Truncate all entries in the store
   */
  public abstract void truncate();

  /**
   * A lot of storage engines support efficient methods for performing large
   * number of writes (puts/deletes) against the data source. This method puts
   * the storage engine in this batch write mode
   *
   * @return true if the storage engine took successful action to switch to
   *         'batch-write' mode
   */
  public boolean beginBatchWrites() {
    return false;
  }

  /**
   *
   * @return true if the storage engine successfully returned to normal mode
   */
  public boolean endBatchWrites() {
    return false;
  }

  public String getName() {
    return null;
  }

  public abstract byte[] get(Integer logicalPartitionId, byte[] key)
      throws VeniceStorageException;

  public abstract void put(Integer logicalPartitionId, byte[] key, byte[] value)
      throws VeniceStorageException;

  public abstract void delete(Integer logicalPartitionId, byte[] key)
      throws VeniceStorageException;

  public void close()
      throws VeniceStorageException {
    this.isOpen.compareAndSet(true, false);
  }
}
