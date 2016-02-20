package com.linkedin.venice.store;

import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.exceptions.PersistenceFailureException;
import com.linkedin.venice.exceptions.StorageInitializationException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.server.PartitionAssignmentRepository;
import com.linkedin.venice.store.iterators.CloseableStoreEntriesIterator;
import com.linkedin.venice.store.iterators.CloseableStoreKeysIterator;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.Utils;
import org.apache.log4j.Logger;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * A base storage abstract class which is actually responsible for data persistence. This
 * abstract class implies all the usual responsibilities of a Store implementation,
 * <p/>
 * There are several proposals for storage-partition model:
 * <p/>
 * 1. One storage engine for all stores
 * 1.1 One store uses one database, i.e. all partitions of the store will be in one database.
 * 1.2 One store uses multiple databases, i.e. one partition per database.
 * 2. Each store handled by one storage engine
 * 2.1 All partitions of the store will be handled in one database (current Voldemort implementation)
 * 2.2 One partition per database (Sudha suggests)
 * 3. Each partition handled by one storage engine (original proposal before today’s discussion, super high overhead)
 * <p/>
 * The point of having one storage engine(environment) or one database for one partition, is to simplify the complexity of rebalancing/partition migration/host swap.
 * The team agreed to take (2.2) as default storage-partition model for now, and run performance tests to see if it goes well.
 */
public abstract class AbstractStorageEngine implements Store {
  private final String storeName;
  protected final VeniceStoreConfig storeDef;
  protected final AtomicBoolean isOpen;
  protected final PartitionAssignmentRepository partitionNodeAssignmentRepo;
  protected final Logger logger = Logger.getLogger(getClass());
  protected ConcurrentMap<Integer, AbstractStoragePartition> partitionIdToPartitionMap;

  public AbstractStorageEngine(VeniceStoreConfig storeDef,
                               PartitionAssignmentRepository partitionNodeAssignmentRepo,
                               ConcurrentMap<Integer, AbstractStoragePartition> partitionIdToPartitionMap) {

    this.storeDef = storeDef;
    storeName = storeDef.getStoreName();
    this.isOpen = new AtomicBoolean(true);
    this.partitionNodeAssignmentRepo = partitionNodeAssignmentRepo;
    this.partitionIdToPartitionMap = partitionIdToPartitionMap;
  }

  /**
   * Get store name served by this storage engine
   *
   * @return associated storeName
   */
  public String getName() {
    return this.storeName;
  }

  /**
   * Return true or false based on whether a given partition exists within this storage engine
   *
   * @param partitionId The partition to look for
   * @return True/False, does the partition exist on this node
   */
  public boolean containsPartition(int partitionId) {
    return partitionIdToPartitionMap.containsKey(partitionId);
  }

  /**
   * Adds a partition to the current store
   *
   * @param partitionId - id of partition to add
   */
  public abstract void addStoragePartition(int partitionId)
    throws StorageInitializationException;

  /**
   * Removes and returns a partition from the current store
   *
   * @param partitionId - id of partition to retrieve and remove
   * @return The AbstractStoragePartition object removed by this operation
   */
  public abstract AbstractStoragePartition removePartition(int partitionId)
    throws VeniceException;

  /**
   * Get an iterator over entries in the store. The key is the first
   * element in the entry and the value is the second element.
   * <p/>
   * The iterator iterates over every partition in the store and inside
   * each partition, iterates over the partition entries.
   *
   * @return An iterator over the entries in this AbstractStorageEngine.
   */
  public abstract CloseableStoreEntriesIterator storeEntries()
    throws VeniceException;

  /**
   * Get an iterator over keys in the store.
   * <p/>
   * The iterator returns the key element from the storeEntries
   *
   * @return An iterator over the keys in this AbstractStorageEngine.
   */
  public abstract CloseableStoreKeysIterator storeKeys()
    throws VeniceException;

  /**
   * Truncate all entries in the store
   */
  public void truncate() {
    for (AbstractStoragePartition partition : this.partitionIdToPartitionMap.values()) {
      partition.truncate();
    }
  }

  /**
   * A lot of storage engines support efficient methods for performing large
   * number of writes (puts/deletes) against the data source. This method puts
   * the storage engine in this batch write mode
   *
   * @return true if the storage engine took successful action to switch to
   * 'batch-write' mode
   */
  public boolean beginBatchWrites() {
    return false;
  }

  /**
   * @return true if the storage engine successfully returned to normal mode
   */
  public boolean endBatchWrites() {
    return false;
  }

  public void put(Integer logicalPartitionId, byte[] key, byte[] value) throws VeniceException {
    Utils.notNull(key, "Key cannot be null.");
    if (!containsPartition(logicalPartitionId)) {
      String errorMessage = "PUT request failed for Key: " + ByteUtils.toHexString(key) + " . Invalid partition id: "
        + logicalPartitionId;
      logger.error(errorMessage);
      throw new PersistenceFailureException(errorMessage);
    }
    AbstractStoragePartition partition = this.partitionIdToPartitionMap.get(logicalPartitionId);
    partition.put(key, value);
  }

  public void delete(Integer logicalPartitionId, byte[] key) throws VeniceException {
    Utils.notNull(key, "Key cannot be null.");
    if (!containsPartition(logicalPartitionId)) {
      String errorMessage = "DELETE request failed for key: " + ByteUtils.toHexString(key) + " . Invalid partition id: "
        + logicalPartitionId;
      logger.error(errorMessage);
      throw new PersistenceFailureException(errorMessage);
    }
    AbstractStoragePartition partition = this.partitionIdToPartitionMap.get(logicalPartitionId);
    partition.delete(key);
  }

  public byte[] get(Integer logicalPartitionId, byte[] key) throws VeniceException {
    Utils.notNull(key, "Key cannot be null.");
    if (!containsPartition(logicalPartitionId)) {
      String errorMessage =
        "GET request failed for key " + ByteUtils.toHexString(key) + " . Invalid partition id: " + logicalPartitionId;
      logger.error(errorMessage);
      throw new PersistenceFailureException(errorMessage);
    }
    AbstractStoragePartition partition = this.partitionIdToPartitionMap.get(logicalPartitionId);
    return partition.get(key);
  }

  public void close() throws PersistenceFailureException {
    this.isOpen.compareAndSet(true, false);
  }

  protected void initialStoreForPatitions(PartitionAssignmentRepository partitionAssignmentRepository){
    // Create and initialize the individual databases for each partition
    for (int partitionId : partitionNodeAssignmentRepo.getLogicalPartitionIds(this.getName())) {
      addStoragePartition(partitionId);
    }
  }

  public Logger getLogger(){
    return logger;
  }
}
