package com.linkedin.venice.store;

import com.linkedin.venice.exceptions.PersistenceFailureException;
import com.linkedin.venice.exceptions.StorageInitializationException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.store.iterators.CloseableStoreEntriesIterator;
import com.linkedin.venice.store.iterators.CloseableStoreKeysIterator;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.Utils;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
  protected final AtomicBoolean isOpen;
  protected final Logger logger = Logger.getLogger(getClass());
  protected ConcurrentMap<Integer, AbstractStoragePartition> partitionIdToPartitionMap;

  public AbstractStorageEngine(String storeName) {
    this.storeName = storeName;
    this.isOpen = new AtomicBoolean(true);
    this.partitionIdToPartitionMap = new ConcurrentHashMap<>();
  }

  /**
   * Load the existing storage partitions.
   * The implementation should decide when to call this function properly to restore partitions.
   */
  protected synchronized void restoreStoragePartitions() {
    Set<Integer> partitionIds = getPersistedPartitionIds();
    partitionIds.forEach(this::addStoragePartition);
  }

  /**
   * List the existing partition ids for current storage engine persisted previously
   * @return
   */
  protected abstract Set<Integer> getPersistedPartitionIds();

  public abstract AbstractStoragePartition createStoragePartition(int partitionId);

  /**
   * Adds a partition to the current store
   *
   * @param partitionId - id of partition to add
   */
  public synchronized void addStoragePartition(int partitionId) {
    /**
     * If this method is called by anyone other than the constructor, i.e- the admin service, the caller should ensure
     * that after the addition of the storage partition:
     *  1. populate the partition node assignment repository
     */
    if (partitionIdToPartitionMap.containsKey(partitionId)) {
      logger.error("Failed to add a storage partition for partitionId: " + partitionId + " Store " + this.getName() +" . This partition already exists!");
      throw new StorageInitializationException("Partition " + partitionId + " of store " + this.getName() + " already exists.");
    }
    AbstractStoragePartition partition = createStoragePartition(partitionId);
    partitionIdToPartitionMap.put(partitionId, partition);
  }

  /**
   * Removes and returns a partition from the current store
   *
   * @param partitionId - id of partition to retrieve and remove
   */
  public synchronized void dropPartition(int partitionId) {
    /**
     * The caller of this method should ensure that:
     * 1. The SimpleKafkaConsumerTask associated with this partition is shutdown
     * 2. The partition node assignment repo is cleaned up and then remove this storage partition.
     *    Else there can be situations where the data is consumed from Kafka and not persisted.
     */
    if (!partitionIdToPartitionMap.containsKey(partitionId)) {
      logger.error("Failed to remove a non existing partition: " + partitionId + " Store " + this.getName() );
      return;
    }
    /* NOTE: bdb database is not closed here. */
    logger.info("Removing Partition: " + partitionId + " Store " + this.getName() );
    AbstractStoragePartition partition = partitionIdToPartitionMap.remove(partitionId);
    partition.drop();
    if(partitionIdToPartitionMap.size() == 0) {
      logger.info("All Partitions deleted for Store " + this.getName() );
    }
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
   * Get all Partition Ids which are assigned to the current Node.
   *
   * @return partition Ids that are hosted in the current Storage Engine.
   */
  public synchronized Set<Integer> getPartitionIds() {
    return partitionIdToPartitionMap.keySet();
  }

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
      String errorMessage = "PUT request failed for Key: " + ByteUtils.toLogString(key) + " . Invalid partition id: "
        + logicalPartitionId + " Store " + getName();
      logger.error(errorMessage);
      throw new PersistenceFailureException(errorMessage);
    }
    AbstractStoragePartition partition = this.partitionIdToPartitionMap.get(logicalPartitionId);
    partition.put(key, value);
  }

  public void delete(Integer logicalPartitionId, byte[] key) throws VeniceException {
    Utils.notNull(key, "Key cannot be null.");
    if (!containsPartition(logicalPartitionId)) {
      String errorMessage = "DELETE request failed for key: " + ByteUtils.toLogString(key) + " . Invalid partition id: "
        + logicalPartitionId + " Store " + getName();
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
        "GET request failed for key " + ByteUtils.toLogString(key) + " . Invalid partition id: " + logicalPartitionId
            + " Store " + getName();
      logger.error(errorMessage);
      throw new PersistenceFailureException(errorMessage);
    }
    AbstractStoragePartition partition = this.partitionIdToPartitionMap.get(logicalPartitionId);
    return partition.get(key);
  }

  public void close() throws PersistenceFailureException {
    this.isOpen.compareAndSet(true, false);
  }

  public Logger getLogger(){
    return logger;
  }
}
