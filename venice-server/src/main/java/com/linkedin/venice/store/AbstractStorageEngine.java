package com.linkedin.venice.store;

import com.linkedin.venice.exceptions.PersistenceFailureException;
import com.linkedin.venice.exceptions.StorageInitializationException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.Utils;
import java.util.Collections;
import java.util.Map;
import java.nio.ByteBuffer;
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
  protected final Logger logger = Logger.getLogger(AbstractStorageEngine.class);
  protected ConcurrentMap<Integer, AbstractStoragePartition> partitionIdToPartitionMap;

  public AbstractStorageEngine(String storeName) {
    this.storeName = storeName;
    this.isOpen = new AtomicBoolean(true);
    this.partitionIdToPartitionMap = new ConcurrentHashMap<>();
  }

  public abstract PersistenceType getType();

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

  public abstract AbstractStoragePartition createStoragePartition(StoragePartitionConfig storagePartitionConfig);

  public synchronized void addStoragePartition(int partitionId) {
    addStoragePartition(new StoragePartitionConfig(storeName, partitionId));
  }

  /**
   * Adjust the opened storage partition according to the provided storagePartitionConfig.
   * It will throw exception if there is no opened storage partition for the given partition id.
   * @param storagePartitionConfig
   */
  protected synchronized void adjustStoragePartition(StoragePartitionConfig storagePartitionConfig) {
    validateStoreName(storagePartitionConfig);
    int partitionId = storagePartitionConfig.getPartitionId();
    if (!containsPartition(partitionId)) {
      throw new VeniceException("There is no opened storage partition for store: " + getName() +
      ", partition id: " + partitionId + ", please open it first");
    }
    AbstractStoragePartition partition = this.partitionIdToPartitionMap.get(partitionId);
    if (partition.verifyConfig(storagePartitionConfig)) {
      logger.info("No adjustment needed for store name: " + getName() + ", partition id: " + partitionId);
      return;
    }
    // Need to re-open storage partition according to the provided partition config
    logger.info("Reopen database with storage partition config: " + storagePartitionConfig);
    closePartition(partitionId);
    addStoragePartition(storagePartitionConfig);
  }

  private void validateStoreName(StoragePartitionConfig storagePartitionConfig) {
    if (!storagePartitionConfig.getStoreName().equals(getName())) {
      throw new VeniceException("Store name in partition config: " + storagePartitionConfig.getStoreName() + " doesn't match current store engine: " + getName());
    }
  }

  /**
   * Adds a partition to the current store
   *
   * @param storagePartitionConfig - config for the partition to be added
   */
  public synchronized void addStoragePartition(StoragePartitionConfig storagePartitionConfig) {
    validateStoreName(storagePartitionConfig);
    int partitionId = storagePartitionConfig.getPartitionId();
    /**
     * If this method is called by anyone other than the constructor, i.e- the admin service, the caller should ensure
     * that after the addition of the storage partition:
     *  1. populate the partition node assignment repository
     */
    if (containsPartition(partitionId)) {
      logger.error("Failed to add a storage partition for partitionId: " + partitionId + " Store " + this.getName() +" . This partition already exists!");
      throw new StorageInitializationException("Partition " + partitionId + " of store " + this.getName() + " already exists.");
    }
    AbstractStoragePartition partition = createStoragePartition(storagePartitionConfig);
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
    if (!containsPartition(partitionId)) {
      logger.error("Failed to remove a non existing partition: " + partitionId + " Store " + this.getName() );
      return;
    }
    /* NOTE: bdb database is not closed here. */
    logger.info("Removing Partition: " + partitionId + " Store " + this.getName() );
    AbstractStoragePartition partition = partitionIdToPartitionMap.remove(partitionId);
    partition.drop();
    if(partitionIdToPartitionMap.size() == 0) {
      logger.info("All Partitions deleted for Store " + this.getName() );
      /**
       * The reason to invoke {@link #drop} here is that storage engine might need to do some cleanup
       * in the store level.
       */
      drop();
    }
  }

  /**
   * Drop the whole store
   */
  public synchronized void drop() {
    logger.info("Started dropping store: " + getName());
    partitionIdToPartitionMap.forEach( (partitionId, partition) -> dropPartition(partitionId));
    logger.info("Finished dropping store: " + getName());
  }

  public synchronized void closePartition(int partitionId) {
    if (!containsPartition(partitionId)) {
      logger.error("Failed to close a non existing partition: " + partitionId + " Store " + this.getName() );
      return;
    }
    AbstractStoragePartition partition = partitionIdToPartitionMap.remove(partitionId);
    partition.close();
    if(partitionIdToPartitionMap.size() == 0) {
      logger.info("All Partitions closed for Store " + this.getName() );
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
   * A lot of storage engines support efficient methods for performing large
   * number of writes (puts/deletes) against the data source. This method puts
   * the storage engine in this batch write mode
   */
  public void beginBatchWrite(StoragePartitionConfig storagePartitionConfig, Map<String, String> checkpointedInfo) {
    logger.info("Begin batch write for storage partition config: " + storagePartitionConfig + " with checkpointed info: " + checkpointedInfo);
    final int partitionId = storagePartitionConfig.getPartitionId();
    /**
     * We want to adjust the storage partition first since it will possibly re-open the underlying database in
     * different mode.
     */
    adjustStoragePartition(storagePartitionConfig);
    getStoragePartition(partitionId).beginBatchWrite(checkpointedInfo);
  }

  /**
   * @return true if the storage engine successfully returned to normal mode
   */
  public void endBatchWrite(StoragePartitionConfig storagePartitionConfig) {
    logger.info("End batch write for storage partition config: " + storagePartitionConfig);
    final int partitionId = storagePartitionConfig.getPartitionId();
    getStoragePartition(partitionId).endBatchWrite();
    /**
     * After end of batch push, we would like to adjust the underlying database for the future ingestion, such as from streaming.
     */
    adjustStoragePartition(storagePartitionConfig);
  }

  private void validatePartitionForKey(Integer logicalPartitionId, byte[] key, String operationType) {
    Utils.notNull(key, "Key cannot be null.");
    if (!containsPartition(logicalPartitionId)) {
      String errorMessage = operationType + " request failed for Key: " + ByteUtils.toLogString(key) + " . Invalid partition id: "
          + logicalPartitionId + " Store " + getName();
      logger.error(errorMessage);
      throw new PersistenceFailureException(errorMessage);
    }
  }

  public void put(Integer logicalPartitionId, byte[] key, byte[] value) throws VeniceException {
    validatePartitionForKey(logicalPartitionId, key, "Put");
    AbstractStoragePartition partition = this.partitionIdToPartitionMap.get(logicalPartitionId);
    partition.put(key, value);
  }

  public void put(Integer logicalPartitionId, byte[] key, ByteBuffer value) throws VeniceException {
    validatePartitionForKey(logicalPartitionId, key, "Put");
    AbstractStoragePartition partition = this.partitionIdToPartitionMap.get(logicalPartitionId);
    partition.put(key, value);
  }

  public void delete(Integer logicalPartitionId, byte[] key) throws VeniceException {
    validatePartitionForKey(logicalPartitionId, key, "Delete");
    AbstractStoragePartition partition = this.partitionIdToPartitionMap.get(logicalPartitionId);
    partition.delete(key);
  }

  public byte[] get(Integer logicalPartitionId, byte[] key) throws VeniceException {
    validatePartitionForKey(logicalPartitionId, key, "Get");
    AbstractStoragePartition partition = this.partitionIdToPartitionMap.get(logicalPartitionId);
    return partition.get(key);
  }

  public Map<String, String> sync(int partitionId) {
    if (!containsPartition(partitionId)) {
      logger.warn("Partition " + partitionId + " doesn't exist, no sync operation will be executed");
      return Collections.emptyMap();
    }
    AbstractStoragePartition partition = this.partitionIdToPartitionMap.get(partitionId);
    return partition.sync();
  }

  public void close() throws PersistenceFailureException {
    this.isOpen.compareAndSet(true, false);
  }

  public Logger getLogger(){
    return logger;
  }

  // for test purpose
  public AbstractStoragePartition getStoragePartition(int partitionId) {
    if (!this.partitionIdToPartitionMap.containsKey(partitionId)) {
      throw new VeniceException("Partition: " + partitionId + " of store: " + getName() + " doesn't exist");
    }
    return this.partitionIdToPartitionMap.get(partitionId);
  }
}
