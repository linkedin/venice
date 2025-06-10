package com.linkedin.davinci.store;

import static com.linkedin.davinci.store.StoragePartitionAdjustmentTrigger.BEGIN_BATCH_PUSH;
import static com.linkedin.davinci.store.StoragePartitionAdjustmentTrigger.CHECK_DATABASE_INTEGRITY;
import static com.linkedin.davinci.store.StoragePartitionAdjustmentTrigger.END_BATCH_PUSH;

import com.linkedin.davinci.callback.BytesStreamingCallback;
import com.linkedin.venice.exceptions.PersistenceFailureException;
import com.linkedin.venice.exceptions.StorageInitializationException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.SparseConcurrentList;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


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
public abstract class AbstractStorageEngine<Partition extends AbstractStoragePartition>
    implements StorageEngine<Partition> {
  private static final Logger LOGGER = LogManager.getLogger(AbstractStorageEngine.class);

  private static final byte[] VERSION_METADATA_KEY = "VERSION_METADATA".getBytes();
  private static final String PARTITION_METADATA_PREFIX = "P_";

  // Using a large positive number for metadata partition id instead of -1 can avoid database naming issues.
  public static final int METADATA_PARTITION_ID = 1000_000_000;

  private final String storeVersionName;
  private final SparseConcurrentList<Partition> partitionList = new SparseConcurrentList<>();
  private Partition metadataPartition;
  private final AtomicReference<StoreVersionState> versionStateCache = new AtomicReference<>();
  private final InternalAvroSpecificSerializer<StoreVersionState> storeVersionStateSerializer;
  private final InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer;

  private boolean suppressLogs = false;

  /**
   * This lock is used to guard the re-opening logic in {@link #adjustStoragePartition} since
   * {@link #getPartitionOrThrow} is not synchronized and it could be invoked during the execution
   * of {@link #adjustStoragePartition}.
   *
   * The reason to introduce this rw lock is that for write-compute enabled stores, the lookup could happen
   * during the ingestion even before reporting ready-to-serve yet. And right now, the lookup is happening
   * in consumer thread, and the {@link #adjustStoragePartition} will be invoked in drainer thread.
   *
   * TODO: evaluate whether we could remove `synchronized` keyword from the functions in this class.
   */
  private final List<ReadWriteLock> rwLockForStoragePartitionAdjustmentList = new SparseConcurrentList<>();

  public AbstractStorageEngine(
      String storeVersionName,
      InternalAvroSpecificSerializer<StoreVersionState> storeVersionStateSerializer,
      InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer) {
    this.storeVersionName = storeVersionName;
    this.metadataPartition = null;
    this.storeVersionStateSerializer = storeVersionStateSerializer;
    this.partitionStateSerializer = partitionStateSerializer;
  }

  /**
   * Making it public is for testing purpose.
   */
  public ReadWriteLock getRWLockForPartitionOrThrow(int partitionId) {
    ReadWriteLock readWriteLock = rwLockForStoragePartitionAdjustmentList.get(partitionId);
    if (readWriteLock == null) {
      throw new VeniceException(
          "Failed to get read-write lock for partition: " + partitionId + ", store: " + getStoreVersionName());
    }
    return readWriteLock;
  }

  @Override
  public String getStoreVersionName() {
    return storeVersionName;
  }

  @Override
  public String toString() {
    return getStoreVersionName();
  }

  public abstract Partition createStoragePartition(StoragePartitionConfig partitionConfig);

  /**
   * Load the existing storage partitions.
   * The implementation should decide when to call this function properly to restore partitions.
   */
  protected synchronized void restoreStoragePartitions(
      boolean restoreMetadataPartition,
      boolean restoreDataPartitions) {
    Set<Integer> partitionIds = getPersistedPartitionIds();

    /**
     * We remove the special partition IDs from the set because we don't want to store them in the
     * {@link #partitionList}, as that would blow up the array size of the collection, causing memory
     * pressure and potentially OOMing.
     */
    partitionIds.remove(METADATA_PARTITION_ID);

    if (restoreMetadataPartition) {
      LOGGER.info("Metadata partition restore enabled. Restoring metadata partition.");
      this.metadataPartition =
          createStoragePartition(new StoragePartitionConfig(storeVersionName, METADATA_PARTITION_ID));
    }

    if (restoreDataPartitions) {
      LOGGER.info("Data partitions restore enabled. Restoring data partitions.");
      partitionIds.stream()
          .sorted((o1, o2) -> Integer.compare(o2, o1)) // reverse order, to minimize array resizing in {@link
                                                       // SparseConcurrentList}
          .forEach(this::addStoragePartitionIfAbsent);
    }
  }

  protected final synchronized void restoreStoragePartitions() {
    restoreStoragePartitions(true, true);
  }

  // For testing purpose only.
  protected AbstractStoragePartition getMetadataPartition() {
    return metadataPartition;
  }

  /**
   * Adjust the opened storage partition according to the provided storagePartitionConfig.
   * It will throw exception if there is no opened storage partition for the given partition id.
   *
   * The reason to have {@param partitionId} is mainly used to ease the unit test.
   */
  @Override
  public synchronized void adjustStoragePartition(
      int partitionId,
      StoragePartitionAdjustmentTrigger mode,
      StoragePartitionConfig partitionConfig) {
    validateStoreName(partitionConfig);
    if (partitionId != partitionConfig.getPartitionId()) {
      throw new VeniceException(
          "StoragePartitionConfig should contain the right partition id: " + partitionId + ", but got "
              + partitionConfig.getPartitionId());
    }
    LOGGER.info("Storage partition adjustment got triggered by: {} with config: {}", mode, partitionConfig);
    AbstractStoragePartition partition = getPartitionOrThrow(partitionId);
    if (partition.verifyConfig(partitionConfig)) {
      LOGGER.info("Store partition adjustment will be skipped as there is no difference");
      return;
    }
    // Need to re-open storage partition according to the provided partition config
    LOGGER.info("Reopen database with storage partition config: {}", partitionConfig);
    ReadWriteLock readWriteLock = getRWLockForPartitionOrThrow(partitionId);
    readWriteLock.writeLock().lock();
    try {
      closePartition(partitionId);
      addStoragePartition(partitionConfig);
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  @Override
  public synchronized void addStoragePartitionIfAbsent(int partitionId) {
    if (containsPartition(partitionId)) {
      return;
    }
    addStoragePartition(new StoragePartitionConfig(storeVersionName, partitionId));
  }

  synchronized void addStoragePartition(StoragePartitionConfig storagePartitionConfig) {
    validateStoreName(storagePartitionConfig);
    int partitionId = storagePartitionConfig.getPartitionId();

    if (partitionId == METADATA_PARTITION_ID) {
      throw new StorageInitializationException("The metadata partition is not allowed to be set via this function!");
    }

    if (containsPartition(partitionId)) {
      LOGGER.error(
          "Failed to add a storage partition for partitionId: {} Store {}. This partition already exists!",
          partitionId,
          this.getStoreVersionName());
      throw new StorageInitializationException(
          "Partition " + partitionId + " of store " + this.getStoreVersionName() + " already exists.");
    }

    Partition partition = createStoragePartition(storagePartitionConfig);
    this.partitionList.set(partitionId, partition);
    if (this.rwLockForStoragePartitionAdjustmentList.get(partitionId) == null) {
      /**
       * It is intentional to keep the read-write lock even the partition gets moved to other places
       * since the same partition can be moved back or reopened.
       * Creating a new instance of read-write lock in this function will introduce some race condition
       * since some function could be waiting on a previous instance and some other function may start
       * using the new instance of the read-write lock for the same partition.
       */
      this.rwLockForStoragePartitionAdjustmentList.set(partitionId, new ReentrantReadWriteLock());
    }
  }

  @Override
  public synchronized void closePartition(int partitionId) {
    AbstractStoragePartition partition = this.partitionList.remove(partitionId);
    if (partition == null) {
      LOGGER.error("Failed to close a non existing partition: {} Store {}", partitionId, getStoreVersionName());
      return;
    }
    partition.close();
    if (getNumberOfPartitions() == 0) {
      LOGGER.info("All Partitions closed for store {} ", getStoreVersionName());
    }
  }

  @Override
  public synchronized void closeMetadataPartition() {
    if (metadataPartitionCreated()) {
      metadataPartition.close();
      metadataPartition = null;
      versionStateCache.set(null);
    }
  }

  /**
   * Removes and returns a partition from the current store
   *
   * @param partitionId - id of partition to retrieve and remove
   */
  @Override
  public synchronized void dropPartition(int partitionId) {
    dropPartition(partitionId, true);
  }

  /**
   * Removes and returns a partition from the current store
   *
   * @param partitionId - id of partition to retrieve and remove
   * @param dropMetadataPartitionWhenEmpty - if true, the whole store will be dropped if ALL partitions are removed
   */
  @Override
  public synchronized void dropPartition(int partitionId, boolean dropMetadataPartitionWhenEmpty) {
    /**
     * The caller of this method should ensure that:
     * 1. The SimpleKafkaConsumerTask associated with this partition is shutdown
     * 2. The partition node assignment repo is cleaned up and then remove this storage partition.
     *    Else there can be situations where the data is consumed from Kafka and not persisted.
     */
    if (!containsPartition(partitionId)) {
      LOGGER.error("Failed to remove a non existing partition: {} Store {}", partitionId, getStoreVersionName());
      return;
    }
    if (!suppressLogs) {
      LOGGER.info("Removing Partition: {} Store {}", partitionId, getStoreVersionName());
    }

    /**
     * Partition offset should be cleared by StorageEngine drops the corresponding partition. Here we may not be able to
     * guarantee the drop-partition order in bulk deletion, but if metadata partition get removed first, then it needs not
     * to clear partition offset.
     */
    if (metadataPartitionCreated() && partitionId != METADATA_PARTITION_ID) {
      clearPartitionOffset(partitionId);
    }

    AbstractStoragePartition partition = this.partitionList.remove(partitionId);
    partition.drop();

    if (getNumberOfPartitions() == 0 && dropMetadataPartitionWhenEmpty) {
      if (!suppressLogs) {
        LOGGER.info("All Partitions deleted for Store {}", getStoreVersionName());
      }
      /**
       * The reason to invoke {@link #drop} here is that storage engine might need to do some cleanup
       * in the store level.
       */
      drop();
    }
  }

  private synchronized void dropMetadataPartition() {
    if (metadataPartitionCreated()) {
      metadataPartition.drop();
      metadataPartition = null;
      versionStateCache.set(null);
    }
  }

  /**
   * Drop the whole store
   */
  @Override
  public synchronized void drop() {
    // check if its already dropped.
    if (getNumberOfPartitions() == 0 && !metadataPartitionCreated()) {
      return;
    }
    if (!suppressLogs) {
      LOGGER.info("Started dropping store: {}", getStoreVersionName());
    }

    // partitionList is implementation of SparseConcurrentList which sets element to null on `remove`. So its fine
    // to call size() while removing elements from the list.
    for (int partitionId = 0; partitionId < partitionList.size(); partitionId++) {
      if (!containsPartition(partitionId)) {
        continue;
      }
      dropPartition(partitionId);
    }
    dropMetadataPartition();
    if (!suppressLogs) {
      LOGGER.info("Finished dropping store: {}", getStoreVersionName());
    }
  }

  @Override
  public synchronized Map<String, String> sync(int partitionId) {
    AbstractStoragePartition partition = partitionList.get(partitionId);
    if (partition == null) {
      LOGGER.warn("Partition {} doesn't exist, no sync operation will be executed", partitionId);
      return Collections.emptyMap();
    }
    return partition.sync();
  }

  @Override
  public synchronized void close() throws VeniceException {
    long startTime = System.currentTimeMillis();
    List<Partition> tmpList = new ArrayList<>();
    // SparseConcurrentList does not support parallelStream, copy to a tmp list.
    partitionList.forEach(p -> tmpList.add(p));
    tmpList.parallelStream().forEach(Partition::close);
    LOGGER.info(
        "Closing {} rockDB partitions of store {} took {} ms",
        tmpList.size(),
        storeVersionName,
        LatencyUtils.getElapsedTimeFromMsToMs(startTime));
    partitionList.clear();
    closeMetadataPartition();
  }

  @Override
  public boolean isClosed() {
    return this.partitionList.isEmpty();
  }

  /**
   * checks whether the current state of the database is valid
   * during the start of ingestion.
   */
  @Override
  public boolean checkDatabaseIntegrity(
      int partitionId,
      Map<String, String> checkpointedInfo,
      StoragePartitionConfig storagePartitionConfig) {
    adjustStoragePartition(partitionId, CHECK_DATABASE_INTEGRITY, storagePartitionConfig);
    return getPartitionOrThrow(partitionId).checkDatabaseIntegrity(checkpointedInfo);
  }

  /**
   * A lot of storage engines support efficient methods for performing large
   * number of writes (puts/deletes) against the data source. This method puts
   * the storage engine in this batch write mode
   */
  @Override
  public synchronized void beginBatchWrite(
      StoragePartitionConfig storagePartitionConfig,
      Map<String, String> checkpointedInfo,
      Optional<Supplier<byte[]>> checksumSupplier) {
    LOGGER.info(
        "Begin batch write for storage partition config: {}  with checkpoint info: {}",
        storagePartitionConfig,
        checkpointedInfo);
    /**
     * We want to adjust the storage partition first since it will possibly re-open the underlying database in
     * different mode.
     */
    adjustStoragePartition(storagePartitionConfig.getPartitionId(), BEGIN_BATCH_PUSH, storagePartitionConfig);
    getPartitionOrThrow(storagePartitionConfig.getPartitionId()).beginBatchWrite(checkpointedInfo, checksumSupplier);
  }

  /**
   * @return true if the storage engine successfully returned to normal mode
   */
  @Override
  public synchronized void endBatchWrite(StoragePartitionConfig storagePartitionConfig) {
    LOGGER.info("End batch write for storage partition config: {}", storagePartitionConfig);
    AbstractStoragePartition partition = getPartitionOrThrow(storagePartitionConfig.getPartitionId());
    partition.endBatchWrite();
    /**
     * After end of batch push, we would like to adjust the underlying database for the future ingestion, such as from streaming.
     */
    adjustStoragePartition(storagePartitionConfig.getPartitionId(), END_BATCH_PUSH, storagePartitionConfig);

    if (!partition.validateBatchIngestion()) {
      throw new VeniceException("Storage temp files not fully ingested for store: " + storeVersionName);
    }
  }

  private void executeWithSafeGuard(int partitionId, Runnable runnable) {
    executeWithSafeGuard(partitionId, () -> {
      runnable.run();
      return null;
    });
  }

  private <T> T executeWithSafeGuard(int partitionId, Callable<T> callable) {
    ReadWriteLock readWriteLock = getRWLockForPartitionOrThrow(partitionId);
    readWriteLock.readLock().lock();
    try {
      return callable.call();
    } catch (Exception e) {
      if (e instanceof VeniceException) {
        throw (VeniceException) e;
      }
      throw new VeniceException(e);
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  /**
   * Reopen the underlying database.
   */
  @Override
  public void reopenStoragePartition(int partitionId) {
    executeWithSafeGuard(partitionId, () -> {
      if (!containsPartition(partitionId)) {
        LOGGER.warn("Partition {}_{} doesn't exist.", storeVersionName, partitionId);
        return;
      }
      AbstractStoragePartition storagePartition = getPartitionOrThrow(partitionId);
      storagePartition.reopen();
    });
  }

  @Override
  public void put(int partitionId, byte[] key, byte[] value) throws VeniceException {
    executeWithSafeGuard(partitionId, () -> {
      AbstractStoragePartition partition = getPartitionOrThrow(partitionId);
      partition.put(key, value);
    });
  }

  @Override
  public void put(int partitionId, byte[] key, ByteBuffer value) throws VeniceException {
    executeWithSafeGuard(partitionId, () -> {
      AbstractStoragePartition partition = getPartitionOrThrow(partitionId);
      partition.put(key, value);
    });
  }

  @Override
  public void putWithReplicationMetadata(int partitionId, byte[] key, ByteBuffer value, byte[] replicationMetadata)
      throws VeniceException {
    executeWithSafeGuard(partitionId, () -> {
      AbstractStoragePartition partition = getPartitionOrThrow(partitionId);
      partition.putWithReplicationMetadata(key, value, replicationMetadata);
    });
  }

  @Override
  public void putReplicationMetadata(int partitionId, byte[] key, byte[] replicationMetadata) throws VeniceException {
    executeWithSafeGuard(partitionId, () -> {
      AbstractStoragePartition partition = getPartitionOrThrow(partitionId);
      partition.putReplicationMetadata(key, replicationMetadata);
    });
  }

  @Override
  public byte[] get(int partitionId, byte[] key) throws VeniceException {
    return executeWithSafeGuard(partitionId, () -> {
      AbstractStoragePartition partition = getPartitionOrThrow(partitionId);
      return partition.get(key);
    });
  }

  @Override
  public ByteBuffer get(int partitionId, byte[] key, ByteBuffer valueToBePopulated) throws VeniceException {
    return executeWithSafeGuard(partitionId, () -> {
      AbstractStoragePartition partition = getPartitionOrThrow(partitionId);
      return partition.get(key, valueToBePopulated);
    });
  }

  @Override
  public byte[] get(int partitionId, ByteBuffer keyBuffer) throws VeniceException {
    return executeWithSafeGuard(partitionId, () -> {
      AbstractStoragePartition partition = getPartitionOrThrow(partitionId);
      return partition.get(keyBuffer);
    });
  }

  @Override
  public void getByKeyPrefix(int partitionId, byte[] partialKey, BytesStreamingCallback bytesStreamingCallback) {
    executeWithSafeGuard(partitionId, () -> {
      AbstractStoragePartition partition = getPartitionOrThrow(partitionId);
      partition.getByKeyPrefix(partialKey, bytesStreamingCallback);
    });
  }

  @Override
  public void delete(int partitionId, byte[] key) throws VeniceException {
    executeWithSafeGuard(partitionId, () -> {
      AbstractStoragePartition partition = getPartitionOrThrow(partitionId);
      partition.delete(key);
    });
  }

  @Override
  public void deleteWithReplicationMetadata(int partitionId, byte[] key, byte[] replicationMetadata)
      throws VeniceException {
    executeWithSafeGuard(partitionId, () -> {
      AbstractStoragePartition partition = getPartitionOrThrow(partitionId);
      partition.deleteWithReplicationMetadata(key, replicationMetadata);
    });
  }

  @Override
  public byte[] getReplicationMetadata(int partitionId, ByteBuffer key) {
    return executeWithSafeGuard(partitionId, () -> {
      AbstractStoragePartition partition = getPartitionOrThrow(partitionId);
      return partition.getReplicationMetadata(key);
    });
  }

  /**
   * Put the offset associated with the partitionId into the metadata partition.
   */
  @Override
  public synchronized void putPartitionOffset(int partitionId, OffsetRecord offsetRecord) {
    if (!metadataPartitionCreated()) {
      throw new StorageInitializationException("Metadata partition not created!");
    }
    if (partitionId == METADATA_PARTITION_ID) {
      throw new IllegalArgumentException("Metadata partition id should not be used as argument in putPartitionOffset.");
    }
    if (partitionId < 0) {
      throw new IllegalArgumentException("Invalid partition id argument in putPartitionOffset");
    }
    metadataPartition.put(getPartitionMetadataKey(partitionId), offsetRecord.toBytes());
  }

  /**
   * Retrieve the offset associated with the partitionId from the metadata partition.
   */
  @Override
  public synchronized Optional<OffsetRecord> getPartitionOffset(int partitionId) {
    if (!metadataPartitionCreated()) {
      throw new StorageInitializationException("Metadata partition not created!");
    }
    if (partitionId == METADATA_PARTITION_ID) {
      throw new IllegalArgumentException("Metadata partition id should not be used as argument in getPartitionOffset.");
    }
    if (partitionId < 0) {
      throw new IllegalArgumentException("Invalid partition id argument in getPartitionOffset");
    }
    byte[] value = metadataPartition.get(getPartitionMetadataKey(partitionId));
    if (value == null) {
      return Optional.empty();
    }
    return Optional.of(new OffsetRecord(value, partitionStateSerializer));
  }

  /**
   * Clear the offset associated with the partitionId in the metadata partition.
   */
  @Override
  public synchronized void clearPartitionOffset(int partitionId) {
    if (!metadataPartitionCreated()) {
      LOGGER.info(
          "Metadata partition not created; there is nothing to clear for {} partition {}",
          storeVersionName,
          partitionId);
      return;
    }
    if (partitionId == METADATA_PARTITION_ID) {
      throw new IllegalArgumentException(
          "Metadata partition id should not be used as argument in clearPartitionOffset.");
    }
    if (partitionId < 0) {
      throw new IllegalArgumentException("Invalid partition id argument in clearPartitionOffset");
    }
    metadataPartition.delete(getPartitionMetadataKey(partitionId));
  }

  /**
   * Put the store version state into the metadata partition.
   */
  @Override
  public synchronized void putStoreVersionState(StoreVersionState versionState) {
    if (!metadataPartitionCreated()) {
      throw new StorageInitializationException("Metadata partition not created!");
    }
    versionStateCache.set(versionState);
    metadataPartition
        .put(VERSION_METADATA_KEY, storeVersionStateSerializer.serialize(getStoreVersionName(), versionState));
  }

  /**
   * Used in ingestion isolation mode update the storage engine's cache in sync with the updates to the state in
   * {@link com.linkedin.davinci.ingestion.main.MainIngestionStorageMetadataService}
   */
  @Override
  public void updateStoreVersionStateCache(StoreVersionState versionState) {
    versionStateCache.set(versionState);
  }

  /**
   * Retrieve the store version state from the metadata partition.
   */
  @Override
  public StoreVersionState getStoreVersionState() {
    while (true) {
      StoreVersionState versionState = versionStateCache.get();
      if (versionState != null) {
        return versionState;
      }
      if (metadataPartition == null) {
        return null;
      }
      byte[] value = metadataPartition.get(VERSION_METADATA_KEY);
      if (value == null) {
        return null;
      }
      versionState = storeVersionStateSerializer.deserialize(storeVersionName, value);
      if (versionStateCache.compareAndSet(null, versionState)) {
        return versionState;
      } // else retry the loop...
    }
  }

  /**
   * Clear the store version state in the metadata partition.
   */
  @Override
  public synchronized void clearStoreVersionState() {
    versionStateCache.set(null);
    metadataPartition.delete(VERSION_METADATA_KEY);
  }

  /**
   * Return true or false based on whether a given partition exists within this storage engine
   *
   * @param partitionId The partition to look for
   * @return True/False, does the partition exist on this node
   */
  @Override
  public synchronized boolean containsPartition(int partitionId) {
    return this.partitionList.get(partitionId) != null;
  }

  /**
   * A function which behaves like {@link Map#size()}, in the sense that it ignores empty
   * (null) slots in the list.
   *
   * @return the number of non-null partitions in {@link #partitionList}
   */
  public synchronized long getNumberOfPartitions() {
    return this.partitionList.nonNullSize();
  }

  /**
   * Get all Partition Ids which are assigned to the current Node.
   *
   * @return partition Ids that are hosted in the current Storage Engine.
   */
  @Override
  public synchronized Set<Integer> getPartitionIds() {
    return this.partitionList.values().stream().map(Partition::getPartitionId).collect(Collectors.toSet());
  }

  protected Collection<Partition> getPartitions() {
    return this.partitionList.values();
  }

  @Override
  public Partition getPartitionOrThrow(int partitionId) {
    Partition partition;
    ReadWriteLock readWriteLock = getRWLockForPartitionOrThrow(partitionId);
    readWriteLock.readLock().lock();
    try {
      partition = partitionList.get(partitionId);
    } finally {
      readWriteLock.readLock().unlock();
    }
    if (partition == null) {
      VeniceException e = new PersistenceFailureException(
          "Partition: " + partitionId + " of store: " + getStoreVersionName() + " does not exist");
      LOGGER.error("Failed to get the partition with msg: {}", e.getMessage());
      throw e;
    }
    return partition;
  }

  private static byte[] getPartitionMetadataKey(int partitionId) {
    return (PARTITION_METADATA_PREFIX + partitionId).getBytes();
  }

  private boolean metadataPartitionCreated() {
    return metadataPartition != null;
  }

  private void validateStoreName(StoragePartitionConfig storagePartitionConfig) {
    if (!storagePartitionConfig.getStoreName().equals(getStoreVersionName())) {
      throw new VeniceException(
          "Store name in partition config: " + storagePartitionConfig.getStoreName()
              + " doesn't match current store engine: " + getStoreVersionName());
    }
  }

  public void suppressLogs(boolean suppressLogs) {
    this.suppressLogs = suppressLogs;
  }

  @Override
  public AbstractStorageIterator getIterator(int partitionId) {
    throw new UnsupportedOperationException("Method not supported for storage engine");
  }
}
