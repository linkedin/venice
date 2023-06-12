package com.linkedin.davinci.store;

import com.linkedin.davinci.callback.BytesStreamingCallback;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.exceptions.PersistenceFailureException;
import com.linkedin.venice.exceptions.StorageInitializationException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.SparseConcurrentList;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
public abstract class AbstractStorageEngine<Partition extends AbstractStoragePartition> implements Closeable {
  private static final Logger LOGGER = LogManager.getLogger(AbstractStorageEngine.class);

  private static final byte[] VERSION_METADATA_KEY = "VERSION_METADATA".getBytes();
  private static final String PARTITION_METADATA_PREFIX = "P_";

  // Using a large positive number for metadata partition id instead of -1 can avoid database naming issues.
  public static final int METADATA_PARTITION_ID = 1000_000_000;

  private final String storeName;
  private final List<Partition> partitionList = new SparseConcurrentList<>();
  private Partition metadataPartition;
  private final AtomicReference<StoreVersionState> versionStateCache = new AtomicReference<>();
  private final InternalAvroSpecificSerializer<StoreVersionState> storeVersionStateSerializer;
  private final InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer;

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
      String storeName,
      InternalAvroSpecificSerializer<StoreVersionState> storeVersionStateSerializer,
      InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer) {
    this.storeName = storeName;
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
          "Failed to get read-write lock for partition: " + partitionId + ", store: " + getStoreName());
    }
    return readWriteLock;
  }

  public String getStoreName() {
    return storeName;
  }

  @Override
  public String toString() {
    return getStoreName();
  }

  public abstract PersistenceType getType();

  public abstract long getStoreSizeInBytes();

  public long getCachedStoreSizeInBytes() {
    return 0;
  }

  public long getRMDSizeInBytes() {
    return 0;
  }

  public long getCachedRMDSizeInBytes() {
    return 0;
  }

  protected abstract Set<Integer> getPersistedPartitionIds();

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
      this.metadataPartition = createStoragePartition(new StoragePartitionConfig(storeName, METADATA_PARTITION_ID));
    }

    if (restoreDataPartitions) {
      LOGGER.info("Data partitions restore enabled. Restoring data partitions.");
      partitionIds.stream()
          .sorted((o1, o2) -> Integer.compare(o2, o1)) // reverse order, to minimize array resizing in {@link
                                                       // SparseConcurrentList}
          .forEach(this::addStoragePartition);
    }
  }

  protected final synchronized void restoreStoragePartitions() {
    restoreStoragePartitions(true, true);
  }

  // For testing purpose only.
  public AbstractStoragePartition getMetadataPartition() {
    return metadataPartition;
  }

  public synchronized void preparePartitionForReading(int partitionId) {
    if (!containsPartition(partitionId)) {
      LOGGER.warn("Partition {}_{} was removed before reopening.", storeName, partitionId);
      return;
    }
    StoragePartitionConfig partitionConfig = new StoragePartitionConfig(storeName, partitionId);
    partitionConfig.setWriteOnlyConfig(false);

    adjustStoragePartition(partitionConfig);
  }

  /**
   * Adjust the opened storage partition according to the provided storagePartitionConfig.
   * It will throw exception if there is no opened storage partition for the given partition id.
   */
  public synchronized void adjustStoragePartition(StoragePartitionConfig partitionConfig) {
    validateStoreName(partitionConfig);
    int partitionId = partitionConfig.getPartitionId();
    AbstractStoragePartition partition = getPartitionOrThrow(partitionId);
    if (partition.verifyConfig(partitionConfig)) {
      LOGGER.info("No adjustment needed for store name: {}, partition id: {}", getStoreName(), partitionId);
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

  public void addStoragePartition(int partitionId) {
    addStoragePartition(new StoragePartitionConfig(storeName, partitionId));
  }

  public synchronized void addStoragePartition(StoragePartitionConfig storagePartitionConfig) {
    validateStoreName(storagePartitionConfig);
    int partitionId = storagePartitionConfig.getPartitionId();

    if (partitionId == METADATA_PARTITION_ID) {
      throw new StorageInitializationException("The metadata partition is not allowed to be set via this function!");
    }

    if (containsPartition(partitionId)) {
      LOGGER.error(
          "Failed to add a storage partition for partitionId: {} Store {}. This partition already exists!",
          partitionId,
          this.getStoreName());
      throw new StorageInitializationException(
          "Partition " + partitionId + " of store " + this.getStoreName() + " already exists.");
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

  public synchronized void closePartition(int partitionId) {
    AbstractStoragePartition partition = this.partitionList.remove(partitionId);
    if (partition == null) {
      LOGGER.error("Failed to close a non existing partition: {} Store {}", partitionId, getStoreName());
      return;
    }
    partition.close();
    if (getNumberOfPartitions() == 0) {
      LOGGER.info("All Partitions closed for store {} ", getStoreName());
    }
  }

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
  public synchronized void dropPartition(int partitionId) {
    /**
     * The caller of this method should ensure that:
     * 1. The SimpleKafkaConsumerTask associated with this partition is shutdown
     * 2. The partition node assignment repo is cleaned up and then remove this storage partition.
     *    Else there can be situations where the data is consumed from Kafka and not persisted.
     */
    if (!containsPartition(partitionId)) {
      LOGGER.error("Failed to remove a non existing partition: {} Store {}", partitionId, getStoreName());
      return;
    }
    LOGGER.info("Removing Partition: {} Store {}", partitionId, getStoreName());

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

    if (getNumberOfPartitions() == 0) {
      LOGGER.info("All Partitions deleted for Store {}", getStoreName());
      /**
       * The reason to invoke {@link #drop} here is that storage engine might need to do some cleanup
       * in the store level.
       */
      drop();
    }
  }

  public synchronized void dropMetadataPartition() {
    if (metadataPartitionCreated()) {
      metadataPartition.drop();
      metadataPartition = null;
      versionStateCache.set(null);
    }
  }

  /**
   * Drop the whole store
   */
  public synchronized void drop() {
    // check if its already dropped.
    if (getNumberOfPartitions() == 0 && !metadataPartitionCreated()) {
      return;
    }

    LOGGER.info("Started dropping store: {}", getStoreName());
    // partitionList is implementation of SparseConcurrentList which sets element to null on `remove`. So its fine
    // to call size() while removing elements from the list.
    for (int partitionId = 0; partitionId < partitionList.size(); partitionId++) {
      if (!containsPartition(partitionId)) {
        continue;
      }
      dropPartition(partitionId);
    }
    dropMetadataPartition();
    LOGGER.info("Finished dropping store: {}", getStoreName());
  }

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
        partitionList.size(),
        storeName,
        LatencyUtils.getElapsedTimeInMs(startTime));
    partitionList.clear();
    closeMetadataPartition();
  }

  /**
   * checks whether the current state of the database is valid
   * during the start of ingestion.
   */
  public boolean checkDatabaseIntegrity(
      int partitionId,
      Map<String, String> checkpointedInfo,
      StoragePartitionConfig storagePartitionConfig) {
    adjustStoragePartition(storagePartitionConfig);
    return getPartitionOrThrow(partitionId).checkDatabaseIntegrity(checkpointedInfo);
  }

  /**
   * A lot of storage engines support efficient methods for performing large
   * number of writes (puts/deletes) against the data source. This method puts
   * the storage engine in this batch write mode
   */
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
    adjustStoragePartition(storagePartitionConfig);
    getPartitionOrThrow(storagePartitionConfig.getPartitionId()).beginBatchWrite(checkpointedInfo, checksumSupplier);
  }

  /**
   * @return true if the storage engine successfully returned to normal mode
   */
  public synchronized void endBatchWrite(StoragePartitionConfig storagePartitionConfig) {
    LOGGER.info("End batch write for storage partition config: {}", storagePartitionConfig);
    AbstractStoragePartition partition = getPartitionOrThrow(storagePartitionConfig.getPartitionId());
    partition.endBatchWrite();
    /**
     * After end of batch push, we would like to adjust the underlying database for the future ingestion, such as from streaming.
     */
    adjustStoragePartition(storagePartitionConfig);

    if (!partition.validateBatchIngestion()) {
      throw new VeniceException("Storage temp files not fully ingested for store: " + storeName);
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
  public void reopenStoragePartition(int partitionId) {
    executeWithSafeGuard(partitionId, () -> {
      if (!containsPartition(partitionId)) {
        LOGGER.warn("Partition {}_{} doesn't exist.", storeName, partitionId);
        return;
      }
      AbstractStoragePartition storagePartition = getPartitionOrThrow(partitionId);
      storagePartition.reopen();
    });
  }

  public void put(int partitionId, byte[] key, byte[] value) throws VeniceException {
    executeWithSafeGuard(partitionId, () -> {
      AbstractStoragePartition partition = getPartitionOrThrow(partitionId);
      partition.put(key, value);
    });
  }

  public void put(int partitionId, byte[] key, ByteBuffer value) throws VeniceException {
    executeWithSafeGuard(partitionId, () -> {
      AbstractStoragePartition partition = getPartitionOrThrow(partitionId);
      partition.put(key, value);
    });
  }

  public void putWithReplicationMetadata(int partitionId, byte[] key, ByteBuffer value, byte[] replicationMetadata)
      throws VeniceException {
    executeWithSafeGuard(partitionId, () -> {
      AbstractStoragePartition partition = getPartitionOrThrow(partitionId);
      partition.putWithReplicationMetadata(key, value, replicationMetadata);
    });
  }

  public void putReplicationMetadata(int partitionId, byte[] key, byte[] replicationMetadata) throws VeniceException {
    executeWithSafeGuard(partitionId, () -> {
      AbstractStoragePartition partition = getPartitionOrThrow(partitionId);
      partition.putReplicationMetadata(key, replicationMetadata);
    });
  }

  public <K, V> void put(int partitionId, K key, V value) {
    executeWithSafeGuard(partitionId, () -> {
      AbstractStoragePartition partition = getPartitionOrThrow(partitionId);
      partition.put(key, value);
    });
  }

  public byte[] get(int partitionId, byte[] key) throws VeniceException {
    return executeWithSafeGuard(partitionId, () -> {
      AbstractStoragePartition partition = getPartitionOrThrow(partitionId);
      return partition.get(key);
    });
  }

  public ByteBuffer get(int partitionId, byte[] key, ByteBuffer valueToBePopulated) throws VeniceException {
    return executeWithSafeGuard(partitionId, () -> {
      AbstractStoragePartition partition = getPartitionOrThrow(partitionId);
      return partition.get(key, valueToBePopulated);
    });
  }

  public byte[] get(int partitionId, ByteBuffer keyBuffer) throws VeniceException {
    return executeWithSafeGuard(partitionId, () -> {
      AbstractStoragePartition partition = getPartitionOrThrow(partitionId);
      return partition.get(keyBuffer);
    });
  }

  public void getByKeyPrefix(int partitionId, byte[] partialKey, BytesStreamingCallback bytesStreamingCallback) {
    executeWithSafeGuard(partitionId, () -> {
      AbstractStoragePartition partition = getPartitionOrThrow(partitionId);
      partition.getByKeyPrefix(partialKey, bytesStreamingCallback);
    });
  }

  public void delete(int partitionId, byte[] key) throws VeniceException {
    executeWithSafeGuard(partitionId, () -> {
      AbstractStoragePartition partition = getPartitionOrThrow(partitionId);
      partition.delete(key);
    });
  }

  public void deleteWithReplicationMetadata(int partitionId, byte[] key, byte[] replicationMetadata)
      throws VeniceException {
    executeWithSafeGuard(partitionId, () -> {
      AbstractStoragePartition partition = getPartitionOrThrow(partitionId);
      partition.deleteWithReplicationMetadata(key, replicationMetadata);
    });
  }

  public byte[] getReplicationMetadata(int partitionId, byte[] key) {
    return executeWithSafeGuard(partitionId, () -> {
      AbstractStoragePartition partition = getPartitionOrThrow(partitionId);
      return partition.getReplicationMetadata(key);
    });
  }

  /**
   * Put the offset associated with the partitionId into the metadata partition.
   */
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
  public synchronized void clearPartitionOffset(int partitionId) {
    if (!metadataPartitionCreated()) {
      throw new StorageInitializationException("Metadata partition not created!");
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
  public synchronized void putStoreVersionState(StoreVersionState versionState) {
    if (!metadataPartitionCreated()) {
      throw new StorageInitializationException("Metadata partition not created!");
    }
    versionStateCache.set(versionState);
    metadataPartition.put(VERSION_METADATA_KEY, storeVersionStateSerializer.serialize(getStoreName(), versionState));
  }

  /**
   * Used in ingestion isolation mode update the storage engine's cache in sync with the updates to the state in
   * {@link com.linkedin.davinci.ingestion.main.MainIngestionStorageMetadataService}
   */
  public void updateStoreVersionStateCache(StoreVersionState versionState) {
    versionStateCache.set(versionState);
  }

  /**
   * Retrieve the store version state from the metadata partition.
   */
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
      versionState = storeVersionStateSerializer.deserialize(storeName, value);
      if (versionStateCache.compareAndSet(null, versionState)) {
        return versionState;
      } // else retry the loop...
    }
  }

  /**
   * Clear the store version state in the metadata partition.
   */
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
  public synchronized boolean containsPartition(int partitionId) {
    return this.partitionList.get(partitionId) != null;
  }

  public synchronized boolean containsPartition(int userPartition, PartitionerConfig partitionerConfig) {
    int amplificationFactor = partitionerConfig == null ? 1 : partitionerConfig.getAmplificationFactor();
    for (int subPartition: PartitionUtils.getSubPartitions(userPartition, amplificationFactor)) {
      if (!containsPartition(subPartition)) {
        return false;
      }
    }
    return true;
  }

  /**
   * A function which behaves like {@link Map#size()}, in the sense that it ignores empty
   * (null) slots in the list.
   *
   * @return the number of non-null partitions in {@link #partitionList}
   */
  public synchronized long getNumberOfPartitions() {
    return this.partitionList.stream().filter(Objects::nonNull).count();
  }

  /**
   * Get all Partition Ids which are assigned to the current Node.
   *
   * @return partition Ids that are hosted in the current Storage Engine.
   */
  public synchronized Set<Integer> getPartitionIds() {
    return this.partitionList.stream()
        .filter(Objects::nonNull)
        .map(Partition::getPartitionId)
        .collect(Collectors.toSet());
  }

  public AbstractStoragePartition getPartitionOrThrow(int partitionId) {
    AbstractStoragePartition partition;
    ReadWriteLock readWriteLock = getRWLockForPartitionOrThrow(partitionId);
    readWriteLock.readLock().lock();
    try {
      partition = partitionList.get(partitionId);
    } finally {
      readWriteLock.readLock().unlock();
    }
    if (partition == null) {
      VeniceException e = new PersistenceFailureException(
          "Partition: " + partitionId + " of store: " + getStoreName() + " does not exist");
      LOGGER.error("Msg: {} Cause: {}", e.getMessage(), e.getCause());
      throw e;
    }
    return partition;
  }

  public synchronized long getPartitionSizeInBytes(int partitionId) {
    AbstractStoragePartition partition = partitionList.get(partitionId);
    return partition != null ? partition.getPartitionSizeInBytes() : 0;
  }

  private static byte[] getPartitionMetadataKey(int partitionId) {
    return (PARTITION_METADATA_PREFIX + partitionId).getBytes();
  }

  private boolean metadataPartitionCreated() {
    return metadataPartition != null;
  }

  private void validateStoreName(StoragePartitionConfig storagePartitionConfig) {
    if (!storagePartitionConfig.getStoreName().equals(getStoreName())) {
      throw new VeniceException(
          "Store name in partition config: " + storagePartitionConfig.getStoreName()
              + " doesn't match current store engine: " + getStoreName());
    }
  }

  public CompressionStrategy getCompressionStrategy() {
    StoreVersionState svs = getStoreVersionState();
    return svs == null ? CompressionStrategy.NO_OP : CompressionStrategy.valueOf(svs.compressionStrategy);
  }

  public boolean isChunked() {
    StoreVersionState svs = getStoreVersionState();
    return svs == null ? false : svs.chunked;
  }

  public boolean hasMemorySpaceLeft() {
    return true;
  }
}
