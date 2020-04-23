package com.linkedin.venice.store;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.exceptions.PersistenceFailureException;
import com.linkedin.venice.exceptions.StorageInitializationException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.storage.BdbStorageMetadataService;
import com.linkedin.venice.utils.Utils;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.log4j.Logger;


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
public abstract class AbstractStorageEngine<P extends AbstractStoragePartition> implements Store {
  private static final String VERSION_METADATA_KEY = "VERSION_METADATA";
  private static final String METADATA_MIGRATION_KEY = "METADATA_MIGRATION";
  private static final String METADATA_MIGRATION_VALUE = "DUMMY_VALUE";
  private static final String PARTITION_METADATA_PREFIX = "P_";
  private final String storeName;
  // Storing StoreVersionState in metadata partition requires specific serializer.
  private final InternalAvroSpecificSerializer<StoreVersionState> storeVersionStateSerializer;
  private final AtomicReference<StoreVersionState> storeVersionStateCache = new AtomicReference<>();

  protected final AtomicBoolean isOpen;
  protected final Logger logger = Logger.getLogger(AbstractStorageEngine.class);
  protected ConcurrentMap<Integer, P> partitionIdToPartitionMap;

  // Using a large positive number for metadata partition id instead of -1 can avoid database naming issues.
  public static final int METADATA_PARTITION_ID = 1000_000_000;


  public AbstractStorageEngine(String storeName) {
    this.storeName = storeName;
    this.isOpen = new AtomicBoolean(true);
    this.partitionIdToPartitionMap = new ConcurrentHashMap<>();
    this.storeVersionStateSerializer = AvroProtocolDefinition.STORE_VERSION_STATE.getSerializer();
  }

  public abstract PersistenceType getType();

  /**
   * Load the existing storage partitions.
   * The implementation should decide when to call this function properly to restore partitions.
   */
  protected synchronized void restoreStoragePartitions() {
    Set<Integer> partitionIds = getPersistedPartitionIds();
    partitionIds.forEach(this::addStoragePartition);
    // Create a new metadata partition in storage engine if no persisted metadata partition found.
    partitionIdToPartitionMap.computeIfAbsent(METADATA_PARTITION_ID, k -> createStoragePartition(new StoragePartitionConfig(storeName, METADATA_PARTITION_ID)));
  }

  /**
   * Following method checks if a store was previously migrated to use RocksDB but still trying to start the server with config isRocksDBOffsetMetadataEnabled()
   * set to false. For rolling back all the data needs to be wiped clean
   */
  public void validateMigrationStatus() {
    if (null != partitionIdToPartitionMap.get(METADATA_PARTITION_ID).get(METADATA_MIGRATION_KEY.getBytes())) {
      throw new VeniceException(String.format("Store %s has previously migrated to RocksDB metadata offset store, but now its set (server.enable.rocksdb.offset.metadata = false) to use BDB. Please delete all data including offset for this store", storeName));
    }
  }

  /**
   * bootstrap and validate metadata offset records from BDB to rocksDB partition.
   * it writes a dummy value to key METADATA_MIGRATION_KEY to indicate that this has been migrated to rocksDB
   * @param bdbStorageMetadataService
   */
  public synchronized void bootStrapAndValidateOffsetRecordsFromBDB(BdbStorageMetadataService bdbStorageMetadataService) {
    // Check if this store has already been migrated to use rocksDB offset store
    if (null != partitionIdToPartitionMap.get(METADATA_PARTITION_ID).get(METADATA_MIGRATION_KEY.getBytes())) {
      return;
    }

    bootStrapOffsetRecordsFromBDB(bdbStorageMetadataService);
    validateBootstrap(bdbStorageMetadataService);

    // Set the migration key indicating that the migration is done for this store.
    partitionIdToPartitionMap.get(METADATA_PARTITION_ID).put(
        METADATA_MIGRATION_KEY.getBytes(),
        METADATA_MIGRATION_VALUE.getBytes()
    );
  }

  /**
   * Loads offset from BDB into RocksDB metadata partition
   * @param bdbStorageMetadataService
   */
  private void bootStrapOffsetRecordsFromBDB(BdbStorageMetadataService bdbStorageMetadataService) {
    if (!bdbStorageMetadataService.isStarted()) {
      throw new VeniceException("BDBMetadataService not started");
    }

    for (Integer partitionId : partitionIdToPartitionMap.keySet()) {
      if (partitionId == METADATA_PARTITION_ID) {
        continue;
      }
      OffsetRecord offsetRecord = bdbStorageMetadataService.getLastOffset(storeName, partitionId);
      if (offsetRecord.getOffset() != OffsetRecord.LOWEST_OFFSET) {
        putPartitionOffset(partitionId, offsetRecord);
      }
    }
    Optional<StoreVersionState> storeVersionState = bdbStorageMetadataService.getStoreVersionState(storeName);
    if (storeVersionState.isPresent()) {
      putStoreVersionState(storeVersionState.get());
    }
  }

  /**
   * Validate bootstrapped records from BDB and if they match clean up the BDB records
   * else throw exception.
   * @param bdbStorageMetadataService
   */
  private void validateBootstrap(BdbStorageMetadataService bdbStorageMetadataService) {
    Set<Integer> bdbPartitions = new HashSet<>();
    for (Integer partitionId : partitionIdToPartitionMap.keySet()) {
      if (partitionId == METADATA_PARTITION_ID) {
        continue;
      }
      OffsetRecord bdbOffsetRecord = bdbStorageMetadataService.getLastOffset(storeName, partitionId);
      Optional<OffsetRecord> rocksDBOffsetRecord = getPartitionOffset(partitionId);

      if (bdbOffsetRecord.getOffset() != OffsetRecord.LOWEST_OFFSET) {
        if (!rocksDBOffsetRecord.isPresent()) {
          throw new VeniceException("RocksDB offset record missing but exits in BDB :" + bdbOffsetRecord);
        }
        if (!bdbOffsetRecord.equals(rocksDBOffsetRecord.get())) {
          throw new VeniceException("BDB and RocksDB offset record mismatch, BDB record  :" + bdbOffsetRecord + " rocksdb record " + rocksDBOffsetRecord);
        }
        bdbPartitions.add(partitionId);
      }
    }

    Optional<StoreVersionState> BDBStoreVersionState = bdbStorageMetadataService.getStoreVersionState(storeName);
    Optional<StoreVersionState> rocksDBStoreVersionState = getStoreVersionState();

    if (BDBStoreVersionState.isPresent() ) {
      if (!rocksDBStoreVersionState.isPresent()) {
        throw new VeniceException("RocksDB StoreVersionState record missing, but exist in BDB  :" + BDBStoreVersionState);
      }
      if (!BDBStoreVersionState.get().equals(rocksDBStoreVersionState.get())) {
        throw new VeniceException("BDB and RocksDB StoreVersionState record mismatch, BDB record :" + BDBStoreVersionState + " rocksdb record " + rocksDBStoreVersionState);
      }
    }

    //all validations passed. clear the BDB offset records
    for (Integer partitionId : bdbPartitions) {
      bdbStorageMetadataService.clearOffset(storeName, partitionId);
    }

    if (BDBStoreVersionState.isPresent()) {
      bdbStorageMetadataService.clearStoreVersionState(storeName);
    }
  }

  /**
   * List the existing partition ids for current storage engine persisted previously
   * @return
   */
  protected abstract Set<Integer> getPersistedPartitionIds();

  public abstract P createStoragePartition(StoragePartitionConfig storagePartitionConfig);

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
    P partition = createStoragePartition(storagePartitionConfig);
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
    logger.info("Removing Partition: " + partitionId + " Store " + this.getName());

    /**
     * Partition offset should be cleared by StorageEngine drops the corresponding partition. Here we may not be able to
     * guarantee the drop-partition order in bulk deletion, but if metadata partition get removed first, then it needs not
     * to clear partition offset.
     */
    if (containsPartition(METADATA_PARTITION_ID) && (partitionId != METADATA_PARTITION_ID)) {
      clearPartitionOffset(partitionId);
    }

    AbstractStoragePartition partition = partitionIdToPartitionMap.remove(partitionId);
    partition.drop();

    // Make sure we remove the metadata partition before we cleanup the whole storage engine.
    if ((partitionIdToPartitionMap.size() == 1) && (containsPartition(METADATA_PARTITION_ID))) {
      // Clear the version metadata state that stores both inside cache and metadata partition of the AbstractStorageEngine.
      clearStoreVersionState();
      // Make sure we drop the metadata partition.
      AbstractStoragePartition metadataPartition = partitionIdToPartitionMap.remove(METADATA_PARTITION_ID);
      metadataPartition.drop();
      logger.info("Metadata partition for Store " + storeName + " are removed.");
    }

    if (partitionIdToPartitionMap.size() == 0) {
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
    // Filter out metadata partition id as it is an internal concept for AbstractStorageEngine.
    return partitionIdToPartitionMap.keySet().stream().filter(s -> s != METADATA_PARTITION_ID).collect(Collectors.toSet());
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

  protected void validatePartitionForKey(Integer logicalPartitionId, Object key, String operationType) {
    Utils.notNull(key, "Key cannot be null.");
    if (!containsPartition(logicalPartitionId)) {
      String errorMessage = operationType + " request failed. Invalid partition id: "
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

  /**
   * Put the offset associated with the partitionId into the metadata partition.
   */
  public void putPartitionOffset(int partitionId, OffsetRecord offsetRecord) {
    if (!partitionIdToPartitionMap.containsKey(partitionId)) {
      throw new IllegalArgumentException("partitionId " + partitionId + " does not exist in partitionIdToPartitionMap.");
    }
    partitionIdToPartitionMap.get(METADATA_PARTITION_ID)
        .put(
            composeMetadataPartitionKey(PARTITION_METADATA_PREFIX, partitionId).getBytes(),
            offsetRecord.toBytes()
        );
  }

  /**
   * Retrieve the offset associated with the partitionId from the metadata partition.
   */
  public Optional<OffsetRecord> getPartitionOffset(int partitionId) {
    if (!partitionIdToPartitionMap.containsKey(partitionId)) {
      throw new IllegalArgumentException("partitionId " + partitionId + " does not exist in partitionIdToPartitionMap.");
    }
    byte[] key = composeMetadataPartitionKey(PARTITION_METADATA_PREFIX, partitionId).getBytes();
    byte[] value = partitionIdToPartitionMap.get(METADATA_PARTITION_ID)
        .get(key);
    if (null == value) {
      return Optional.empty();
    }
    return Optional.of(new OffsetRecord(value));
  }

  /**
   * Clear the offset associated with the partitionId in the metadata partition.
   */
  public void clearPartitionOffset(int partitionId) {
    if (!partitionIdToPartitionMap.containsKey(partitionId)) {
      throw new IllegalArgumentException("partitionId " + partitionId + " does not exist in partitionIdToPartitionMap.");
    }
    if (partitionId == METADATA_PARTITION_ID) {
      throw new IllegalArgumentException("Metadata partition id should not be used as argument in clearPartitionOffset.");
    }
    if (!partitionIdToPartitionMap.containsKey(METADATA_PARTITION_ID)) {
      throw new VeniceException("Metadata partition does not exist in partitionIdToPartitionMap.");
    }
    partitionIdToPartitionMap.get(METADATA_PARTITION_ID)
        .delete(composeMetadataPartitionKey(PARTITION_METADATA_PREFIX, partitionId).getBytes());
  }

  /**
   * Put the store version state into the metadata partition.
   */
  public void putStoreVersionState(StoreVersionState record) {
    partitionIdToPartitionMap.get(METADATA_PARTITION_ID)
        .put(
            VERSION_METADATA_KEY.getBytes(),
            storeVersionStateSerializer.serialize(getName(), record)
        );
  }

  /**
   * Retrieve the store version state from the metadata partition.
   */
  public Optional<StoreVersionState> getStoreVersionState() {
    // If store version state cache is set, return the cache value directly.
    StoreVersionState storeVersionState = storeVersionStateCache.get();
    if (storeVersionState != null) {
      return Optional.of(storeVersionState);
    }
    byte[] value = partitionIdToPartitionMap.get(METADATA_PARTITION_ID)
        .get(VERSION_METADATA_KEY.getBytes());
    if (null == value) {
      return Optional.empty();
    }
    storeVersionState = storeVersionStateSerializer.deserialize(storeName, value);
    // Update store version state cache.
    storeVersionStateCache.set(storeVersionState);
    return Optional.of(storeVersionState);
  }

  /**
   * Clear the store version state in the metadata partition.
   */
  public void clearStoreVersionState() {
    // Delete the cached store version state.
    storeVersionStateCache.set(null);
    partitionIdToPartitionMap.get(METADATA_PARTITION_ID)
        .delete(VERSION_METADATA_KEY.getBytes());
  }

  /**
   * Retrieve the store version's CompressionStrategy from cached StoreVersionState.
   */
  public CompressionStrategy getStoreVersionCompressionStrategy() {
    return getStoreVersionState()
        .map(storeVersionState -> CompressionStrategy.valueOf(storeVersionState.compressionStrategy))
        .orElse(CompressionStrategy.NO_OP);
  }

  /**
   * Retrieve the store version's chunked flag from cached StoreVersionState.
   */
  public boolean isStoreVersionChunked() {
    return getStoreVersionState().map(storeVersionState -> storeVersionState.chunked).orElse(false);
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

  @Override
  public byte[] get(Integer logicalPartitionId, ByteBuffer keyBuffer) throws VeniceException {
    validatePartitionForKey(logicalPartitionId, keyBuffer, "Get");
    AbstractStoragePartition partition = this.partitionIdToPartitionMap.get(logicalPartitionId);
    return partition.get(keyBuffer);
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

  public abstract long getStoreSizeInBytes();

  public long getPartitionSizeInBytes(int partitionId) {
    AbstractStoragePartition partition = this.partitionIdToPartitionMap.get(partitionId);
    return partition.getPartitionSizeInBytes();
  }

  public String composeMetadataPartitionKey(String metadataPrefix, int partitionId) {
    return metadataPrefix + partitionId;
  }
}
