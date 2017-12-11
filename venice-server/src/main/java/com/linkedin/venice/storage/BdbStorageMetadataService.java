package com.linkedin.venice.storage;

import com.linkedin.venice.config.VeniceClusterConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.Time;
import com.sleepycat.je.CacheMode;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * This is a single metadata database for all stores hosted on this node.
 *
 * It contains only data which is relevant to the local storage node, and which does not need to be replicated
 * outside of storage nodes. It is not currently query-able from external processes, though that could be made
 * available if there is a need for it.
 *
 * The number of records in the database should be the sum of:
 * 1. The total number of partitions served by this node.
 * 2. The total number of store-versions served by this node.
 */
public class BdbStorageMetadataService extends AbstractVeniceService implements StorageMetadataService  {

  public static final String OFFSETS_STORE_NAME = "offsets_store";

  private static final long CHECKPOINTER_BYTES_INTERVAL = 0L; // To enable time based checkpointing
  private static final int PARTITION_FOR_STORE_VERSION_STATE = -1;

  private static final Logger logger = Logger.getLogger(BdbStorageMetadataService.class);
  private static final String STORE_VERSION_STATE_DESCRIPTOR_PREFIX = "StoreVersionState on Topic: ";
  private static final String OFFSET_RECORD_DESCRIPTOR_PREFIX = "OffsetRecord on Topic: ";
  private static final String OFFSET_RECORD_DESCRIPTOR_PART_2 =  " PartitionId: ";
  private final Environment offsetsBdbEnvironment;
  private final Map<String, Boolean> chunkingEnabledCache = new HashMap<>();
  private AtomicBoolean isOpen;
  private Database offsetsBdbDatabase;
  private final InternalAvroSpecificSerializer<StoreVersionState> storeVersionStateSerializer;

  public BdbStorageMetadataService(VeniceClusterConfig veniceClusterConfig) {
    String bdbMasterDir = veniceClusterConfig.getOffsetDatabasePath();
    File bdbDir = new File(bdbMasterDir);
    if (!bdbDir.exists()) {
      logger.info("Creating BDB data directory '" + bdbDir.getAbsolutePath() + ".");
      bdbDir.mkdirs();
    }

    EnvironmentConfig envConfig = new EnvironmentConfig();
    envConfig.setAllowCreate(true);
    envConfig.setReadOnly(false);
    // This is required since by default EnvironmentConfig is not transactional.
    envConfig.setTransactional(true);
    envConfig.setConfigParam(EnvironmentConfig.CHECKPOINTER_BYTES_INTERVAL, Long.toString(CHECKPOINTER_BYTES_INTERVAL));
    envConfig.setConfigParam(EnvironmentConfig.CHECKPOINTER_WAKEUP_INTERVAL,
        Long.toString(veniceClusterConfig.getOffsetManagerFlushIntervalMs() * Time.US_PER_MS));
    envConfig.setConfigParam(EnvironmentConfig.LOG_FILE_MAX, Integer.toString(veniceClusterConfig.getOffsetManagerLogFileMaxBytes()));
    envConfig.setConfigParam(EnvironmentConfig.ENV_RECOVERY_FORCE_CHECKPOINT, Boolean.toString(true));
    envConfig.setCacheSize(veniceClusterConfig.getOffsetDatabaseCacheSizeInBytes());
    envConfig.setCacheMode(CacheMode.DEFAULT); // We don't want to evict LN for the metadata service.

    this.offsetsBdbEnvironment = new Environment(bdbDir, envConfig);
    this.storeVersionStateSerializer = AvroProtocolDefinition.STORE_VERSION_STATE.getSerializer();
  }

  // PUBLIC API

  @Override
  public boolean startInner() throws Exception {
    DatabaseConfig dbConfig = new DatabaseConfig();
    dbConfig.setAllowCreate(true);
    dbConfig.setSortedDuplicates(false);
    // This is required since by default DatabaseConfig is not transactional.
    dbConfig.setTransactional(true);

    logger.info("Creating BDB environment for storing offsets: ");
    this.offsetsBdbDatabase = offsetsBdbEnvironment.openDatabase(null, OFFSETS_STORE_NAME, dbConfig);
    this.isOpen = new AtomicBoolean(true);

    // There is no async process in this function, so we are completely finished with the start up process.
    return true;
  }

  @Override
  public void stopInner()
      throws VeniceException {
    try {
      if (this.isOpen.compareAndSet(true, false)) {
        this.offsetsBdbEnvironment.sync();
        this.offsetsBdbDatabase.close();
        // This will make sure the 'cleaner thread' will be shutdown properly.
        this.offsetsBdbEnvironment.cleanLog();
        this.offsetsBdbEnvironment.close();
      }
    } catch (DatabaseException e) {
      logger.error(e);
      throw new VeniceException("Shutdown failed for BDB database " + OFFSETS_STORE_NAME, e);
    }
  }

  @Override
  public void put(String topicName, int partitionId, OffsetRecord record) throws VeniceException {
    if (partitionId < 0) {
      throw new IllegalArgumentException("partitionId cannot be < 0");
    }
    DatabaseEntry keyEntry = getBDBKey(topicName, partitionId);
    byte[] value = record.toBytes();
    put(keyEntry, value, getOffsetRecordDescriptor(topicName, partitionId));
  }

  /**
   * Persist a new {@link StoreVersionState} for the given {@param topicName}.
   *
   * @param topicName for which to retrieve the current {@link StoreVersionState}.
   * @param record    the {@link StoreVersionState} to persist
   */
  @Override
  public void put(String topicName, StoreVersionState record) throws VeniceException {
    DatabaseEntry keyEntry = getBDBKey(topicName, PARTITION_FOR_STORE_VERSION_STATE);
    byte[] value = storeVersionStateSerializer.serialize(topicName, record);
    put(keyEntry, value, getStoreVersionStateDescriptor(topicName));
    chunkingEnabledCache.put(topicName, record.chunked);
  }

  @Override
  public void clearOffset(String topicName, int partitionId) {
    if (partitionId < 0) {
      throw new IllegalArgumentException("partitionId cannot be < 0");
    }
    DatabaseEntry keyEntry = getBDBKey(topicName, partitionId);
    clear(keyEntry, getOffsetRecordDescriptor(topicName, partitionId));
  }

  /**
   * This will clear the store-version state tied to {@param topicName}.
   *
   * @param topicName to be cleared
   */
  @Override
  public void clearStoreVersionState(String topicName) {
    DatabaseEntry keyEntry = getBDBKey(topicName, PARTITION_FOR_STORE_VERSION_STATE);
    chunkingEnabledCache.remove(topicName);
    clear(keyEntry, getStoreVersionStateDescriptor(topicName));
  }

  /**
   * @param topicName  kafka topic to which the consumer thread is registered to.
   * @param partitionId  kafka partition id for which the consumer thread is registered to.
   * @return an instance of {@link OffsetRecord}, possibly an empty one if no record was found
   * @throws VeniceException
   */
  @Override
  public OffsetRecord getLastOffset(String topicName, int partitionId) throws VeniceException {
    if (partitionId < 0) {
      throw new IllegalArgumentException("partitionId cannot be < 0");
    }
    DatabaseEntry keyEntry = getBDBKey(topicName , partitionId);
    byte[] value = get(keyEntry, getOffsetRecordDescriptor(topicName, partitionId));
    if (null == value) {
      return new OffsetRecord();
    } else {
      return new OffsetRecord(value);
    }
  }

  /**
   * Gets the currently-persisted {@link StoreVersionState} for this topic.
   *
   * @param topicName kafka topic to which the consumer thread is registered to.
   * @return an instance of {@link StoreVersionState} corresponding to this topic, or {@link Optional#empty()} if not found
   */
  @Override
  public Optional<StoreVersionState> getStoreVersionState(String topicName) throws VeniceException {
    DatabaseEntry keyEntry = getBDBKey(topicName , PARTITION_FOR_STORE_VERSION_STATE);
    byte[] value = get(keyEntry, getStoreVersionStateDescriptor(topicName));
    if (null == value) {
      return Optional.empty();
    } else {
      StoreVersionState storeVersionState = storeVersionStateSerializer.deserialize(topicName, value);
      chunkingEnabledCache.put(topicName, storeVersionState.chunked);
      return Optional.of(storeVersionState);
    }
  }

  @Override
  public boolean isStoreVersionChunked(String topicName) {
    Boolean chunkingEnabled = chunkingEnabledCache.get(topicName);
    if (null == chunkingEnabled) {
      return StorageMetadataService.super.isStoreVersionChunked(topicName);
    }
    return chunkingEnabled;
  }

  // PRIVATE FUNCTIONS

  private Supplier<String> getStoreVersionStateDescriptor(String topicName) {
    return () -> STORE_VERSION_STATE_DESCRIPTOR_PREFIX + topicName;
  }

  private Supplier<String> getOffsetRecordDescriptor(String topicName, int partitionId) {
    return () -> OFFSET_RECORD_DESCRIPTOR_PREFIX + topicName + OFFSET_RECORD_DESCRIPTOR_PART_2 + partitionId;
  }

  private DatabaseEntry getBDBKey(String topic, int partitionId) {
    String keyStr = topic + "_" + partitionId;
    byte[] key = keyStr.getBytes(StandardCharsets.UTF_8);
    return new DatabaseEntry(key);
  }

  private void put(DatabaseEntry keyEntry, byte[] value, Supplier<String> descriptor) throws VeniceException {
    DatabaseEntry valueEntry = new DatabaseEntry(value);

    try {
      OperationStatus status = offsetsBdbDatabase.put(null, keyEntry, valueEntry);

      if (status != OperationStatus.SUCCESS) {
        String errorStr = "Put failed with  " + status + " for " + descriptor.get();
        logger.error(errorStr);
        throw new VeniceException(errorStr);
      }
    } catch (DatabaseException e) {
      String errorMessage = "Error in put for BDB database " + OFFSETS_STORE_NAME + " for: " + descriptor.get();
      logger.error(errorMessage , e);
      throw new VeniceException(e);
    }
  }

  private void clear(DatabaseEntry keyEntry, Supplier<String> descriptor) throws VeniceException {
    try {
      OperationStatus status = offsetsBdbDatabase.delete(null, keyEntry);

      switch (status) {
        case SUCCESS:
          logger.info("Successfully deleted offset data for " + descriptor.get());
          break;
        case NOTFOUND:
          logger.warn("Attempted to delete offset data but it was already missing for " + descriptor.get());
          break;
        default:
          throw new VeniceException("Failed to delete offset data with status '" + status + "' for " + descriptor.get());
      }
    } catch (DatabaseException e) {
      String errorMessage = "Error trying to clearOffset in BDB database " + OFFSETS_STORE_NAME + " for " + descriptor.get();
      logger.error(errorMessage , e);
      throw new VeniceException(errorMessage, e);
    }

  }

  private byte[] get(DatabaseEntry keyEntry, Supplier<String> descriptor) throws VeniceException {
    DatabaseEntry valueEntry = new DatabaseEntry();

    try {
      OperationStatus status = offsetsBdbDatabase.get(null, keyEntry, valueEntry, LockMode.READ_UNCOMMITTED);
      switch (status) {
        case SUCCESS: return valueEntry.getData();
        case NOTFOUND: return null;
        default: throw new VeniceException("Unexpected operation status '" + status.name() + "' for " + descriptor.get());
      }
    } catch (DatabaseException e) {
      String errorMessage = "Error retrieving offset for " + descriptor.get();
      logger.error(errorMessage, e);
      throw new VeniceException(errorMessage, e);
    }

  }
}
