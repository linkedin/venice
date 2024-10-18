package com.linkedin.davinci.blobtransfer;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.AbstractStoragePartition;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.Checkpoint;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;


/**
 * This class will manage the snapshot creation, for batch store and hybrid store.
 */

public class BlobSnapshotManager {
  private static final Logger LOGGER = LogManager.getLogger(BlobSnapshotManager.class);
  private static final InternalAvroSpecificSerializer<StoreVersionState> storeVersionStateSerializer =
      AvroProtocolDefinition.STORE_VERSION_STATE.getSerializer();
  private final static int DEFAULT_SNAPSHOT_RETENTION_TIME_IN_MIN = 30;
  private final long SNAPSHOT_RETENTION_TIME_IN_MILLIS =
      TimeUnit.MINUTES.toMillis(DEFAULT_SNAPSHOT_RETENTION_TIME_IN_MIN);
  public final static int DEFAULT_MAX_CONCURRENT_USERS = 5;

  // A map to keep track of the number of hosts using a snapshot for a particular topic and partition, use to restrict
  // concurrent user count
  // Example: <topicName, <partitionId, concurrentUsersAmount>>
  private Map<String, Map<Integer, AtomicLong>> concurrentSnapshotUsers;
  // A map to keep track of the snapshot timestamps for a particular topic and partition, use to track snapshot
  // staleness
  // Example: <topicName, <partitionId, timestamp>>
  private Map<String, Map<Integer, Long>> snapshotTimestamps;
  // A map to keep track the snapshot respective offset record for a particular topic and partition, use to keep
  // snapshot/offset consistency
  // Example: <topicName, <partitionId, offset>>
  private Map<String, Map<Integer, BlobTransferPartitionMetadata>> snapshotMetadataRecords;

  private final ReadOnlyStoreRepository readOnlyStoreRepository;
  private final StorageEngineRepository storageEngineRepository;
  private final StorageMetadataService storageMetadataService;
  private final int maxConcurrentUsers;
  private final long snapshotRetentionTimeInMillis;
  private final Lock lock = new ReentrantLock();

  /**
   * Constructor for the BlobSnapshotManager
   */
  public BlobSnapshotManager(
      ReadOnlyStoreRepository readOnlyStoreRepository,
      StorageEngineRepository storageEngineRepository,
      StorageMetadataService storageMetadataService,
      int maxConcurrentUsers,
      int snapshotRetentionTimeInMin) {
    this.readOnlyStoreRepository = readOnlyStoreRepository;
    this.storageEngineRepository = storageEngineRepository;
    this.storageMetadataService = storageMetadataService;
    this.maxConcurrentUsers = maxConcurrentUsers;
    this.snapshotRetentionTimeInMillis = TimeUnit.MINUTES.toMillis(snapshotRetentionTimeInMin);

    this.concurrentSnapshotUsers = new VeniceConcurrentHashMap<>();
    this.snapshotTimestamps = new VeniceConcurrentHashMap<>();
    this.snapshotMetadataRecords = new VeniceConcurrentHashMap<>();
  }

  /**
   * The constructor for the BlobSnapshotManager,
   * with default max concurrent users and snapshot retention time
   */
  @VisibleForTesting
  public BlobSnapshotManager(
      ReadOnlyStoreRepository readOnlyStoreRepository,
      StorageEngineRepository storageEngineRepository,
      StorageMetadataService storageMetadataService) {
    this(
        readOnlyStoreRepository,
        storageEngineRepository,
        storageMetadataService,
        DEFAULT_MAX_CONCURRENT_USERS,
        DEFAULT_SNAPSHOT_RETENTION_TIME_IN_MIN);
  }

  /**
   * Get the transfer metadata for a particular payload
   * 1.  the store is not hybrid, it will prepare the metadata and return it
   * 2.  the store is hybrid,
   *   2. 1. throttle the request if many concurrent users
   *   2. 2. check snapshot staleness
   *      2. 2. 1. if stale, recreate the snapshot and metadata, then return the metadata
   *      2. 2. 2. if not stale, directly return the metadata
   *
   * @param payload the blob transfer payload
   * @return the need transfer metadata to client
   */
  public BlobTransferPartitionMetadata getTransferMetadata(BlobTransferPayload payload) throws VeniceException {
    boolean isHybrid = isStoreHybrid(payload.getStoreName());
    if (!isHybrid) {
      return prepareMetadata(payload);
    } else {
      String topicName = payload.getTopicName();
      int partitionId = payload.getPartition();

      snapshotTimestamps.putIfAbsent(topicName, new VeniceConcurrentHashMap<>());
      snapshotMetadataRecords.putIfAbsent(topicName, new VeniceConcurrentHashMap<>());
      concurrentSnapshotUsers.putIfAbsent(topicName, new VeniceConcurrentHashMap<>());
      concurrentSnapshotUsers.get(topicName).putIfAbsent(partitionId, new AtomicLong(0));

      try (AutoCloseableLock ignored = AutoCloseableLock.of(lock)) {
        // check if the concurrent user count exceeds the limit
        checkIfConcurrentUserExceedsLimit(topicName, partitionId);

        // check if the snapshot is stale and need to be recreated
        if (isSnapshotStale(topicName, partitionId)) {
          // recreate the snapshot and metadata
          recreateSnapshotAndMetadata(payload);
        } else {
          LOGGER.info(
              "Snapshot for topic {} partition {} is not stale, skip creating new snapshot. ",
              topicName,
              partitionId);
        }
        concurrentSnapshotUsers.get(topicName).get(partitionId).incrementAndGet();
        return snapshotMetadataRecords.get(topicName).get(partitionId);
      }
    }
  }

  /**
   * Recreate a snapshot and metadata for a hybrid store
   * and update the snapshot timestamp and metadata records
   * @param blobTransferRequest the blob transfer request
   */
  private void recreateSnapshotAndMetadata(BlobTransferPayload blobTransferRequest) {
    String topicName = blobTransferRequest.getTopicName();
    int partitionId = blobTransferRequest.getPartition();
    try {
      // 1. get the snapshot metadata before recreating the snapshot
      BlobTransferPartitionMetadata metadataBeforeRecreateSnapshot = prepareMetadata(blobTransferRequest);
      // 2. recreate the snapshot
      createSnapshot(topicName, partitionId);

      // update the snapshot timestamp to reflect the latest snapshot creation time
      snapshotTimestamps.get(topicName).put(partitionId, System.currentTimeMillis());
      // update the snapshot offset record to reflect the latest snapshot offset
      snapshotMetadataRecords.get(topicName).put(partitionId, metadataBeforeRecreateSnapshot);
      LOGGER.info("Successfully recreated snapshot for topic {} partition {}. ", topicName, partitionId);
    } catch (Exception e) {
      String errorMessage =
          String.format("Failed to create snapshot for topic %s partition %d", topicName, partitionId);
      LOGGER.error(errorMessage, e);
      throw new VeniceException(errorMessage);
    }
  }

  /**
   * Check if the concurrent user count exceeds the limit
   * @param topicName the topic name
   * @param partitionId the partition id
   * @throws VeniceException if the concurrent user count exceeds the limit
   */
  private void checkIfConcurrentUserExceedsLimit(String topicName, int partitionId) throws VeniceException {
    boolean exceededMaxConcurrentUsers = getConcurrentSnapshotUsers(topicName, partitionId) >= maxConcurrentUsers;
    if (exceededMaxConcurrentUsers) {
      String errorMessage = String.format(
          "Exceeded the maximum number of concurrent users %d for topic %s partition %d",
          maxConcurrentUsers,
          topicName,
          partitionId);
      LOGGER.error(errorMessage);
      throw new VeniceException(errorMessage);
    }
  }

  /**
   * Check if the snapshot is stale
   * @param topicName the topic name
   * @param partitionId the partition id
   * @return true if the snapshot is stale, false otherwise
   */
  private boolean isSnapshotStale(String topicName, int partitionId) {
    if (!snapshotTimestamps.containsKey(topicName) || !snapshotTimestamps.get(topicName).containsKey(partitionId)) {
      return true;
    }
    return System.currentTimeMillis()
        - snapshotTimestamps.get(topicName).get(partitionId) > snapshotRetentionTimeInMillis;
  }

  /**
   * Decrease the count of hosts using the snapshot
   */
  public void decreaseConcurrentUserCount(String topicName, int partitionId) {
    Map<Integer, AtomicLong> concurrentPartitionUsers = concurrentSnapshotUsers.get(topicName);
    if (concurrentPartitionUsers == null) {
      throw new VeniceException("No topic found: " + topicName);
    }
    AtomicLong concurrentUsers = concurrentPartitionUsers.get(partitionId);
    if (concurrentUsers == null) {
      throw new VeniceException(String.format("%d partition not found on topic %s", partitionId, topicName));
    }
    long result = concurrentUsers.decrementAndGet();
    if (result < 0) {
      throw new VeniceException("Concurrent user count cannot be negative");
    }
  }

  protected long getConcurrentSnapshotUsers(String topicName, int partitionId) {
    if (topicName == null) {
      throw new IllegalArgumentException("RocksDB instance and topicName cannot be null");
    }
    if (!concurrentSnapshotUsers.containsKey(topicName)
        || !concurrentSnapshotUsers.get(topicName).containsKey(partitionId)) {
      return 0;
    }
    return concurrentSnapshotUsers.get(topicName).get(partitionId).get();
  }

  /**
   * Check if the store is hybrid
   * @param storeName the name of the store
   * @return true if the store is hybrid, false otherwise
   */
  public boolean isStoreHybrid(String storeName) {
    Store store = readOnlyStoreRepository.getStore(storeName);
    if (store != null) {
      return store.isHybrid();
    }
    return false;
  }

  /**
   * util method to create a snapshot
   * It will check the snapshot directory and delete it if it exists, then generate a new snapshot
   */
  public static void createSnapshot(RocksDB rocksDB, String fullPathForPartitionDBSnapshot) {
    LOGGER.info("Creating snapshot in directory: {}", fullPathForPartitionDBSnapshot);

    // clean up the snapshot directory if it exists
    File partitionSnapshotDir = new File(fullPathForPartitionDBSnapshot);
    if (partitionSnapshotDir.exists()) {
      LOGGER.info("Snapshot directory already exists, deleting old snapshots at {}", fullPathForPartitionDBSnapshot);
      try {
        FileUtils.deleteDirectory(partitionSnapshotDir);
      } catch (IOException e) {
        throw new VeniceException(
            "Failed to delete the existing snapshot directory: " + fullPathForPartitionDBSnapshot,
            e);
      }
    }

    try {
      LOGGER.info("Start creating snapshots in directory: {}", fullPathForPartitionDBSnapshot);

      Checkpoint checkpoint = Checkpoint.create(rocksDB);
      checkpoint.createCheckpoint(fullPathForPartitionDBSnapshot);

      LOGGER.info("Finished creating snapshots in directory: {}", fullPathForPartitionDBSnapshot);
    } catch (RocksDBException e) {
      throw new VeniceException(
          "Received exception during RocksDB's snapshot creation in directory " + fullPathForPartitionDBSnapshot,
          e);
    }
  }

  /**
   * Create a snapshot for a particular partition
   */
  public void createSnapshot(String kafkaVersionTopic, int partitionId) {
    AbstractStorageEngine storageEngine =
        Objects.requireNonNull(storageEngineRepository.getLocalStorageEngine(kafkaVersionTopic));
    AbstractStoragePartition partition = storageEngine.getPartitionOrThrow(partitionId);
    partition.createSnapshot();
  }

  /**
   * Decrease the user count for the snapshot to allow other hosts can use it.
   * @param blobTransferRequest the blob transfer request
   */
  public void removeConcurrentUserRestriction(BlobTransferPayload blobTransferRequest) {
    boolean isHybridStore = isStoreHybrid(blobTransferRequest.getStoreName());
    if (!isHybridStore) {
      LOGGER.info(
          "Snapshot for topic {} partition {} is not new hybrid, skip reset user count",
          blobTransferRequest.getTopicName(),
          blobTransferRequest.getPartition());
      return;
    }

    // decrease the user count for allowing other hosts can use it
    decreaseConcurrentUserCount(blobTransferRequest.getTopicName(), blobTransferRequest.getPartition());
    LOGGER.info(
        "Decreased user count for snapshot of topic {} partition {}",
        blobTransferRequest.getTopicName(),
        blobTransferRequest.getPartition());
  }

  /**
   * Get the snapshot metadata for a particular topic and partition
   * @param topicName the topic name
   * @param partitionId the partition id
   * @return the snapshot metadata
   */
  public BlobTransferPartitionMetadata getTransferredSnapshotMetadata(String topicName, int partitionId) {
    return snapshotMetadataRecords.get(topicName).get(partitionId);
  }

  /**
   * cleanup the snapshot timestamp and metadata records and concurrent users
   * @return
   */
  public void resetSnapshotTracking() {
    snapshotTimestamps.clear();
    snapshotMetadataRecords.clear();
    concurrentSnapshotUsers.clear();
  }

  /**
   * Prepare the metadata for a blob transfer request
   * @param blobTransferRequest the blob transfer request
   * @return the metadata for the blob transfer request
   */
  public BlobTransferPartitionMetadata prepareMetadata(BlobTransferPayload blobTransferRequest) {
    // prepare metadata
    BlobTransferPartitionMetadata metadata = null;
    StoreVersionState storeVersionState =
        storageMetadataService.getStoreVersionState(blobTransferRequest.getTopicName());
    java.nio.ByteBuffer storeVersionStateByte =
        ByteBuffer.wrap(storeVersionStateSerializer.serialize(blobTransferRequest.getTopicName(), storeVersionState));

    OffsetRecord offsetRecord =
        storageMetadataService.getLastOffset(blobTransferRequest.getTopicName(), blobTransferRequest.getPartition());
    java.nio.ByteBuffer offsetRecordByte = ByteBuffer.wrap(offsetRecord.toBytes());

    metadata = new BlobTransferPartitionMetadata(
        blobTransferRequest.getTopicName(),
        blobTransferRequest.getPartition(),
        offsetRecordByte,
        storeVersionStateByte);

    return metadata;
  }
}
