package com.linkedin.davinci.blobtransfer;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.alpini.base.concurrency.Executors;
import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.AbstractStoragePartition;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.SparseConcurrentList;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class will manage the snapshot creation, for batch store and hybrid store.
 */

public class BlobSnapshotManager {
  private static final Logger LOGGER = LogManager.getLogger(BlobSnapshotManager.class);
  private static final InternalAvroSpecificSerializer<StoreVersionState> storeVersionStateSerializer =
      AvroProtocolDefinition.STORE_VERSION_STATE.getSerializer();
  private final static int DEFAULT_SNAPSHOT_RETENTION_TIME_IN_MIN = 30;
  public final static int DEFAULT_MAX_CONCURRENT_USERS = 5;
  public final static int DEFAULT_SNAPSHOT_CLEANUP_INTERVAL_IN_MINS = 120;

  // A map to keep track of the number of hosts using a snapshot for a particular topic and partition, use to restrict
  // concurrent user count
  // Example: <topicName, <partitionId, concurrentUsersAmount>>
  private VeniceConcurrentHashMap<String, VeniceConcurrentHashMap<Integer, AtomicInteger>> concurrentSnapshotUsers;
  // A map to keep track of the snapshot timestamps for a particular topic and partition, use to track snapshot
  // staleness
  // Example: <topicName, <partitionId, timestamp>>
  private VeniceConcurrentHashMap<String, VeniceConcurrentHashMap<Integer, Long>> snapshotTimestamps;
  // A map to keep track the snapshot respective offset record for a particular topic and partition, use to keep
  // snapshot/offset consistency
  // Example: <topicName, <partitionId, offset>>
  private VeniceConcurrentHashMap<String, VeniceConcurrentHashMap<Integer, BlobTransferPartitionMetadata>> snapshotMetadataRecords;

  // Locks for coordinating access to each snapshot
  // Example: <topicName, <partitionId, lock>>
  private VeniceConcurrentHashMap<String, SparseConcurrentList<ReentrantLock>> snapshotAccessLocks;

  private final StorageEngineRepository storageEngineRepository;
  private final StorageMetadataService storageMetadataService;
  private final int maxConcurrentUsers;
  private final long snapshotRetentionTimeInMillis;
  private final int snapshotCleanupIntervalInMins;
  private final BlobTransferUtils.BlobTransferTableFormat blobTransferTableFormat;
  private final ScheduledExecutorService snapshotCleanupScheduler;

  /**
   * Constructor for the BlobSnapshotManager
   */
  public BlobSnapshotManager(
      StorageEngineRepository storageEngineRepository,
      StorageMetadataService storageMetadataService,
      int maxConcurrentUsers,
      int snapshotRetentionTimeInMin,
      BlobTransferUtils.BlobTransferTableFormat transferTableFormat,
      int snapshotCleanupIntervalInMins) {
    this.storageEngineRepository = storageEngineRepository;
    this.storageMetadataService = storageMetadataService;
    this.maxConcurrentUsers = maxConcurrentUsers;
    this.snapshotRetentionTimeInMillis = TimeUnit.MINUTES.toMillis(snapshotRetentionTimeInMin);
    this.blobTransferTableFormat = transferTableFormat;
    this.snapshotCleanupIntervalInMins = snapshotCleanupIntervalInMins;

    this.concurrentSnapshotUsers = new VeniceConcurrentHashMap<>();
    this.snapshotTimestamps = new VeniceConcurrentHashMap<>();
    this.snapshotMetadataRecords = new VeniceConcurrentHashMap<>();

    this.snapshotAccessLocks = new VeniceConcurrentHashMap<>();

    this.snapshotCleanupScheduler = Executors
        .newSingleThreadScheduledExecutor(new DaemonThreadFactory("Venice-BlobTransfer-Snapshot-Cleanup-Scheduler"));

    scheduleCleanupOutOfRetentionSnapshotTask();
  }

  /**
   * The constructor for the BlobSnapshotManager,
   * with default max concurrent users and snapshot retention time
   */
  @VisibleForTesting
  public BlobSnapshotManager(
      StorageEngineRepository storageEngineRepository,
      StorageMetadataService storageMetadataService) {
    this(
        storageEngineRepository,
        storageMetadataService,
        DEFAULT_MAX_CONCURRENT_USERS,
        DEFAULT_SNAPSHOT_RETENTION_TIME_IN_MIN,
        BlobTransferUtils.BlobTransferTableFormat.BLOCK_BASED_TABLE,
        DEFAULT_SNAPSHOT_CLEANUP_INTERVAL_IN_MINS);
  }

  /**
   * Get the transfer metadata for a particular payload
   * 1. throttle the request if many concurrent users.
   * 2. check snapshot staleness
   *     2.1. if stale:
   *            2.1.1. if it does not have active users: recreate the snapshot and metadata, then return the metadata
   *            2.1.2. if it has active users: no need to recreate the snapshot, throw an exception to let the client move to next candidate.
   *     2.2. if not stale, directly return the metadata
   *
   * @param payload the blob transfer payload
   * @return the need transfer metadata to client
   */
  public BlobTransferPartitionMetadata getTransferMetadata(BlobTransferPayload payload) throws VeniceException {
    String topicName = payload.getTopicName();
    int partitionId = payload.getPartition();

    // check if storageEngineRepository has this store partition, so exit early if not, otherwise won't be able to
    // create snapshot
    if (storageEngineRepository.getLocalStorageEngine(topicName) == null
        || !storageEngineRepository.getLocalStorageEngine(topicName).containsPartition(partitionId)) {
      throw new VeniceException("No storage engine found for topic: " + topicName + " partition: " + partitionId);
    }

    ReentrantLock lock = getSnapshotLock(topicName, partitionId);
    try (AutoCloseableLock ignored = AutoCloseableLock.of(lock)) {
      // 1. check if the concurrent user count exceeds the limit
      checkIfConcurrentUserExceedsLimit(topicName, partitionId);

      initializeTrackingValues(topicName, partitionId);

      boolean havingActiveUsers = getConcurrentSnapshotUsers(topicName, partitionId) > 0;
      boolean isSnapshotStale = isSnapshotStale(topicName, partitionId);
      increaseConcurrentUserCount(topicName, partitionId);

      // 2. check if the snapshot is stale and need to be recreated
      if (isSnapshotStale) {
        if (!havingActiveUsers) {
          recreateSnapshotAndMetadata(payload);
        } else {
          String errorMessage = String.format(
              "Snapshot for topic %s partition %d is still in use by others, can not recreate snapshot for new transfer request.",
              topicName,
              partitionId);
          LOGGER.warn(errorMessage);
          throw new VeniceException(errorMessage);
        }
      } else {
        LOGGER.info(
            "Snapshot for topic {} partition {} is not stale, skip creating new snapshot. ",
            topicName,
            partitionId);
      }
      return snapshotMetadataRecords.get(topicName).get(partitionId);
    }
  }

  /**
   * Recreate a snapshot and metadata for both batch and hybrid store
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
   * Increase the count of hosts using the snapshot
   */
  void increaseConcurrentUserCount(String topicName, int partitionId) {
    concurrentSnapshotUsers.computeIfAbsent(topicName, k -> new VeniceConcurrentHashMap<>())
        .computeIfAbsent(partitionId, k -> new AtomicInteger(0))
        .incrementAndGet();
  }

  /**
   * Decrease the count of hosts using the snapshot
   */
  public void decreaseConcurrentUserCount(BlobTransferPayload blobTransferRequest) {
    String topicName = blobTransferRequest.getTopicName();
    int partitionId = blobTransferRequest.getPartition();
    Map<Integer, AtomicInteger> concurrentPartitionUsers = concurrentSnapshotUsers.get(topicName);
    if (concurrentPartitionUsers == null) {
      throw new VeniceException("No topic found: " + topicName);
    }

    AtomicInteger concurrentUsers = concurrentPartitionUsers.get(partitionId);
    if (concurrentUsers == null) {
      throw new VeniceException(String.format("%d partition not found on topic %s", partitionId, topicName));
    }
    long result = concurrentUsers.decrementAndGet();
    if (result < 0) {
      LOGGER.warn(
          "Concurrent user count for topic {} partition {} is negative: {}. This should not happen, but resetting to 0. ",
          topicName,
          partitionId,
          result);
      concurrentUsers.set(0);
    }

    LOGGER.info("Concurrent user count for topic {} partition {} decreased to {}", topicName, partitionId, result);
  }

  protected int getConcurrentSnapshotUsers(String topicName, int partitionId) {
    if (topicName == null) {
      throw new IllegalArgumentException("RocksDB instance and topicName cannot be null");
    }
    Map<Integer, AtomicInteger> partitionUsageMap = concurrentSnapshotUsers.get(topicName);
    if (partitionUsageMap == null) {
      return 0;
    }
    AtomicInteger usage = partitionUsageMap.get(partitionId);
    if (usage == null) {
      return 0;
    }
    return usage.get();
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
   * Cleanup the snapshot for a particular partition
   * @param kafkaVersionTopic the topic name
   * @param partitionId the partition id
   */
  public void cleanupSnapshot(String kafkaVersionTopic, int partitionId) {
    AbstractStorageEngine storageEngine =
        Objects.requireNonNull(storageEngineRepository.getLocalStorageEngine(kafkaVersionTopic));
    AbstractStoragePartition partition = storageEngine.getPartitionOrThrow(partitionId);
    partition.cleanupSnapshot();
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
   * Prepare the metadata for a blob transfer request
   * @param blobTransferRequest the blob transfer request
   * @return the metadata for the blob transfer request
   */
  public BlobTransferPartitionMetadata prepareMetadata(BlobTransferPayload blobTransferRequest) {
    if (storageMetadataService == null || storeVersionStateSerializer == null) {
      throw new VeniceException("StorageMetadataService or storeVersionStateSerializer is not initialized");
    }

    if (storageMetadataService.getStoreVersionState(blobTransferRequest.getTopicName()) == null
        || storageMetadataService
            .getLastOffset(blobTransferRequest.getTopicName(), blobTransferRequest.getPartition()) == null) {
      throw new VeniceException("Cannot get store version state or offset record from storage metadata service.");
    }

    // prepare metadata
    StoreVersionState storeVersionState =
        storageMetadataService.getStoreVersionState(blobTransferRequest.getTopicName());
    java.nio.ByteBuffer storeVersionStateByte =
        ByteBuffer.wrap(storeVersionStateSerializer.serialize(blobTransferRequest.getTopicName(), storeVersionState));

    OffsetRecord offsetRecord =
        storageMetadataService.getLastOffset(blobTransferRequest.getTopicName(), blobTransferRequest.getPartition());
    java.nio.ByteBuffer offsetRecordByte = ByteBuffer.wrap(offsetRecord.toBytes());

    return new BlobTransferPartitionMetadata(
        blobTransferRequest.getTopicName(),
        blobTransferRequest.getPartition(),
        offsetRecordByte,
        storeVersionStateByte);
  }

  /**
   * Get the current snapshot format, which is a config value.
   * @return the transfer table format, BLOCK_BASED_TABLE or PLAIN_TABLE.
   */
  public BlobTransferUtils.BlobTransferTableFormat getBlobTransferTableFormat() {
    return this.blobTransferTableFormat;
  }

  /**
   * Get the lock for a particular topic and partition
   */
  private ReentrantLock getSnapshotLock(String topicName, int partitionId) {
    return snapshotAccessLocks.computeIfAbsent(topicName, k -> new SparseConcurrentList<>())
        .computeIfAbsent(partitionId, p -> new ReentrantLock());
  }

  /**
   * Initialize tracking values for a topic-partition
   */
  private void initializeTrackingValues(String topicName, int partitionId) {
    snapshotTimestamps.computeIfAbsent(topicName, k -> new VeniceConcurrentHashMap<>());
    snapshotMetadataRecords.computeIfAbsent(topicName, k -> new VeniceConcurrentHashMap<>());
    concurrentSnapshotUsers.computeIfAbsent(topicName, k -> new VeniceConcurrentHashMap<>())
        .computeIfAbsent(partitionId, k -> new AtomicInteger(0));
  }

  /**
   * Remove tracking values for a topic-partition when the snapshot is cleaned up
   */
  public void removeTrackingValues(String topicName, int partitionId) {
    removePartitionEntry(snapshotTimestamps, topicName, partitionId);
    removePartitionEntry(snapshotMetadataRecords, topicName, partitionId);
    removePartitionEntry(concurrentSnapshotUsers, topicName, partitionId);
    SparseConcurrentList<ReentrantLock> lockList = snapshotAccessLocks.get(topicName);
    if (lockList != null) {
      lockList.remove(partitionId);
      if (lockList.isEmpty()) {
        snapshotAccessLocks.remove(topicName);
      }
    }
  }

  /**
   * Remove the partition entry from the map
   */
  private <V> void removePartitionEntry(
      Map<String, VeniceConcurrentHashMap<Integer, V>> map,
      String topicName,
      int partitionId) {
    VeniceConcurrentHashMap<Integer, V> partitionMap = map.get(topicName);
    if (partitionMap != null) {
      partitionMap.remove(partitionId);
      if (partitionMap.isEmpty()) {
        map.remove(topicName);
      }
    }
  }

  /**
   * A regular cleanup task to clean up the snapshot folder which is out of retention time.
   */
  public void cleanupOutOfRetentionSnapshot(String topicName, int partitionId) {
    ReentrantLock lock = getSnapshotLock(topicName, partitionId);
    try (AutoCloseableLock ignored = AutoCloseableLock.of(lock)) {

      if (getConcurrentSnapshotUsers(topicName, partitionId) > 0 || !isSnapshotStale(topicName, partitionId)) {
        return;
      }

      LOGGER.info("Cleaning up stale snapshot for topic {} partition {}", topicName, partitionId);
      cleanupSnapshot(topicName, partitionId);
      removeTrackingValues(topicName, partitionId);

      LOGGER.info("Successfully cleaned up snapshot for topic {} partition {}", topicName, partitionId);
    } catch (Exception e) {
      LOGGER.error("Failed to clean up snapshot for topic {} partition {}", topicName, partitionId, e);
    }
  }

  /**
   * Schedule a task to clean up the snapshot folder which is out of retention time for all topics and partitions.
   */
  private void scheduleCleanupOutOfRetentionSnapshotTask() {
    if (snapshotCleanupScheduler != null) {
      snapshotCleanupScheduler.scheduleAtFixedRate(() -> {
        if (snapshotTimestamps.isEmpty()) {
          return;
        }

        LOGGER.info("Start cleaning up stale snapshots for all topics and partitions");

        snapshotTimestamps.forEach((topicName, partitionMap) -> {
          if (partitionMap != null) {
            Set<Integer> partitionIds = new HashSet<>(partitionMap.keySet());

            partitionIds.forEach(partitionId -> {
              if (partitionMap.containsKey(partitionId)) {
                cleanupOutOfRetentionSnapshot(topicName, partitionId);
              }
            });
          }
        });

        LOGGER.info("Finished cleaning up stale snapshots for all topics and partitions");
      }, 0, snapshotCleanupIntervalInMins, TimeUnit.MINUTES);
    }
  }

  public void shutdown() {
    concurrentSnapshotUsers.clear();
    snapshotTimestamps.clear();
    snapshotMetadataRecords.clear();
    snapshotAccessLocks.clear();

    if (snapshotCleanupScheduler != null) {
      snapshotCleanupScheduler.shutdown();
    }
  }
}
