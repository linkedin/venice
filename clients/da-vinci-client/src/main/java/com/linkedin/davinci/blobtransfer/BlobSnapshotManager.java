package com.linkedin.davinci.blobtransfer;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.alpini.base.concurrency.Executors;
import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.AbstractStoragePartition;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
  private VeniceConcurrentHashMap<String, VeniceConcurrentHashMap<Integer, ReentrantLock>> snapshotAccessLocks;
  // Flags to track cleanup operations in progress
  // Example: <topicName, <partitionId, isCleanupInProgress>>
  private VeniceConcurrentHashMap<String, VeniceConcurrentHashMap<Integer, AtomicBoolean>> cleanupInProgress;
  // Flags to track creation operations in progress
  // Example: <topicName, <partitionId, isCreationInProgress>>
  private VeniceConcurrentHashMap<String, VeniceConcurrentHashMap<Integer, AtomicBoolean>> creationInProgress;

  private final ReadOnlyStoreRepository readOnlyStoreRepository;
  private final StorageEngineRepository storageEngineRepository;
  private final StorageMetadataService storageMetadataService;
  private final int maxConcurrentUsers;
  private final long snapshotRetentionTimeInMillis;
  private final BlobTransferUtils.BlobTransferTableFormat blobTransferTableFormat;
  private final ScheduledExecutorService snapshotCleanupScheduler;

  /**
   * Constructor for the BlobSnapshotManager
   */
  public BlobSnapshotManager(
      ReadOnlyStoreRepository readOnlyStoreRepository,
      StorageEngineRepository storageEngineRepository,
      StorageMetadataService storageMetadataService,
      int maxConcurrentUsers,
      int snapshotRetentionTimeInMin,
      BlobTransferUtils.BlobTransferTableFormat transferTableFormat) {
    this.readOnlyStoreRepository = readOnlyStoreRepository;
    this.storageEngineRepository = storageEngineRepository;
    this.storageMetadataService = storageMetadataService;
    this.maxConcurrentUsers = maxConcurrentUsers;
    this.snapshotRetentionTimeInMillis = TimeUnit.MINUTES.toMillis(snapshotRetentionTimeInMin);
    this.blobTransferTableFormat = transferTableFormat;

    this.concurrentSnapshotUsers = new VeniceConcurrentHashMap<>();
    this.snapshotTimestamps = new VeniceConcurrentHashMap<>();
    this.snapshotMetadataRecords = new VeniceConcurrentHashMap<>();

    this.snapshotAccessLocks = new VeniceConcurrentHashMap<>();
    this.cleanupInProgress = new VeniceConcurrentHashMap<>();
    this.creationInProgress = new VeniceConcurrentHashMap<>();

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
      ReadOnlyStoreRepository readOnlyStoreRepository,
      StorageEngineRepository storageEngineRepository,
      StorageMetadataService storageMetadataService) {
    this(
        readOnlyStoreRepository,
        storageEngineRepository,
        storageMetadataService,
        DEFAULT_MAX_CONCURRENT_USERS,
        DEFAULT_SNAPSHOT_RETENTION_TIME_IN_MIN,
        BlobTransferUtils.BlobTransferTableFormat.BLOCK_BASED_TABLE);
  }

  /**
   * Get the transfer metadata for a particular payload
   * 1. throttle the request if many concurrent users.
   * 2. check if any cleanup or recreate operations are in progress, if yes, throw an exception
   * 3. check snapshot staleness
   *     3.1. if stale:
   *            3.1.1. if it does not have active users: recreate the snapshot and metadata, then return the metadata
   *            3.1.2. if it has active users: no need to recreate the snapshot, throw an exception to let the client move to next candidate.
   *     3.2. if not stale, directly return the metadata
   *
   * @param payload the blob transfer payload
   * @return the need transfer metadata to client
   */
  public BlobTransferPartitionMetadata getTransferMetadata(BlobTransferPayload payload) throws VeniceException {
    String topicName = payload.getTopicName();
    int partitionId = payload.getPartition();

    // 1. check if the concurrent user count exceeds the limit
    checkIfConcurrentUserExceedsLimit(topicName, partitionId);

    // check if storageEngineRepository has this store partition, so exit early if not, otherwise won't be able to
    // create snapshot
    if (storageEngineRepository.getLocalStorageEngine(topicName) == null
        || !storageEngineRepository.getLocalStorageEngine(topicName).containsPartition(partitionId)) {
      throw new VeniceException("No storage engine found for topic: " + topicName + " partition: " + partitionId);
    }

    initializeTrackingValues(topicName, partitionId);

    ReentrantLock lock = getSnapshotLock(topicName, partitionId);
    try (AutoCloseableLock ignored = AutoCloseableLock.of(lock)) {
      // 2. check if any cleanup or recreate is in progress
      if (isCleanupInProgress(topicName, partitionId) || isCreationInProgress(topicName, partitionId)) {
        String errorMessage = String.format(
            "Cleanup/Creation in progress for topic %s partition %d, cannot access snapshot at this time",
            topicName,
            partitionId);
        throw new VeniceException(errorMessage);
      }

      boolean havingActiveUsers = getConcurrentSnapshotUsers(topicName, partitionId) > 0;
      boolean isSnapshotStale = isSnapshotStale(topicName, partitionId);
      increaseConcurrentUserCount(topicName, partitionId);

      // 3. check if the snapshot is stale and need to be recreated
      if (isSnapshotStale) {
        if (!havingActiveUsers) {
          if (!isCreationInProgress(topicName, partitionId) && !isCleanupInProgress(topicName, partitionId)) {
            try {
              setCreationInProgress(topicName, partitionId, true);
              recreateSnapshotAndMetadata(payload);
            } finally {
              setCreationInProgress(topicName, partitionId, false);
            }
          }
        } else {
          String errorMessage = String.format(
              "Snapshot for topic %s partition %d is still in use by others, can not recreate snapshot for new transfer request.",
              topicName,
              partitionId);
          LOGGER.warn(errorMessage);
          throw new VeniceException(errorMessage);
        }
      } else {
        LOGGER.debug(
            "Snapshot for topic {} partition {} is not stale, skip creating new snapshot. ",
            topicName,
            partitionId);
      }
      return snapshotMetadataRecords.get(topicName).get(partitionId);
    } catch (Exception e) {
      throw new VeniceException(
          String.format("Failed to get transfer metadata for topic %s partition %d", topicName, partitionId),
          e);
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
    snapshotAccessLocks.putIfAbsent(topicName, new VeniceConcurrentHashMap<>());
    snapshotAccessLocks.get(topicName).putIfAbsent(partitionId, new ReentrantLock());
    return snapshotAccessLocks.get(topicName).get(partitionId);
  }

  /**
   * Initialize tracking values for a topic-partition
   */
  private void initializeTrackingValues(String topicName, int partitionId) {
    snapshotTimestamps.computeIfAbsent(topicName, k -> new VeniceConcurrentHashMap<>());
    snapshotMetadataRecords.computeIfAbsent(topicName, k -> new VeniceConcurrentHashMap<>());
    concurrentSnapshotUsers.computeIfAbsent(topicName, k -> new VeniceConcurrentHashMap<>())
        .computeIfAbsent(partitionId, k -> new AtomicInteger(0));
    cleanupInProgress.computeIfAbsent(topicName, k -> new VeniceConcurrentHashMap<>())
        .computeIfAbsent(partitionId, k -> new AtomicBoolean(false));
    creationInProgress.computeIfAbsent(topicName, k -> new VeniceConcurrentHashMap<>())
        .computeIfAbsent(partitionId, k -> new AtomicBoolean(false));
  }

  /**
   * Check if cleanup is in progress for per topic per partition
   */
  private boolean isCleanupInProgress(String topicName, int partitionId) {
    return cleanupInProgress.containsKey(topicName) && cleanupInProgress.get(topicName).containsKey(partitionId)
        && cleanupInProgress.get(topicName).get(partitionId).get();
  }

  /**
   * Check if creation is in progress for per topic per partition
   */
  private boolean isCreationInProgress(String topicName, int partitionId) {
    return creationInProgress.containsKey(topicName) && creationInProgress.get(topicName).containsKey(partitionId)
        && creationInProgress.get(topicName).get(partitionId).get();
  }

  /**
   * Mark cleanup as in progress or completed for per topic per partition
   */
  private void setCleanupInProgress(String topicName, int partitionId, boolean inProgress) {
    cleanupInProgress.computeIfAbsent(topicName, k -> new VeniceConcurrentHashMap<>())
        .computeIfAbsent(partitionId, k -> new AtomicBoolean(false))
        .set(inProgress);
  }

  /**
   * Mark creation as in progress or completed for per topic per partition
   */
  private void setCreationInProgress(String topicName, int partitionId, boolean inProgress) {
    creationInProgress.computeIfAbsent(topicName, k -> new VeniceConcurrentHashMap<>())
        .computeIfAbsent(partitionId, k -> new AtomicBoolean(false))
        .set(inProgress);
  }

  /**
   * A regular cleanup task to clean up the snapshot folder which is out of retention time.
   */
  public void cleanupOutOfRetentionSnapshot(String topicName, int partitionId) {
    ReentrantLock lock = getSnapshotLock(topicName, partitionId);
    try (AutoCloseableLock ignored = AutoCloseableLock.of(lock)) {
      setCleanupInProgress(topicName, partitionId, true);

      if (getConcurrentSnapshotUsers(topicName, partitionId) > 0 || !isSnapshotStale(topicName, partitionId)
          || isCreationInProgress(topicName, partitionId)) {
        return;
      }

      LOGGER.info("Cleaning up stale snapshot for topic {} partition {}", topicName, partitionId);
      cleanupSnapshot(topicName, partitionId);

      if (snapshotTimestamps.containsKey(topicName)) {
        snapshotTimestamps.get(topicName).remove(partitionId);
        if (snapshotTimestamps.get(topicName).isEmpty()) {
          snapshotTimestamps.remove(topicName);
        }
      }

      if (snapshotMetadataRecords.containsKey(topicName)) {
        snapshotMetadataRecords.get(topicName).remove(partitionId);
        if (snapshotMetadataRecords.get(topicName).isEmpty()) {
          snapshotMetadataRecords.remove(topicName);
        }
      }

      LOGGER.info("Successfully cleaned up snapshot for topic {} partition {}", topicName, partitionId);
    } catch (Exception e) {
      LOGGER.error("Failed to clean up snapshot for topic {} partition {}", topicName, partitionId, e);
    } finally {
      setCleanupInProgress(topicName, partitionId, false);
    }
  }

  /**
   * Schedule a task to clean up the snapshot folder which is out of retention time for all topics and partitions.
   */
  public void scheduleCleanupOutOfRetentionSnapshotTask() {
    if (snapshotCleanupScheduler != null) {
      snapshotCleanupScheduler.scheduleAtFixedRate(() -> {
        if (snapshotTimestamps.isEmpty()) {
          return;
        }

        // deep copy the snapshotTimestamps map to avoid concurrent modification while iterating
        VeniceConcurrentHashMap<String, VeniceConcurrentHashMap<Integer, Long>> snapshotTimestampsCopy =
            new VeniceConcurrentHashMap<>();
        for (Map.Entry<String, VeniceConcurrentHashMap<Integer, Long>> entry: snapshotTimestamps.entrySet()) {
          String topicName = entry.getKey();
          VeniceConcurrentHashMap<Integer, Long> partitionMap = new VeniceConcurrentHashMap<>();
          for (Map.Entry<Integer, Long> partitionEntry: entry.getValue().entrySet()) {
            partitionMap.put(partitionEntry.getKey(), partitionEntry.getValue());
          }
          snapshotTimestampsCopy.put(topicName, partitionMap);
        }

        LOGGER.info("Start cleaning up stale snapshots for all topics and partitions");
        for (Map.Entry<String, VeniceConcurrentHashMap<Integer, Long>> entry: snapshotTimestampsCopy.entrySet()) {
          String topicName = entry.getKey();
          VeniceConcurrentHashMap<Integer, Long> partitionMap = entry.getValue();
          for (Map.Entry<Integer, Long> partitionEntry: partitionMap.entrySet()) {
            int partitionId = partitionEntry.getKey();
            cleanupOutOfRetentionSnapshot(topicName, partitionId);
          }
        }
        LOGGER.info("Finished cleaning up stale snapshots for all topics and partitions");
      }, 0, 1, TimeUnit.DAYS);
    }
  }

  public void shutdown() {
    concurrentSnapshotUsers.clear();
    snapshotTimestamps.clear();
    snapshotMetadataRecords.clear();
    snapshotAccessLocks.clear();
    cleanupInProgress.clear();
    creationInProgress.clear();

    if (snapshotCleanupScheduler != null) {
      snapshotCleanupScheduler.shutdown();
    }
  }
}
