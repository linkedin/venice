package com.linkedin.davinci.blobtransfer;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.store.rocksdb.RocksDBUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import java.io.File;
import java.io.IOException;
import java.util.Map;
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
 * This class will take and return a snapshot of a hybrid store, if someone is using a snapshot then it will return
 * the snapshot that person is using, otherwise, it will return the last snapshot taken if it is not stale
 * If the snapshot is stale and no one is using it, it will update the snapshot and return the new one
 */

public class BlobSnapshotManager {
  private final Map<String, Map<Integer, AtomicLong>> concurrentSnapshotUsers;
  private Map<String, Map<Integer, Long>> snapShotTimestamps;
  private final long snapshotRetentionTime;
  private final String basePath;
  private final ReadOnlyStoreRepository readOnlyStoreRepository;
  private final Lock lock = new ReentrantLock();
  private static final Logger LOGGER = LogManager.getLogger(BlobSnapshotManager.class);

  /**
   * Constructor for the BlobSnapshotManager
   */
  public BlobSnapshotManager(
      String basePath,
      long snapshotRetentionTime,
      ReadOnlyStoreRepository readOnlyStoreRepository) {
    this.basePath = basePath;
    this.snapshotRetentionTime = snapshotRetentionTime;
    this.readOnlyStoreRepository = readOnlyStoreRepository;
    this.snapShotTimestamps = new VeniceConcurrentHashMap<>();
    this.concurrentSnapshotUsers = new VeniceConcurrentHashMap<>();
  }

  /**
   * Checks if the snapshot is stale, if it is and no one is using it, it updates the snapshot,
   * otherwise it increases the count of people using the snapshot
   */
  public void maybeUpdateHybridSnapshot(RocksDB rocksDB, String topicName, int partitionId) {
    if (rocksDB == null || topicName == null) {
      throw new IllegalArgumentException("RocksDB instance and topicName cannot be null");
    }
    if (!isStoreHybrid(topicName)) {
      LOGGER.warn("Store {} is not hybrid, skipping snapshot update", topicName);
      return;
    }
    String fullPathForPartitionDBSnapshot = RocksDBUtils.composeSnapshotDir(this.basePath, topicName, partitionId);
    snapShotTimestamps.putIfAbsent(topicName, new VeniceConcurrentHashMap<>());
    concurrentSnapshotUsers.putIfAbsent(topicName, new VeniceConcurrentHashMap<>());
    concurrentSnapshotUsers.get(topicName).putIfAbsent(partitionId, new AtomicLong(0));
    try (AutoCloseableLock ignored = AutoCloseableLock.of(lock)) {
      if (isSnapshotStale(topicName, partitionId)) {
        if (concurrentSnapshotUsers.get(topicName).get(partitionId) == null
            || concurrentSnapshotUsers.get(topicName).get(partitionId).get() == 0) {
          updateHybridSnapshot(rocksDB, topicName, partitionId);
          snapShotTimestamps.get(topicName).put(partitionId, System.currentTimeMillis());
        }
      }
      concurrentSnapshotUsers.get(topicName).get(partitionId).incrementAndGet();
      LOGGER.info(
          "Retrieved snapshot from {} with timestamp {}",
          fullPathForPartitionDBSnapshot,
          snapShotTimestamps.get(topicName).get(partitionId));
    }
  }

  public void decreaseConcurrentUserCount(String topicName, int partitionId) {
    Map<Integer, AtomicLong> concurrentPartitionUsers = concurrentSnapshotUsers.get(topicName);
    if (concurrentPartitionUsers == null) {
      throw new VeniceException("No topic found: " + topicName);
    }
    AtomicLong concurrentUsers = concurrentPartitionUsers.get(partitionId);
    if (concurrentUsers == null) {
      throw new VeniceException(String.format("%d partition not found on topic %s", partitionId, topicName));
    }
    try (AutoCloseableLock ignored = AutoCloseableLock.of(lock)) {
      long result = concurrentUsers.decrementAndGet();
      if (result < 0) {
        throw new VeniceException("Concurrent user count cannot be negative");
      }
    }
  }

  long getConcurrentSnapshotUsers(RocksDB rocksDB, String topicName, int partitionId) {
    if (rocksDB == null || topicName == null) {
      throw new IllegalArgumentException("RocksDB instance and topicName cannot be null");
    }
    if (!concurrentSnapshotUsers.containsKey(topicName)
        || !concurrentSnapshotUsers.get(topicName).containsKey(partitionId)) {
      return 0;
    }
    return concurrentSnapshotUsers.get(topicName).get(partitionId).get();
  }

  void setSnapShotTimestamps(Map<String, Map<Integer, Long>> snapShotTimestamps) {
    this.snapShotTimestamps = snapShotTimestamps;
  }

  /**
   * Checks if the current snapshot of the partition is stale
   */
  private boolean isSnapshotStale(String topicName, int partitionId) {
    if (!snapShotTimestamps.containsKey(topicName) || !snapShotTimestamps.get(topicName).containsKey(partitionId)) {
      return true;
    }
    return System.currentTimeMillis() - snapShotTimestamps.get(topicName).get(partitionId) > snapshotRetentionTime;
  }

  private boolean isStoreHybrid(String topicName) {
    Store store = readOnlyStoreRepository.getStore(topicName);
    if (store != null) {
      return store.isHybrid();
    }
    return false;
  }

  /**
   * Updates the snapshot of the hybrid store
   */
  private void updateHybridSnapshot(RocksDB rocksDB, String topicName, int partitionId) {
    String fullPathForPartitionDBSnapshot = RocksDBUtils.composeSnapshotDir(this.basePath, topicName, partitionId);
    File partitionSnapshotDir = new File(fullPathForPartitionDBSnapshot);
    if (partitionSnapshotDir.exists()) {
      if (!partitionSnapshotDir.delete()) {
        throw new VeniceException(
            "Failed to delete the existing snapshot directory: " + fullPathForPartitionDBSnapshot);
      }
      return;
    }
    try {
      Checkpoint checkpoint = createCheckpoint(rocksDB);

      LOGGER.info("Creating snapshots in directory: {}", fullPathForPartitionDBSnapshot);
      checkpoint.createCheckpoint(fullPathForPartitionDBSnapshot);
      LOGGER.info("Finished creating snapshots in directory: {}", fullPathForPartitionDBSnapshot);
    } catch (RocksDBException e) {
      throw new VeniceException(
          "Received exception during RocksDB's snapshot creation in directory " + fullPathForPartitionDBSnapshot,
          e);
    }
  }

  @VisibleForTesting
  protected Checkpoint createCheckpoint(RocksDB rocksDB) {
    return Checkpoint.create(rocksDB);
  }

  /**
   * util method to create a snapshot for batch only
   * It will check the snapshot directory and delete it if it exists, then generate a new snapshot
   */
  public static void createSnapshotForBatch(RocksDB rocksDB, String fullPathForPartitionDBSnapshot) {
    LOGGER.info("Creating snapshot for batch in directory: {}", fullPathForPartitionDBSnapshot);

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
      LOGGER.info("Start creating snapshots for batch in directory: {}", fullPathForPartitionDBSnapshot);

      Checkpoint checkpoint = Checkpoint.create(rocksDB);
      checkpoint.createCheckpoint(fullPathForPartitionDBSnapshot);

      LOGGER.info("Finished creating snapshots for batch in directory: {}", fullPathForPartitionDBSnapshot);
    } catch (RocksDBException e) {
      throw new VeniceException(
          "Received exception during RocksDB's snapshot creation in directory " + fullPathForPartitionDBSnapshot,
          e);
    }
  }
}
