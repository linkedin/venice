package com.linkedin.davinci.blob;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.store.rocksdb.RocksDBUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import java.io.File;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.Checkpoint;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;


public class BlobSnapshotManager {
  private final Map<String, Map<Integer, AtomicLong>> concurrentSnapshotUsers;
  private Map<String, Map<Integer, Long>> snapShotTimestamps;
  private final long snapshotRetentionTime;
  private final String basePath;
  private final Lock lock = new ReentrantLock();
  private static final Logger LOGGER = LogManager.getLogger(BlobSnapshotManager.class);

  /**
   * Constructor for the BlobSnapshotManager
   */
  public BlobSnapshotManager(String basePath, long snapshotRetentionTime) {
    this.basePath = basePath;
    this.snapshotRetentionTime = snapshotRetentionTime;
    this.snapShotTimestamps = new VeniceConcurrentHashMap<>();
    this.concurrentSnapshotUsers = new VeniceConcurrentHashMap<>();
  }

  public void setSnapShotTimestamps(Map<String, Map<Integer, Long>> snapShotTimestamps) {
    this.snapShotTimestamps = snapShotTimestamps;
  }

  /**
   * Checks if the snapshot is stale, if it is and no one is using it, it updates the snapshot,
   * otherwise it increases the count of people using the snapshot
   */
  public void maybeUpdateHybridSnapshot(RocksDB rocksDB, String topicName, int partitionId) {
    if (rocksDB == null || topicName == null) {
      throw new IllegalArgumentException("RocksDB instance and topicName cannot be null");
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

  /**
   * Checks if the current snapshot of the partition is stale
   */
  private boolean isSnapshotStale(String topicName, int partitionId) {
    if (!snapShotTimestamps.containsKey(topicName) || !snapShotTimestamps.get(topicName).containsKey(partitionId)) {
      return true;
    }
    return System.currentTimeMillis() - snapShotTimestamps.get(topicName).get(partitionId) > snapshotRetentionTime;
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

}
