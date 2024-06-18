package com.linkedin.davinci.blob;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.store.rocksdb.RocksDBUtils;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.Checkpoint;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;


public class BlobSnapshotManager {
  private final String basePath;

  @VisibleForTesting
  protected Checkpoint createCheckpoint(RocksDB rocksDB) {
    return Checkpoint.create(rocksDB);
  }

  private static final Logger LOGGER = LogManager.getLogger(BlobSnapshotManager.class);
  HashMap<String, Map<Integer, AtomicLong>> concurrentSnapshotUsers;
  HashMap<String, HashMap<Integer, Long>> snapShotTimestamps;
  Long snapshotRetentionTime;

  // Initialized the snapshot manger for this specific dbDir
  public BlobSnapshotManager(String basePath, long snapshotRetentionTime) {
    this.basePath = basePath;
    this.snapshotRetentionTime = snapshotRetentionTime;
    this.snapShotTimestamps = new HashMap<>();
    this.concurrentSnapshotUsers = new HashMap<>();
  }

  // Checks if the current snapshot of the partition is stale
  private boolean isSnapshotStale(String topicName, int partitionId) {
    if (snapShotTimestamps.get(topicName) == null || snapShotTimestamps.get(topicName).get(partitionId) == null) {
      return true;
    }
    long snapshotTimestamp = snapShotTimestamps.get(topicName).get(partitionId);
    return System.currentTimeMillis() - snapshotTimestamp > this.snapshotRetentionTime;
  }

  // Updates the snapshot of the hybdrid store
  private void updateHybridSnapshot(RocksDB rocksDB, String topicName, int partitionId) {
    String fullPathForPartitionDBSnapshot = RocksDBUtils.composeSnapshotDir(this.basePath, topicName, partitionId);
    if (fullPathForPartitionDBSnapshot.isEmpty()) {
      return;
    }
    try {
      Checkpoint checkpoint = createCheckpoint(rocksDB);

      LOGGER.info("Start creating snapshots in directory: {}", fullPathForPartitionDBSnapshot);
      checkpoint.createCheckpoint(fullPathForPartitionDBSnapshot);

      LOGGER.info("Finished creating snapshots in directory: {}", fullPathForPartitionDBSnapshot);
    } catch (RocksDBException e) {
      throw new VeniceException(
          "Received exception during RocksDB's snapshot creation in directory " + fullPathForPartitionDBSnapshot,
          e);
    }
  }

  // Checks if the snapshot is stale, if it is and no one is using it, it updates the snapshot, otherwise it increases
  // the count of people using the snapshot
  public void getHybridSnapshot(RocksDB rocksDB, String topicName, int partitionId) {
    if (rocksDB == null || topicName == null) {
      throw new IllegalArgumentException("RocksDB instance and topicName cannot be null");
    }

    String fullPathForPartitionDBSnapshot = RocksDBUtils.composeSnapshotDir(this.basePath, topicName, partitionId);
    if (!snapShotTimestamps.containsKey(topicName)) {
      snapShotTimestamps.put(topicName, new HashMap<>());
      concurrentSnapshotUsers.put(topicName, new HashMap<>());
    }
    if (isSnapshotStale(topicName, partitionId)) {
      if (concurrentSnapshotUsers.get(topicName).get(partitionId) == null
          || concurrentSnapshotUsers.get(topicName).get(partitionId).get() == 0) {
        updateHybridSnapshot(rocksDB, topicName, partitionId);
        snapShotTimestamps.get(topicName).put(partitionId, System.currentTimeMillis());
      }
      if (getConcurrentUsers(rocksDB, topicName, partitionId) == 0) {
        concurrentSnapshotUsers.get(topicName).put(partitionId, new AtomicLong(1));
      } else {
        concurrentSnapshotUsers.get(topicName).get(partitionId).incrementAndGet();
      }
      return;
    }
    try {
      if (concurrentSnapshotUsers.get(topicName).get(partitionId) != null
          && concurrentSnapshotUsers.get(topicName).get(partitionId).get() >= 0) {
        concurrentSnapshotUsers.get(topicName).get(partitionId).incrementAndGet();
      } else {
        concurrentSnapshotUsers.get(topicName).put(partitionId, new AtomicLong(1));
      }
      LOGGER.info("Retrieved preexisting snapshot in directory: {}", fullPathForPartitionDBSnapshot);
    } catch (Exception e) {
      throw new VeniceException("Error getting snapshot", e);
    }
  }

  public int getConcurrentUsers(RocksDB rocksDB, String topicName, int partitionId) {
    if (rocksDB == null || topicName == null) {
      throw new IllegalArgumentException("RocksDB instance and topicName cannot be null");
    }
    if (!concurrentSnapshotUsers.containsKey(topicName)) {
      return 0;
    }
    if (!concurrentSnapshotUsers.get(topicName).containsKey(partitionId)) {
      return 0;
    }
    return concurrentSnapshotUsers.get(topicName).get(partitionId).intValue();
  }
}
