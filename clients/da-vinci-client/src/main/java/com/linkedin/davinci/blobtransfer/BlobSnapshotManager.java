package com.linkedin.davinci.blobtransfer;

import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.AbstractStoragePartition;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
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
  // A map to keep track of the number of hosts using a snapshot for a particular topic and partition
  private final Map<String, Map<Integer, AtomicLong>> concurrentSnapshotUsers;

  private final ReadOnlyStoreRepository readOnlyStoreRepository;
  private final StorageEngineRepository storageEngineRepository;
  private final Lock lock = new ReentrantLock();
  private static final Logger LOGGER = LogManager.getLogger(BlobSnapshotManager.class);

  /**
   * Constructor for the BlobSnapshotManager
   */
  public BlobSnapshotManager(
      ReadOnlyStoreRepository readOnlyStoreRepository,
      StorageEngineRepository storageEngineRepository) {
    this.readOnlyStoreRepository = readOnlyStoreRepository;
    this.storageEngineRepository = storageEngineRepository;

    this.concurrentSnapshotUsers = new VeniceConcurrentHashMap<>();
  }

  /**
   * Recreate a snapshot for a hybrid store and return true if the snapshot is successfully created
   * If the snapshot is being used by some other host, throw an exception
   */
  public boolean recreateSnapshotForHybrid(BlobTransferPayload payload) {
    String topicName = payload.getTopicName();
    String storeName = payload.getStoreName();
    int partitionId = payload.getPartition();

    if (!isStoreHybrid(storeName)) {
      LOGGER.info("Store {} is not hybrid, skipping snapshot re-create", storeName);
      return false;
    }

    concurrentSnapshotUsers.putIfAbsent(topicName, new VeniceConcurrentHashMap<>());
    concurrentSnapshotUsers.get(topicName).putIfAbsent(partitionId, new AtomicLong(0));

    try (AutoCloseableLock ignored = AutoCloseableLock.of(lock)) {
      if (concurrentSnapshotUsers.get(topicName).get(partitionId) == null
          || concurrentSnapshotUsers.get(topicName).get(partitionId).get() == 0) {
        createSnapshot(topicName, partitionId);
      } else {
        // the snapshot is being used, cannot generate a new snapshot at the same time
        String errorMessage = String.format(
            "Snapshot is being used by some hosts, cannot recreate new snapshot for topic %s partition %d",
            topicName,
            partitionId);
        LOGGER.error(errorMessage);
        throw new VeniceException(errorMessage);
      }
    }

    concurrentSnapshotUsers.get(topicName).get(partitionId).incrementAndGet();
    return true;
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
    try (AutoCloseableLock ignored = AutoCloseableLock.of(lock)) {
      long result = concurrentUsers.decrementAndGet();
      if (result < 0) {
        throw new VeniceException("Concurrent user count cannot be negative");
      }
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
  protected boolean isStoreHybrid(String storeName) {
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
}
