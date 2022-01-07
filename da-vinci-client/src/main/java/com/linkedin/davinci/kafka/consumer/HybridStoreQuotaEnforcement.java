package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.exceptions.StorageQuotaExceededException;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.utils.StoragePartitionDiskUsage;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType.*;


/**
 * This class enforces quota for each partition in hybrid stores and listens to store changes
 *
 * For GF or hadoop push of a hybrid store, we kill the job if there is >= 1 partition exceeds the allocated partition-level quota
 * by throwing {@link StorageQuotaExceededException} and killing the job.
 *
 * For a Samza RT job,
 * if a partition exhausts the partition-level quota, we will stall the consumption for this partition.
 * i.e. stop polling and processing records for this partition until more space is available(quota gets bumped or db compactions shrink disk usage);
 * if only some partitions violates the quota, the job will pause these partitions while keep processing the other good partitions.
 *
 * Assumption: 1. every partition in a store shares similar size; 2. no write-compute messages/partial updates/incremental push
 */
public class HybridStoreQuotaEnforcement implements StoreDataChangedListener {

  private static final Logger logger = LogManager.getLogger(HybridStoreQuotaEnforcement.class);

  private final StoreIngestionTask storeIngestionTask;
  private final ConcurrentMap<Integer, PartitionConsumptionState> partitionConsumptionStateMap;
  private final AbstractStorageEngine storageEngine;
  private final String versionTopic;
  private final String storeName;
  private final int storePartitionCount;
  private final Map<Integer, StoragePartitionDiskUsage> partitionConsumptionSizeMap;
  private final Set<Integer> pausedPartitions;

  private boolean versionIsOnline;

  /**
   * These fields are subject to changes in #handleStoreChanged
   */
  private long storeQuotaInBytes;
  private long diskQuotaPerPartition;
  private boolean isHybridStoreDiskQuotaEnabled;
  // protects isHybridStoreDiskQuotaEnabled.
  private final Lock lock = new ReentrantLock();

  public HybridStoreQuotaEnforcement(StoreIngestionTask storeIngestionTask, AbstractStorageEngine storageEngine,
      Store store, String versionTopic, int storePartitionCount,
      Map<Integer, StoragePartitionDiskUsage> partitionConsumptionSizeMap,
      ConcurrentMap<Integer, PartitionConsumptionState> partitionConsumptionStateMap) {
    this.storeIngestionTask = storeIngestionTask;
    this.partitionConsumptionStateMap = partitionConsumptionStateMap;
    this.storageEngine = storageEngine;
    this.storeName = store.getName();
    this.versionTopic = versionTopic;
    this.storePartitionCount = storePartitionCount;
    this.partitionConsumptionSizeMap = partitionConsumptionSizeMap;
    this.pausedPartitions = new HashSet<>();
    this.storeQuotaInBytes = store.getStorageQuotaInByte();
    this.diskQuotaPerPartition = this.storeQuotaInBytes / this.storePartitionCount;
    int storeVersion = Version.parseVersionFromKafkaTopicName(versionTopic);
    this.isHybridStoreDiskQuotaEnabled = store.isHybridStoreDiskQuotaEnabled();
    Optional<Version> version = store.getVersion(storeVersion);
    checkVersionIsOnline(version);
  }

  @Override
  public void handleStoreCreated(Store store) {
    // no-op
  }

  @Override
  public void handleStoreDeleted(String storeName) {
    // np-op
  }

  private boolean isHybridStoreDiskQuotaUpdated(Store newStore) {
    return isHybridStoreDiskQuotaEnabled != newStore.isHybridStoreDiskQuotaEnabled()
        || storeQuotaInBytes != newStore.getStorageQuotaInByte();
  }

  /**
   * This function updates the per-partition HybridStoreQuotaStatus based on the most recent settings.
   * It uses a zeroed partition-to-size map to report the current hybrid store quota status for each partition without
   * introducing any side effect.
   */
  private void computeAndUpdatePartitionQuotaStatus() {
    Map<Integer, Integer> partitionSize = new HashMap<>();
    for (int partition : partitionConsumptionStateMap.keySet()) {
      partitionSize.put(partition, 0);
    }

    // checkPartitionQuota needs to be thread safe, as it is called in multiple threads.
    checkPartitionQuota(partitionSize);
  }

  /**
   * This function reports QUOTA_NOT_VIOLATED for all partitions of the topic.
   */
  private void reportStoreQuotaNotViolated() {
    for (int partition : partitionConsumptionStateMap.keySet()) {
      storeIngestionTask.reportQuotaNotViolated(partition);
    }
  }

  @Override
  public void handleStoreChanged(Store store) {
    if (!store.getName().equals(storeName)) {
      return;
    }
    int storeVersion = Version.parseVersionFromKafkaTopicName(versionTopic);
    Optional<Version> version = store.getVersion(storeVersion);
    checkVersionIsOnline(version);
    if (this.storeQuotaInBytes != store.getStorageQuotaInByte() || !store.isHybridStoreDiskQuotaEnabled()) {
      logger.info("Store: " + this.storeName + " changed, updated quota from " + this.storeQuotaInBytes
          + " to " + store.getStorageQuotaInByte() + " and store quota is " + (store.isHybridStoreDiskQuotaEnabled() ? "": "not ")
          + "enabled, so we reset the store quota and resume all partitions.");
      resumeAllPartitions();
    }

    try (AutoCloseableLock ignored = new AutoCloseableLock(lock)) {
      boolean isHybridQuotaUpdated = isHybridStoreDiskQuotaUpdated(store);
      boolean isHybridQuotaChangedToDisabled = isHybridStoreDiskQuotaEnabled && !store.isHybridStoreDiskQuotaEnabled();

      this.storeQuotaInBytes = store.getStorageQuotaInByte();
      this.diskQuotaPerPartition = this.storeQuotaInBytes / this.storePartitionCount;
      this.isHybridStoreDiskQuotaEnabled = store.isHybridStoreDiskQuotaEnabled();

      if (isHybridQuotaChangedToDisabled) {
        // A store is changed from disk quota enabled to disabled, mark all its partitions as not violated.
        reportStoreQuotaNotViolated();
      } else if (isHybridQuotaUpdated) {
        // For other cases that disk quota gets updated, recompute all partition quota status and report if there is a change.
        computeAndUpdatePartitionQuotaStatus();
      }
    }
  }

  /**
   * Enforce partition level quota for the map.
   * This function could be invoked by multiple threads when shared consumer is being used.
   * Check {@link StoreIngestionTask#produceToStoreBufferServiceOrKafka} and {@link StoreIngestionTask#processMessages}
   * to find more details.
   *
   * @param subscribedPartitionToSize with partition id as key and batch records size as value
   */
  protected void checkPartitionQuota(Map<Integer, Integer> subscribedPartitionToSize) {
    try (AutoCloseableLock ignored = new AutoCloseableLock(lock)) {
      if (!isHybridStoreDiskQuotaEnabled) {
        logger.warn("Skip checking and reporting storage quota status");
        return;
      }

      for (Map.Entry<Integer, Integer> curr : subscribedPartitionToSize.entrySet()) {
        enforcePartitionQuota(curr.getKey(), curr.getValue());
      }
    }
  }

  /**
   * Check if this partition violates the partition-level quota or not, if this does and it's RT job, pause the
   * partition; if it's GF job or hadoop push for a hybrid store, throw exception
   * @param partition
   * @param recordSize
   * @throws StorageQuotaExceededException
   */
  private void enforcePartitionQuota(int partition, int recordSize) {
    if (!partitionConsumptionSizeMap.containsKey(partition)) {
      partitionConsumptionSizeMap.put(partition, new StoragePartitionDiskUsage(partition, storageEngine));
    }

    partitionConsumptionSizeMap.get(partition).add(recordSize);
    PartitionConsumptionState pcs = null;
    if (partitionConsumptionStateMap.containsKey(partition)) {
      pcs = partitionConsumptionStateMap.get(partition);
    }

    String consumingTopic = getConsumingTopic(pcs);
    String msgIdentifier = consumingTopic + "_" + partition + "_quota_exceeded";
    // Log quota exceeded info only once a minute per partition.
    boolean shouldLogQuotaExceeded = !storeIngestionTask.REDUNDANT_LOGGING_FILTER.isRedundantException(msgIdentifier);
    /**
     * Check if the current partition violates the partition-level quota.
     * It's possible to pause an already-paused partition or resume an un-paused partition. The reason that
     * we don't prevent this is that when a SN gets restarted, the in-memory paused partitions are empty. If
     * we check if the partition is in paused partitions to decide whether to resume it, we may never resume it.
     */
    if (isStorageQuotaExceeded(partition, shouldLogQuotaExceeded)) {
      storeIngestionTask.reportQuotaViolated(partition);

      /**
       * If the version is already online but the completion has not been reported, we directly
       * report online for this replica.
       * Otherwise it could induce error replicas during rebalance for online version.
       */
      if (isVersionOnline() && pcs != null && !pcs.isCompletionReported()) {
        storeIngestionTask.getReportStatusAdapter().reportCompleted(pcs);
      }

      /**
       * For GF job or real-time job of a hybrid store.
       *
       * TODO: Do a force-refresh of store metadata to get latest state before kill the job
       * We might have potential race condition: The store version is updated to be ONLINE and become CURRENT in Controller,
       * but the notification to storage node gets delayed, the quota exceeding issue will put this partition of current version to be ERROR,
       * which will break the production.
       */
      pausePartition(partition, consumingTopic);
      if (shouldLogQuotaExceeded) {
        logger.info("Quota exceeded for store " + storeName + " partition " + partition + ", paused this partition." + versionTopic);
      }
    } else { /** we have free space for this partition */
      /**
       *  Paused partitions could be resumed
       */
      storeIngestionTask.reportQuotaNotViolated(partition);
      if (isPartitionPausedIngestion(partition)) {
        resumePartition(partition, consumingTopic);
        logger.info("Quota available for store " + storeName + " partition " + partition + ", resumed this partition.");
      }
    }
  }

  /**
   * Compare the partition's usage with partition-level hybrid quota
   * @param partition
   * @param shouldLogQuotaExceeded Log quota exceeded warning only once a minute per partition.
   * @return true if the quota is exceeded for given partition
   */
  private boolean isStorageQuotaExceeded(int partition, boolean shouldLogQuotaExceeded) {
    double diskUsagePerPartition = partitionConsumptionSizeMap.get(partition).getUsage();
    /**
     * emit metrics for the ratio of (partition usage/partition quota)
     */
    // TODO: optimize the metrics
    if (storeIngestionTask.isMetricsEmissionEnabled()) {
      storeIngestionTask.storeIngestionStats.recordStorageQuotaUsed(storeName,
          diskQuotaPerPartition > 0 ? (diskUsagePerPartition / diskQuotaPerPartition) : 0);
    }
    if (storeQuotaInBytes != Store.UNLIMITED_STORAGE_QUOTA && diskUsagePerPartition >= diskQuotaPerPartition) {
      if (shouldLogQuotaExceeded) {
        logger.warn(storeIngestionTask.consumerTaskId + " exceeded the storage quota " + diskQuotaPerPartition);
      }
      return true;
    }
    return false;
  }

  /**
   * After the partition gets paused, consumer.poll() in {@link StoreIngestionTask} won't return any records from this
   * partition without affecting partition subscription
   */
  private void pausePartition(int partition, String consumingTopic) {
    this.storeIngestionTask.getConsumers().forEach(consumer -> consumer.pause(consumingTopic, partition));
    this.pausedPartitions.add(partition);
  }

  private void resumePartition(int partition, String consumingTopic) {
    this.storeIngestionTask.getConsumers().forEach(consumer -> consumer.resume(consumingTopic, partition));
    this.pausedPartitions.remove(partition);
  }

  private void resumeAllPartitions() {
    partitionConsumptionStateMap.forEach((key, value) -> {
      String consumingTopic = getConsumingTopic(value);
      resumePartition(key, consumingTopic);
    });
  }

  private void checkVersionIsOnline(Optional<Version> version) {
    if (!version.isPresent()) {
      int storeVersion = Version.parseVersionFromKafkaTopicName(versionTopic);
      throw new VeniceException("Version: " + storeVersion + " doesn't exist in store: " + storeName);
    } else if (version.get().getStatus().equals(VersionStatus.ONLINE)) {
      versionIsOnline = true;
    }
  }

  /**
   * Check the topic which is currently consumed topic for this partition
   */
  private String getConsumingTopic(PartitionConsumptionState pcs) {
    String consumingTopic = versionTopic;
    if (pcs != null && pcs.getLeaderFollowerState().equals(LEADER)) {
      OffsetRecord offsetRecord = pcs.getOffsetRecord();
      if (offsetRecord.getLeaderTopic() != null) {
        consumingTopic = offsetRecord.getLeaderTopic();
      }
    }
    return consumingTopic;
  }

  protected boolean isPartitionPausedIngestion(int partition) {
    return pausedPartitions.contains(partition);
  }

  public boolean hasPausedPartitionIngestion() {
    return !pausedPartitions.isEmpty();
  }

  protected long getStoreQuotaInBytes() {
    return storeQuotaInBytes;
  }

  protected long getPartitionQuotaInBytes() {
    return diskQuotaPerPartition;
  }

  protected boolean isVersionOnline() {
    return versionIsOnline;
  }
}