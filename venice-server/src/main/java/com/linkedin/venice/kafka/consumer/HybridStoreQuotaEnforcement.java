package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.exceptions.StorageQuotaExceededException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.utils.StoragePartitionDiskUsage;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.Map;
import org.apache.log4j.Logger;

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

  private static final Logger logger = Logger.getLogger(HybridStoreQuotaEnforcement.class);

  private final StoreIngestionTask storeIngestionTask;
  private final AbstractStorageEngine storageEngine;
  private final String topic;
  private final String storeName;
  private final int storePartitionCount;

  /**
   * RT Job refers to the Samza job after batch/GF job finishes.
   * These fields are for RT job; when a samza RT partition exhausted the hybrid quota, we
   * stall the partition.
   */
  private boolean isRTJob;
  private final Map<Integer, StoragePartitionDiskUsage> partitionConsumptionSizeMap;
  private final Set<Integer> pausedPartitions;

  /**
   * These fields are subject to changes in #handleStoreChanged
   */
  private long storeQuotaInBytes;
  private long diskQuotaPerPartition;

  public HybridStoreQuotaEnforcement(StoreIngestionTask storeIngestionTask, AbstractStorageEngine storageEngine, Store store, String topic, int storePartitionCount) {
    this.storeIngestionTask = storeIngestionTask;
    this.storageEngine = storageEngine;
    this.storeName = store.getName();
    this.topic = topic;
    this.storePartitionCount = storePartitionCount;
    this.partitionConsumptionSizeMap = new HashMap<>();
    this.pausedPartitions = new HashSet<>();
    this.storeQuotaInBytes = store.getStorageQuotaInByte();
    this.diskQuotaPerPartition = this.storeQuotaInBytes / this.storePartitionCount;
    int storeVersion = Version.parseVersionFromKafkaTopicName(topic);
    Optional<Version> version = store.getVersion(storeVersion);
    setRTJob(version);
  }

  @Override
  public void handleStoreCreated(Store store) {
    // no-op
  }

  @Override
  public void handleStoreDeleted(String storeName) {
    // np-op
  }

  @Override
  public void handleStoreChanged(Store store) {
    if (store.getName() != storeName) {
      return;
    }
    int storeVersion = Version.parseVersionFromKafkaTopicName(topic);
    Optional<Version> version = store.getVersion(storeVersion);
    setRTJob(version);
    this.storeQuotaInBytes = store.getStorageQuotaInByte();
    this.diskQuotaPerPartition = this.storeQuotaInBytes / this.storePartitionCount;
  }

  /**
   * Enforce partition level quota for the map.
   * This function could be inovked by multiple threads when shared consumer is being used.
   * Check {@link StoreIngestionTask#produceToStoreBufferService} and {@link StoreIngestionTask#processMessages}
   * to find more details.
   *
   * @param subscribedPartitionToSize with partition id as key and batch records size as value
   */
  protected synchronized void checkPartitionQuota(Map<Integer, Integer> subscribedPartitionToSize) {
    for (Map.Entry<Integer, Integer> curr : subscribedPartitionToSize.entrySet()) {
      enforcePartitionQuota(curr.getKey(), curr.getValue());
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

    /**
     * check if the current partition violates the partition-level quota.
     * It's possible to pause an already-paused partition or resume an un-paused partition. The reason that
     * we don't prevent this is that when a SN gets restarted, the in-memory paused partitions are empty. If
     * we check if the partition is in paused partitions to decide whether to resume it, we may never resume it.
     */
    if (isStorageQuotaExceeded(partition)) {
      if (isRTJob()) {
        /**
         * Pause the partition for RT job.
         */
        pausePartition(partition);
        logger.info("Quota exceeded for store " + storeName + " partition " + partition + ", paused this partition.");
      } else {
        /**
         * GF job or hadoop push for a hybrid store, directly throw exception
         *
         * TODO: Do a force-refresh of store metadata to get latest state before kill the job
         * We might have potential race condition: The store version is updated to be ONLINE and become CURRENT in Controller,
         * but the notification to storage node gets delayed, the quota exceeding issue will put this partition of current version to be ERROR,
         * which will break the production.
         */
        throw new StorageQuotaExceededException(storeName,
                                                partition,
                                                partitionConsumptionSizeMap.get(partition).getUsage(),
                                                diskQuotaPerPartition);
      }
    } else { /** we have free space for this partition */
      /**
       * Only RT jobs partitions could be resumed
       */
      if (isRTJob() && isPartitionPausedIngestion(partition)) {
        resumePartition(partition);
        logger.info("Quota available for store " + storeName + " partition " + partition + ", resumed this partition.");
      }
    }
  }

  /**
   * Compare the partition's usage with partition-level hybrid quota
   * @param partition
   * @return true if the quota is exceeded for given partition
   */
  private boolean isStorageQuotaExceeded(int partition) {
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
      logger.warn(storeIngestionTask.consumerTaskId + " exceeded the storage quota " + diskQuotaPerPartition);
      return true;
    }
    return false;
  }

  /**
   * After the partition gets paused, consumer.poll() in {@link StoreIngestionTask} won't return any records from this
   * partition without affecting partition subscription
   */
  private void pausePartition(int partition) {
    this.storeIngestionTask.getConsumer().pause(topic, partition);
    this.pausedPartitions.add(partition);
  }

  private void resumePartition(int partition) {
    this.storeIngestionTask.getConsumer().resume(topic, partition);
    this.pausedPartitions.remove(partition);
  }

  private void setRTJob(Optional<Version> version) {
    if (!version.isPresent()) {
      int storeVersion = Version.parseVersionFromKafkaTopicName(topic);
      throw new VeniceException("Version: " + storeVersion + " doesn't exist in store: " + storeName);
    } else if (version.get().getStatus().equals(VersionStatus.ONLINE)) {
      isRTJob = true;
    }
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

  protected boolean isRTJob() {
    return isRTJob;
  }
}
