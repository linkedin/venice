package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType.LEADER;
import static com.linkedin.venice.utils.RedundantExceptionFilter.getRedundantExceptionFilter;

import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.utils.StoragePartitionDiskUsage;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class has the following responsibilities:
 *
 * 1. Keep track of storage utilization, at least on disk, and potentially also in-memory (when hybrid quota
 *    enforcement is enabled)..
 * 2. Take action (pause/resume) on the associated consumer if storage quota is breached (and enforcement is
 *    enabled).
 * 3. Listen to store config changes related to quota limits and whether enforcement is enabled, and react
 *    accordingly.
 * 4: Report replica status changes if the above actions affect them.
 *      TODO: Consider whether this is tech debt and if we could/should decouple status reporting from this class.
 *            This would allow us to stop passing in the {@link #ingestionNotificationDispatcher} which in turn may allow us
 *            to stop mutating the entries in {@link #partitionConsumptionStateMap} (in which case, we could pass
 *            a map where the values are a read-only interface implemented by {@link PartitionConsumptionState}
 *            and thus preventing mutations of this state from here).
 *
 * For a Samza RT job, if a partition exhausts the partition-level quota, we will stall the consumption for
 * this partition. i.e. stop polling and processing records for this partition until more space is available
 * (quota gets bumped or db compactions shrink disk usage); if only some partitions violates the quota, the
 * job will pause these partitions while keep processing the other good partitions.
 *
 * Assumptions:
 *
 * 1. every partition in a store shares similar size;
 * 2. no write-compute messages/partial updates/incremental push
 */
public class StorageUtilizationManager implements StoreDataChangedListener {
  private static final Logger LOGGER = LogManager.getLogger(StorageUtilizationManager.class);
  private static final RedundantExceptionFilter REDUNDANT_LOGGING_FILTER = getRedundantExceptionFilter();

  private final Map<Integer, PartitionConsumptionState> partitionConsumptionStateMap;
  private final AbstractStorageEngine storageEngine;
  private final Function<Integer, StoragePartitionDiskUsage> storagePartitionDiskUsageFunctionConstructor;
  private final String versionTopic;
  private final String storeName;
  private final int storeVersion;
  private final int partitionCount;
  private final Map<Integer, StoragePartitionDiskUsage> partitionConsumptionSizeMap;
  private final Set<Integer> pausedPartitions;
  private final boolean isHybridQuotaEnabledInServer;
  private final boolean isServerCalculateQuotaUsageBasedOnPartitionsAssignmentEnabled;
  private final boolean isSeparateRealtimeTopicEnabled;
  private final IngestionNotificationDispatcher ingestionNotificationDispatcher;
  private final TopicPartitionConsumerFunction pausePartition;
  private final TopicPartitionConsumerFunction resumePartition;

  private boolean versionIsOnline;

  /** Should only be changed in {@link #setStoreQuota(Store)} */
  private long storeQuotaInBytes;

  /** Should only be changed in {@link #setStoreQuota(Store)} */
  private long diskQuotaPerPartition;

  /** Should only be changed in {@link #setStoreQuota(Store)} */
  private boolean isHybridQuotaEnabledInStoreConfig;

  /** Protects {@link #isHybridQuotaEnabledInStoreConfig} */
  private final Lock hybridStoreDiskQuotaLock = new ReentrantLock();

  /**
   * @param store a snapshot of the {@link Store} associated with this manager at the time of construction
   *              N.B.: It's important not to hang on to a reference of this param since it will become
   *              stale when the store config changes. Refreshing the things we need out of the store object
   *              is handled in {@link #handleStoreChanged(Store)}.
   */
  public StorageUtilizationManager(
      AbstractStorageEngine storageEngine,
      Store store,
      String versionTopic,
      int partitionCount,
      Map<Integer, PartitionConsumptionState> partitionConsumptionStateMap,
      boolean isHybridQuotaEnabledInServer,
      boolean isServerCalculateQuotaUsageBasedOnPartitionsAssignmentEnabled,
      boolean isSeparateRealtimeTopicEnabled,
      IngestionNotificationDispatcher ingestionNotificationDispatcher,
      TopicPartitionConsumerFunction pausePartition,
      TopicPartitionConsumerFunction resumePartition) {
    this.partitionConsumptionStateMap = partitionConsumptionStateMap;
    this.storageEngine = storageEngine;
    this.storagePartitionDiskUsageFunctionConstructor =
        partition -> new StoragePartitionDiskUsage(partition, storageEngine);
    this.storeName = store.getName();
    this.versionTopic = versionTopic;
    if (partitionCount <= 0) {
      throw new IllegalArgumentException("PartitionCount must be positive!");
    }
    this.partitionCount = partitionCount;
    this.partitionConsumptionSizeMap = new VeniceConcurrentHashMap<>();
    this.pausedPartitions = VeniceConcurrentHashMap.newKeySet();
    this.storeVersion = Version.parseVersionFromKafkaTopicName(versionTopic);
    this.isHybridQuotaEnabledInServer = isHybridQuotaEnabledInServer;
    this.isServerCalculateQuotaUsageBasedOnPartitionsAssignmentEnabled =
        isServerCalculateQuotaUsageBasedOnPartitionsAssignmentEnabled;
    this.isSeparateRealtimeTopicEnabled = isSeparateRealtimeTopicEnabled;
    this.ingestionNotificationDispatcher = ingestionNotificationDispatcher;
    this.pausePartition = pausePartition;
    this.resumePartition = resumePartition;
    setStoreQuota(store);
    Version version = store.getVersion(storeVersion);
    versionIsOnline = isVersionOnline(version);
  }

  @Override
  public void handleStoreCreated(Store store) {
    // no-op
  }

  @Override
  public void handleStoreDeleted(String storeName) {
    // np-op
  }

  private boolean isHybridQuotaEnabled() {
    return isHybridQuotaEnabledInStoreConfig && isHybridQuotaEnabledInServer;
  }

  private boolean isHybridStoreDiskQuotaUpdated(Store newStore) {
    return isHybridQuotaEnabledInStoreConfig != newStore.isHybridStoreDiskQuotaEnabled()
        || storeQuotaInBytes != newStore.getStorageQuotaInByte();
  }

  /**
   * This function reports QUOTA_NOT_VIOLATED for all partitions of the topic.
   */
  private void reportStoreQuotaNotViolated() {
    for (PartitionConsumptionState partitionConsumptionState: partitionConsumptionStateMap.values()) {
      ingestionNotificationDispatcher.reportQuotaNotViolated(partitionConsumptionState);
    }
  }

  private void setStoreQuota(Store store) {
    this.storeQuotaInBytes = store.getStorageQuotaInByte();
    this.diskQuotaPerPartition = this.storeQuotaInBytes / this.partitionCount;
    this.isHybridQuotaEnabledInStoreConfig = store.isHybridStoreDiskQuotaEnabled();
  }

  @Override
  public void handleStoreChanged(Store store) {
    if (!store.getName().equals(storeName)) {
      return;
    }
    Version version = store.getVersion(storeVersion);
    if (version == null) {
      LOGGER.debug(
          "Version: {}  doesn't exist in the store: {}",
          Version.parseVersionFromKafkaTopicName(versionTopic),
          storeName);
      return;
    }
    versionIsOnline = isVersionOnline(version);
    if (this.storeQuotaInBytes != store.getStorageQuotaInByte() || !store.isHybridStoreDiskQuotaEnabled()) {
      LOGGER.info(
          "Store: {} changed, updated quota from {} to {} and store quota is {}enabled, "
              + "so we reset the store quota and resume all partitions.",
          this.storeName,
          this.storeQuotaInBytes,
          store.getStorageQuotaInByte(),
          store.isHybridStoreDiskQuotaEnabled() ? "" : "not ");
      resumeAllPartitions();
    }

    try (AutoCloseableLock ignored = AutoCloseableLock.of(hybridStoreDiskQuotaLock)) {
      boolean isHybridQuotaUpdated = isHybridStoreDiskQuotaUpdated(store);
      boolean oldHybridQuotaEnabledState = isHybridQuotaEnabled();
      setStoreQuota(store);
      boolean isHybridQuotaChangedToDisabled = oldHybridQuotaEnabledState && !isHybridQuotaEnabled();

      if (isHybridQuotaChangedToDisabled) {
        // A store is changed from disk quota enabled to disabled, mark all its partitions as not violated.
        reportStoreQuotaNotViolated();
      } else if (isHybridQuotaUpdated) {
        // For other cases that disk quota gets updated, recompute all partition quota status and report if there is a
        // change.
        checkAllPartitionsQuota();
      }
    }
  }

  public void initPartition(int partition) {
    partitionConsumptionSizeMap.put(partition, new StoragePartitionDiskUsage(partition, storageEngine));
  }

  public void removePartition(int partition) {
    partitionConsumptionSizeMap.remove(partition);
  }

  /**
   * Enforce partition level quota for the map.
   * This function could be invoked by multiple threads when shared consumer is being used.
   * Check {@link StoreIngestionTask#produceToStoreBufferServiceOrKafka} and {@link StoreIngestionTask#checkIngestionProgress}
   * to find more details.
   */
  public void checkAllPartitionsQuota() {
    try (AutoCloseableLock ignored = AutoCloseableLock.of(hybridStoreDiskQuotaLock)) {
      if (!isHybridQuotaEnabled()) {
        return;
      }

      for (Map.Entry<Integer, PartitionConsumptionState> entry: partitionConsumptionStateMap.entrySet()) {
        // We pass zero to evaluate the quota without adding more quota usage
        enforcePartitionQuota(entry.getKey(), entry.getValue(), 0);
      }
    }
  }

  public void enforcePartitionQuota(int partition, long additionalRecordSizeUsed) {
    try (AutoCloseableLock ignored = AutoCloseableLock.of(hybridStoreDiskQuotaLock)) {
      if (!isHybridQuotaEnabled()) {
        return;
      }
      enforcePartitionQuota(partition, partitionConsumptionStateMap.get(partition), additionalRecordSizeUsed);
    }
  }

  /**
   * Check if this partition violates the partition-level quota or not, if this does and it's RT job, pause the
   * partition; if it's a reprocessing job or hadoop push for a hybrid store, throw exception
   * @param partition
   * @param additionalRecordSizeUsed
   */
  private void enforcePartitionQuota(int partition, PartitionConsumptionState pcs, long additionalRecordSizeUsed) {
    Objects.requireNonNull(pcs, "The PartitionConsumptionState param cannot be null.");
    StoragePartitionDiskUsage storagePartitionDiskUsage =
        partitionConsumptionSizeMap.computeIfAbsent(partition, storagePartitionDiskUsageFunctionConstructor);

    storagePartitionDiskUsage.add(additionalRecordSizeUsed);

    /**
     * Check if the current partition violates the partition-level quota.
     * It's possible to pause an already-paused partition or resume an un-paused partition. The reason that
     * we don't prevent this is that when a SN gets restarted, the in-memory paused partitions are empty. If
     * we check if the partition is in paused partitions to decide whether to resume it, we may never resume it.
     */
    if (isStorageQuotaExceeded(storagePartitionDiskUsage)) {
      ingestionNotificationDispatcher.reportQuotaViolated(pcs);

      /**
       * If the version is already online but the completion has not been reported, we directly
       * report online for this replica.
       * Otherwise, it could induce error replicas during rebalance for online version.
       */
      if (isVersionOnline() && !pcs.isCompletionReported()) {
        ingestionNotificationDispatcher.reportCompleted(pcs);
      }

      /**
       * For reprocessing job or real-time job of a hybrid store.
       *
       * TODO: Do a force-refresh of store metadata to get latest state before kill the job
       * We might have potential race condition: The store version is updated to be ONLINE and become CURRENT in Controller,
       * but the notification to storage node gets delayed, the quota exceeding issue will put this partition of current version to be ERROR,
       * which will break the production.
       */
      for (String consumingTopic: getConsumingTopics(pcs)) {
        pausePartition(partition, consumingTopic);
        String msgIdentifier = consumingTopic + "_" + partition + "_quota_exceeded";
        // Log quota exceeded info only once a minute per partition.
        boolean shouldLogQuotaExceeded = !REDUNDANT_LOGGING_FILTER.isRedundantException(msgIdentifier);
        if (shouldLogQuotaExceeded) {
          LOGGER.info(
              "Quota exceeded for store version {} partition {}, paused this partition. Partition disk usage: {} >= partition quota: {}",
              versionTopic,
              partition,
              storagePartitionDiskUsage.getUsage(),
              diskQuotaPerPartition);
        }
      }
    } else {
      /**
       *  We have free space for this partition, paused partitions could be resumed
       */
      ingestionNotificationDispatcher.reportQuotaNotViolated(pcs);
      if (isPartitionPausedIngestion(partition)) {
        for (String consumingTopic: getConsumingTopics(pcs)) {
          resumePartition(partition, consumingTopic);
        }
        LOGGER.info("Quota available for store {} partition {}, resumed this partition.", storeName, partition);
      }
    }
  }

  /**
   * Compare the partition's usage with partition-level hybrid quota
   *
   * @param partitionDiskUsage for which to check if the quota was exceeded
   * @return true if the quota is exceeded for given partition
   */
  private boolean isStorageQuotaExceeded(StoragePartitionDiskUsage partitionDiskUsage) {
    return partitionDiskUsage.getUsage() >= diskQuotaPerPartition && storeQuotaInBytes != Store.UNLIMITED_STORAGE_QUOTA;
  }

  /**
   * After the partition gets paused, consumer.poll() in {@link StoreIngestionTask} won't return any records from this
   * partition without affecting partition subscription
   */
  private void pausePartition(int partition, String consumingTopic) {
    pausePartition.execute(consumingTopic, partition);
    this.pausedPartitions.add(partition);
  }

  private void resumePartition(int partition, String consumingTopic) {
    resumePartition.execute(consumingTopic, partition);
    this.pausedPartitions.remove(partition);
  }

  private void resumeAllPartitions() {
    partitionConsumptionStateMap.forEach((key, value) -> {
      for (String consumingTopic: getConsumingTopics(value)) {
        resumePartition(key, consumingTopic);
      }
    });
  }

  private boolean isVersionOnline(Version version) {
    return version != null && version.getStatus().equals(VersionStatus.ONLINE);
  }

  /**
   * Check the topic which is currently consumed topic for this partition
   */
  private List<String> getConsumingTopics(PartitionConsumptionState pcs) {
    List<String> consumingTopics = Collections.singletonList(versionTopic);

    if (pcs.getLeaderFollowerState().equals(LEADER)) {
      OffsetRecord offsetRecord = pcs.getOffsetRecord();
      if (offsetRecord.getLeaderTopic() != null) {
        consumingTopics = new ArrayList<>();
        consumingTopics.add(offsetRecord.getLeaderTopic());
        // For separate RT topic enabled SIT, we should include separate RT topic, if leader topic is a RT topic.
        if (isSeparateRealtimeTopicEnabled && Version.isRealTimeTopic(offsetRecord.getLeaderTopic())) {
          consumingTopics.add(Version.composeSeparateRealTimeTopic(storeName));
        }
      }
    }
    return consumingTopics;
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

  public double getDiskQuotaUsage() {
    long quota = storeQuotaInBytes;
    if (quota == Store.UNLIMITED_STORAGE_QUOTA) {
      return -1.;
    }

    // TODO: Remove this config when prod cluster metric is reported correctly.
    if (isServerCalculateQuotaUsageBasedOnPartitionsAssignmentEnabled) {
      if (partitionCount == 0) {
        return 0.;
      }
      quota *= partitionConsumptionSizeMap.size();
      quota /= partitionCount;
    }

    long usage = 0;
    for (StoragePartitionDiskUsage diskUsage: partitionConsumptionSizeMap.values()) {
      usage += diskUsage.getUsage();
    }
    return (double) usage / quota;
  }

  public void notifyFlushToDisk(PartitionConsumptionState pcs) {
    int partition = pcs.getPartition();
    StoragePartitionDiskUsage partitionConsumptionState = partitionConsumptionSizeMap.get(partition);
    if (partitionConsumptionState != null) {
      partitionConsumptionState.syncWithDB();
    }
  }
}
