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
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
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
// TODO: rename
public class StorageUtilizationManager implements StoreDataChangedListener {
  private static final Logger LOGGER = LogManager.getLogger(StorageUtilizationManager.class);
  private static final RedundantExceptionFilter REDUNDANT_LOGGING_FILTER = getRedundantExceptionFilter();

  private final Map<Integer, PartitionConsumptionState> partitionConsumptionStateMap; // UnmodifiableMap
  private final AbstractStorageEngine storageEngine;
  private final Function<Integer, StoragePartitionDiskUsage> storagePartitionDiskUsageFunctionConstructor;
  private final String versionTopic;
  private final String storeName;
  private final int storeVersion;
  private final int partitionCount;
  private final Map<Integer, StoragePartitionDiskUsage> partitionConsumptionSizeMap;
  private final Set<Integer> pausedPartitionsForQuotaExceeded;
  private final Set<Integer> pausedPartitionsForRecordTooLarge;
  private final boolean isHybridQuotaEnabledInServer;
  private final boolean isServerCalculateQuotaUsageBasedOnPartitionsAssignmentEnabled;
  private final IngestionNotificationDispatcher ingestionNotificationDispatcher;
  private final TopicPartitionConsumerFunction pausePartition;
  private final TopicPartitionConsumerFunction resumePartition;

  private boolean versionIsOnline;

  private final AtomicInteger maxRecordSizeBytes; // TODO: does this need to be an atomic integer?

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
  // TODO: rename class after PR is reviewed because it's no longer just about hybrid quota
  public StorageUtilizationManager(
      AbstractStorageEngine storageEngine,
      Store store,
      String versionTopic,
      int partitionCount,
      Map<Integer, PartitionConsumptionState> partitionConsumptionStateMap,
      boolean isHybridQuotaEnabledInServer,
      boolean isServerCalculateQuotaUsageBasedOnPartitionsAssignmentEnabled,
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
    this.pausedPartitionsForQuotaExceeded = VeniceConcurrentHashMap.newKeySet();
    this.pausedPartitionsForRecordTooLarge = VeniceConcurrentHashMap.newKeySet();
    this.storeVersion = Version.parseVersionFromKafkaTopicName(versionTopic);
    this.isHybridQuotaEnabledInServer = isHybridQuotaEnabledInServer;
    this.isServerCalculateQuotaUsageBasedOnPartitionsAssignmentEnabled =
        isServerCalculateQuotaUsageBasedOnPartitionsAssignmentEnabled;
    this.ingestionNotificationDispatcher = ingestionNotificationDispatcher;
    this.pausePartition = pausePartition;
    this.resumePartition = resumePartition;
    this.maxRecordSizeBytes = new AtomicInteger(store.getMaxRecordSizeBytes());
    setStoreQuota(store);
    Version version = store.getVersion(storeVersion);
    versionIsOnline = isVersionOnline(version);
  }

  // TODO: possibly better naming?
  protected enum PausedConsumptionReason {
    QUOTA_EXCEEDED, RECORD_TOO_LARGE
  }

  private Set<Integer> getPausedPartitionsForReason(PausedConsumptionReason reason) {
    switch (reason) {
      case QUOTA_EXCEEDED:
        return pausedPartitionsForQuotaExceeded;
      case RECORD_TOO_LARGE:
        return pausedPartitionsForRecordTooLarge;
      default:
        throw new IllegalArgumentException("Unknown reason: " + reason);
    }
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
          "Version: {} doesn't exist in the store: {}",
          Version.parseVersionFromKafkaTopicName(versionTopic),
          storeName);
      return;
    }
    versionIsOnline = isVersionOnline(version);
    if (this.storeQuotaInBytes != store.getStorageQuotaInByte() || !store.isHybridStoreDiskQuotaEnabled()) {
      LOGGER.info(
          "Store: {} changed, updated quota from {} to {} and store quota is {}, "
              + "so we reset the store quota and resume all partitions.",
          this.storeName,
          this.storeQuotaInBytes,
          store.getStorageQuotaInByte(),
          store.isHybridStoreDiskQuotaEnabled() ? "enabled" : "not enabled");
      resumeAllPartitionsWherePossible(PausedConsumptionReason.QUOTA_EXCEEDED);
    }

    int oldMaxRecordSizeBytes = this.maxRecordSizeBytes.get();
    if (this.maxRecordSizeBytes.compareAndSet(oldMaxRecordSizeBytes, store.getMaxRecordSizeBytes())) {
      boolean isRecordLimitIncreased = oldMaxRecordSizeBytes < store.getMaxRecordSizeBytes();
      LOGGER.info(
          "Store: {} changed, updated max record size from {} to {}. {}",
          this.storeName,
          oldMaxRecordSizeBytes,
          store.getMaxRecordSizeBytes(),
          (isRecordLimitIncreased)
              ? "Attempting to resume consumption on all partitions paused by RecordTooLarge issue."
              : "");
      if (isRecordLimitIncreased) {
        resumeAllPartitionsWherePossible(PausedConsumptionReason.RECORD_TOO_LARGE);
      }
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

    String consumingTopic = getConsumingTopic(pcs);
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
      pausePartition(partition, consumingTopic, PausedConsumptionReason.QUOTA_EXCEEDED);
      String msgIdentifier = consumingTopic + "_" + partition + "_quota_exceeded";
      // Log quota exceeded info only once a minute per partition.
      boolean shouldLogQuotaExceeded = !REDUNDANT_LOGGING_FILTER.isRedundantException(msgIdentifier);
      if (shouldLogQuotaExceeded) {
        LOGGER.info(
            "Quota exceeded for store {} partition {}, paused this partition. {}",
            storeName,
            partition,
            versionTopic);
      }
    } else { /** we have free space for this partition */
      /**
       *  Paused partitions could be resumed
       */
      ingestionNotificationDispatcher.reportQuotaNotViolated(pcs);
      if (isPartitionPausedForQuotaExceeded(partition)) {
        boolean resumed = resumePartitionIfPossible(partition, consumingTopic, PausedConsumptionReason.QUOTA_EXCEEDED);
        String action = (resumed) ? "resumed" : "but unable to immediately resume";
        LOGGER.info("Quota available for store {} partition {}, {} this partition.", storeName, action, partition);
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
  protected void pausePartition(int partition, String consumingTopic, PausedConsumptionReason reason) {
    getPausedPartitionsForReason(reason).add(partition);
    pausePartition.execute(consumingTopic, partition);
  }

  /**
   * Resume the partition if it's paused for the given reason. In the unlikely event that a partition is paused for both
   * reasons, we will not be able to resume it until both reasons are resolved.
   * @return true if the partition was resumed, false otherwise
   */
  private boolean resumePartitionIfPossible(int partition, String consumingTopic, PausedConsumptionReason reason) {
    getPausedPartitionsForReason(reason).remove(partition);
    if (pausedPartitionsForQuotaExceeded.contains(partition) || pausedPartitionsForRecordTooLarge.contains(partition)) {
      return false;
    }
    resumePartition.execute(consumingTopic, partition);
    return true;
  }

  /**
   * Resumes consumption on all partitions. Intentionally loops over {@link partitionConsumptionStateMap} to avoid
   * any partitions missing from {@link pausedPartitionsForQuotaExceeded} or {@link pausedPartitionsForRecordTooLarge}.
   * In the unlikely case that partition is paused for both reasons, we can't resume it until both reasons are resolved.
   */
  private void resumeAllPartitionsWherePossible(PausedConsumptionReason reason) {
    partitionConsumptionStateMap.forEach((partition, pcs) -> {
      String consumingTopic = getConsumingTopic(pcs);
      resumePartitionIfPossible(partition, consumingTopic, reason);
    });
  }

  protected void pausePartitionForRecordTooLarge(int partition, String consumingTopic) {
    pausePartition(partition, consumingTopic, PausedConsumptionReason.RECORD_TOO_LARGE);
  }

  private boolean isVersionOnline(Version version) {
    return version != null && version.getStatus().equals(VersionStatus.ONLINE);
  }

  /**
   * Check the topic which is currently consumed topic for this partition
   */
  private String getConsumingTopic(PartitionConsumptionState pcs) {
    String consumingTopic = versionTopic;
    if (pcs.getLeaderFollowerState().equals(LEADER)) {
      OffsetRecord offsetRecord = pcs.getOffsetRecord();
      if (offsetRecord.getLeaderTopic() != null) {
        consumingTopic = offsetRecord.getLeaderTopic();
      }
    }
    return consumingTopic;
  }

  protected boolean isPartitionPausedForReason(int partition, PausedConsumptionReason reason) {
    return getPausedPartitionsForReason(reason).contains(partition);
  }

  protected boolean isPartitionPausedForRecordTooLarge(int partition) {
    return pausedPartitionsForRecordTooLarge.contains(partition);
  }

  protected boolean isPartitionPausedForQuotaExceeded(int partition) {
    return pausedPartitionsForQuotaExceeded.contains(partition);
  }

  public boolean hasPausedPartitionForQuotaExceeded() {
    return !pausedPartitionsForQuotaExceeded.isEmpty();
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
