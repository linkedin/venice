package com.linkedin.venice.cleaner;

import static com.linkedin.venice.meta.VersionStatus.ONLINE;

import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.stats.BackupVersionOptimizationServiceStats;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is used to periodically scan inactive store versions and perform optimization if the inactive period
 * of some store version meets the pre-configured threshold, this class will trigger the database reopen action to
 * unload unnecessary RAM usage, which was built up when there were active reads coming.
 * The optimization follows the following rules:
 * 1. Don't touch current version no matter whether it is being actively used in the read path or not.
 * 2. Don't touch versions, which are not ready-to-serve yet.
 * 3. If some specific store version is ready-to-serve, but haven't been active recently, this class will reopen
 *    all the storage partitions belonging to the inactive version to free up unnecessary RAM usage.
 * 4. Don't reopen the database again if there is no more read usage after the optimization.
 *
 * This optimization is mainly for {@link com.linkedin.davinci.store.rocksdb.RocksDBStorageEngine}.
 * 1. Block-based format. Reopening the database will release the index/filter usages from the shared block cache.
 * 2. Plain-table format. Reopening the database will un-mmap the file contents, which were brought into memory by
 *    the past read requests.
 */
public class BackupVersionOptimizationService extends AbstractVeniceService implements ResourceReadUsageTracker {
  /**
   * This object is used to track the per-resource state.
   */
  private static final class ResourceState {
    long lastOptimizationTimestamp;
    long lastReadUsageTimestamp;

    public ResourceState() {
      this.lastOptimizationTimestamp = -1;
      this.lastReadUsageTimestamp = -1;
    }

    public void recordReadUsage() {
      lastReadUsageTimestamp = System.currentTimeMillis();
    }

    public void recordDatabaseOptimization() {
      lastOptimizationTimestamp = System.currentTimeMillis();
    }

    public boolean whetherToOptimize(long noReadThresholdMS) {
      if (lastReadUsageTimestamp == -1) {
        return false;
      }
      if (lastOptimizationTimestamp >= lastReadUsageTimestamp) {
        return false;
      }
      return (System.currentTimeMillis() - lastReadUsageTimestamp >= noReadThresholdMS);
    }
  }

  private static final Logger LOGGER = LogManager.getLogger(BackupVersionOptimizationService.class);

  private final ReadOnlyStoreRepository storeRepository;
  private final StorageEngineRepository storageEngineRepository;
  private final long noReadThresholdMSForDatabaseOptimization;
  private final long scheduleIntervalSeconds;
  private final BackupVersionOptimizationServiceStats stats;

  private final Map<String, ResourceState> resourceStateMap = new VeniceConcurrentHashMap<>();

  private final ScheduledExecutorService executor =
      Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory("BackupVersionCleanupService"));

  private boolean stop = false;

  /**
   * Allocate and initialize a new {@code BackupVersionOptimizationService} object.
   * @param storeRepository provides readonly operations to access stores.
   * @param storageEngineRepository local storage engines for a server node.
   * @param noReadThresholdMSForDatabaseOptimization controls the no read threshold for database optimization to kick in.
   * @param scheduleIntervalSeconds sets the scheduling interval for this service.
   * @param stats records the statistics for this service.
   */
  public BackupVersionOptimizationService(
      ReadOnlyStoreRepository storeRepository,
      StorageEngineRepository storageEngineRepository,
      long noReadThresholdMSForDatabaseOptimization,
      long scheduleIntervalSeconds,
      BackupVersionOptimizationServiceStats stats) {
    this.storeRepository = storeRepository;
    this.storageEngineRepository = storageEngineRepository;
    this.noReadThresholdMSForDatabaseOptimization = noReadThresholdMSForDatabaseOptimization;
    this.scheduleIntervalSeconds = scheduleIntervalSeconds;
    this.stats = stats;
  }

  private Runnable getOptimizationRunnable() {
    return () -> {
      if (stop) {
        return;
      }
      /**
       * Optimize the storage engine for inactive storage partitions
       */
      final Set<String> validResourceSet = new HashSet<>();
      for (AbstractStorageEngine engine: storageEngineRepository.getAllLocalStorageEngines()) {
        String resourceName = engine.getStoreName();
        validResourceSet.add(resourceName);
        String storeName = Version.parseStoreFromVersionTopic(resourceName);
        int versionNumber = Version.parseVersionFromVersionTopicName(resourceName);
        Store store = storeRepository.getStore(storeName);
        if (store == null) {
          LOGGER.warn("Failed to find out the store info from ReadOnlyStoreRepository for: {}", storeName);
          continue;
        }
        int currentVersion = store.getCurrentVersion();
        if (versionNumber == currentVersion) {
          // Don't touch current version.
          continue;
        }
        Optional<Version> versionInfo = store.getVersion(versionNumber);
        if (!versionInfo.isPresent()) {
          LOGGER.warn(
              "Failed to find out the version info for store: {}, version: {} from ReadOnlyStoreRepository",
              storeName,
              versionNumber);
          continue;
        }
        if (!versionInfo.get().getStatus().equals(ONLINE)) {
          // Skip the non-online version
          continue;
        }
        ResourceState resourceState = resourceStateMap.computeIfAbsent(resourceName, k -> new ResourceState());
        boolean optimize = resourceState.whetherToOptimize(noReadThresholdMSForDatabaseOptimization);
        if (optimize) {
          // Iterate all the partitions to check whether these partitions have been accessed recently or not
          final Set<Integer> partitionIdSet = engine.getPartitionIds();
          LOGGER.info("Start optimizing database for resource: {}, partition ids: {}", resourceName, partitionIdSet);
          boolean errored = false;
          for (int partitionId: partitionIdSet) {
            try {
              engine.reopenStoragePartition(partitionId);
              stats.recordBackupVersionDatabaseOptimization();
            } catch (Exception e) {
              LOGGER.error("Failed to optimize database for resource: {}, partition: {}", resourceName, partitionId, e);
              errored = true;
            }
          }
          if (errored) {
            stats.recordBackupVersionDatabaseOptimizationError();
            LOGGER.warn(
                "Encountered issue when optimizing database for resource: {}, "
                    + "and please check the above logs to find more details, and will retry the optimization in next iteration",
                resourceName);
          } else {
            resourceState.recordDatabaseOptimization();
            LOGGER.info("Finished optimizing database for resource: {}", resourceName);
          }
        }
      }
      // Remove the resource states, whose storage engine has been removed
      resourceStateMap.entrySet().removeIf(entry -> !validResourceSet.contains(entry.getKey()));
    };
  }

  @Override
  public boolean startInner() throws Exception {
    // Run the optimization periodically
    executor.scheduleAtFixedRate(getOptimizationRunnable(), 0, scheduleIntervalSeconds, TimeUnit.SECONDS);
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    stop = true;
    executor.shutdownNow();
    executor.awaitTermination(30, TimeUnit.SECONDS);
  }

  @Override
  public void recordReadUsage(String resourceName) {
    resourceStateMap.computeIfAbsent(resourceName, (k) -> new ResourceState()).recordReadUsage();
  }
}
