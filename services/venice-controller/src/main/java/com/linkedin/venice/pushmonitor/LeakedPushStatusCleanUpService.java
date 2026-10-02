package com.linkedin.venice.pushmonitor;

import static com.linkedin.venice.pushmonitor.PushStatusCleanUpServiceState.FAILED;
import static com.linkedin.venice.pushmonitor.PushStatusCleanUpServiceState.RUNNING;
import static com.linkedin.venice.pushmonitor.PushStatusCleanUpServiceState.STOPPED;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controller.HelixVeniceClusterResources;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreCleaner;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.ExceptionUtils;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import com.linkedin.venice.utils.locks.ClusterLockManager;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.helix.HelixException;
import org.apache.helix.zookeeper.zkclient.exception.ZkException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * LeakedPushStatusCleanUpService will wake up regularly (interval is determined by controller config
 * {@link com.linkedin.venice.ConfigKeys#LEAKED_PUSH_STATUS_CLEAN_UP_SERVICE_SLEEP_INTERVAL_MS}), get all existing push
 * status ZNodes on Zookeeper that belong to the specified cluster, without scanning through the replica statuses, find
 * leaked push statuses, drop their Helix resources, and remove residual ZNodes on a subsequent sweep.
 *
 * The life cycle of LeakedPushStatusCleanUpService matches the life cycle of {@link HelixVeniceClusterResources},
 * meaning that there is one clean up service for each cluster, and it's built when the controller is promoted to leader
 * role for the cluster. It starts only after the leader's store repository is initialized, and stops before that
 * repository is cleared under the cluster write lock. Eligibility checks and cleanup share the store write lock with
 * store creation/deletion; this also holds the cluster read lock to fence repository teardown.
 *
 * For an existing store, only versions older than current and absent from metadata are eligible. For a deleted store,
 * all discovered versions are eligible. Missing shared system-store metadata only qualifies when the owning user store
 * is also absent.
 * In either case, the newest eligible version is retained for debugging until its push-status creation age exceeds the
 * configured linger time; an unknown creation time retains it. Older eligible versions are removed immediately.
 *
 * The clean up service is needed because push job killing in server nodes is asynchronous, it's possible that servers
 * write push status after controllers think they have cleaned up the push status.
 */
public class LeakedPushStatusCleanUpService extends AbstractVeniceService {
  private static final Logger LOGGER = LogManager.getLogger(LeakedPushStatusCleanUpService.class);
  private static final Comparator<Integer> VERSION_COMPARATOR = new Comparator<Integer>() {
    @Override
    public int compare(Integer o1, Integer o2) {
      /**
       * Higher version number comes first.
       */
      return o2 - o1;
    }
  };

  /**
   * Keep 1 leaked push status version for debugging.
   */
  private static final int MAX_LEAKED_VERSION_TO_KEEP = 1;

  private final String clusterName;
  private final OfflinePushAccessor offlinePushAccessor;
  private final ReadOnlyStoreRepository metadataRepository;
  private final StoreCleaner storeCleaner;
  private final ClusterLockManager clusterLockManager;
  private final AggPushStatusCleanUpStats aggPushStatusCleanUpStats;
  private final long sleepIntervalInMs;
  private final long leakedResourceAllowedLingerTimeInMs;
  private final Time time;
  private final Thread cleanupThread;
  private final AtomicBoolean stop = new AtomicBoolean(false);

  public LeakedPushStatusCleanUpService(
      String clusterName,
      OfflinePushAccessor offlinePushAccessor,
      ReadOnlyStoreRepository metadataRepository,
      StoreCleaner storeCleaner,
      ClusterLockManager clusterLockManager,
      AggPushStatusCleanUpStats aggPushStatusCleanUpStats,
      long sleepIntervalInMs,
      long leakedResourceAllowedLingerTimeInMs) {
    this(
        clusterName,
        offlinePushAccessor,
        metadataRepository,
        storeCleaner,
        clusterLockManager,
        aggPushStatusCleanUpStats,
        sleepIntervalInMs,
        leakedResourceAllowedLingerTimeInMs,
        SystemTime.INSTANCE);
  }

  LeakedPushStatusCleanUpService(
      String clusterName,
      OfflinePushAccessor offlinePushAccessor,
      ReadOnlyStoreRepository metadataRepository,
      StoreCleaner storeCleaner,
      ClusterLockManager clusterLockManager,
      AggPushStatusCleanUpStats aggPushStatusCleanUpStats,
      long sleepIntervalInMs,
      long leakedResourceAllowedLingerTimeInMs,
      Time time) {
    this.clusterName = clusterName;
    this.offlinePushAccessor = offlinePushAccessor;
    this.metadataRepository = metadataRepository;
    this.storeCleaner = storeCleaner;
    this.clusterLockManager = clusterLockManager;
    this.aggPushStatusCleanUpStats = aggPushStatusCleanUpStats;
    this.sleepIntervalInMs = sleepIntervalInMs;
    this.leakedResourceAllowedLingerTimeInMs = leakedResourceAllowedLingerTimeInMs;
    this.time = time;
    this.cleanupThread = new Thread(new PushStatusCleanUpTask());
  }

  @Override
  public boolean startInner() throws Exception {
    cleanupThread.start();
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    stop.set(true);
    cleanupThread.interrupt();
  }

  /**
   * Helper function; group store versions by store name.
   * @param storeVersions
   * @return a map; key: storeName, value: a list of the store's version numbers observed on push status ZK path
   */
  private static Map<String, PriorityQueue<Integer>> groupVersionsByStore(List<String> storeVersions) {
    Map<String, PriorityQueue<Integer>> storeToVersions = new HashMap<>();
    for (String storeVersion: storeVersions) {
      if (storeVersion == null || storeVersion.lastIndexOf(Version.VERSION_SEPARATOR) <= 0
          || !Version.isVersionTopic(storeVersion)) {
        LOGGER.warn("Found an invalid push status path: {}", storeVersion);
        continue;
      }
      int version;
      try {
        version = Version.parseVersionFromKafkaTopicName(storeVersion);
      } catch (NumberFormatException e) {
        LOGGER.warn("Found an invalid push status path: {}", storeVersion, e);
        continue;
      }
      String storeName = Version.parseStoreFromKafkaTopicName(storeVersion);
      storeToVersions.computeIfAbsent(storeName, n -> new PriorityQueue<>(VERSION_COMPARATOR));
      storeToVersions.computeIfPresent(storeName, (key, queue) -> {
        if (!queue.contains(version)) {
          queue.offer(version);
        }
        return queue;
      });
    }
    return storeToVersions;
  }

  private boolean shouldStop() {
    return stop.get() || Thread.currentThread().isInterrupted();
  }

  private boolean stopOnInterruption(Exception e) {
    if (ExceptionUtils.recursiveClassEquals(e, InterruptedException.class)) {
      Thread.currentThread().interrupt();
    }
    return shouldStop();
  }

  /** One sweep, also used by deterministic tests without starting the background worker. */
  void cleanUpLeakedPushStatuses() {
    if (shouldStop()) {
      return;
    }
    Map<String, PriorityQueue<Integer>> storeToVersions =
        groupVersionsByStore(offlinePushAccessor.loadOfflinePushStatusPaths());
    for (Map.Entry<String, PriorityQueue<Integer>> entry: storeToVersions.entrySet()) {
      if (shouldStop()) {
        return;
      }
      String storeName = entry.getKey();
      VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName);
      boolean sharedMetadata = systemStoreType != null && systemStoreType.isNewMedataRepositoryAdopted();
      String ownerStoreName = sharedMetadata ? systemStoreType.extractRegularStoreName(storeName) : storeName;
      // The lock manager keeps legacy cluster-wide system-store names separate from a same-named user store.
      // Fence both names in that exceptional case without nesting store locks in a conflicting order.
      boolean clusterScopedLock =
          sharedMetadata && systemStoreType.isStoreZkShared() && ownerStoreName.equals(clusterName);
      try (AutoCloseableLock ignored = clusterScopedLock
          ? clusterLockManager.createClusterWriteLock()
          : clusterLockManager.createStoreWriteLock(storeName)) {
        // Lock acquisition may have waited for shutdown to stop this service and clear the repository.
        if (shouldStop()) {
          return;
        }
        Store store = metadataRepository.getStore(storeName);
        if (store == null && sharedMetadata) {
          // Mirror the repository adapter: a null shared system store can mean missing shared metadata, not deletion.
          if (metadataRepository.getStore(ownerStoreName) != null) {
            LOGGER.warn(
                "Skipping push status cleanup for {} in cluster {}: metadata is missing but owning store {} exists",
                storeName,
                clusterName,
                ownerStoreName);
            continue;
          }
        }
        cleanUpStore(storeName, store, entry.getValue());
      } catch (VeniceException | HelixException | ZkException e) {
        if (stopOnInterruption(e)) {
          return;
        }
        // A lookup failure is not evidence of absence. Retry this store next sweep.
        LOGGER.error("Unable to check leaked push statuses for store {} in cluster {}", storeName, clusterName, e);
      }
    }
  }

  private void cleanUpStore(String storeName, Store store, PriorityQueue<Integer> versions) {
    int leakedCount = 0;
    int successfulCount = 0;
    int failedCount = 0;
    try {
      while (!versions.isEmpty() && !shouldStop()) {
        int version = versions.poll();
        // Current/future versions may still be ingesting, and versions in metadata may still be serving.
        if (store != null && (version >= store.getCurrentVersion() || store.containsVersion(version))) {
          continue;
        }
        String kafkaTopic = Version.composeKafkaTopic(storeName, version);
        boolean retainForDebugging = leakedCount++ < MAX_LEAKED_VERSION_TO_KEEP;
        try {
          if (retainForDebugging) {
            Optional<Long> creationTime = offlinePushAccessor.getOfflinePushStatusCreationTime(kafkaTopic);
            if (!creationTime.isPresent()) {
              LOGGER.warn(
                  "Retaining leaked push status {} in cluster {}: creation time is unknown",
                  kafkaTopic,
                  clusterName);
              continue;
            }
            long lingerTime = time.getMilliseconds() - creationTime.get();
            if (lingerTime <= leakedResourceAllowedLingerTimeInMs) {
              LOGGER
                  .info("Retaining leaked push status {} for investigation, linger time: {}ms", kafkaTopic, lingerTime);
              continue;
            }
          }
          if (shouldStop()) {
            return;
          }
          // The StoreCleaner checks leadership even for ZK-only leftovers. Keep discovery until Helix is absent.
          boolean inHelix = storeCleaner.containsHelixResource(clusterName, kafkaTopic);
          if (shouldStop()) {
            return;
          }
          LOGGER.info("Deleting leaked push status: {} in cluster {}", kafkaTopic, clusterName);
          if (inHelix) {
            storeCleaner.deleteHelixResource(clusterName, kafkaTopic);
          } else {
            offlinePushAccessor.deleteOfflinePushStatusAndItsPartitionStatuses(kafkaTopic);
          }
          successfulCount++;
        } catch (VeniceException | HelixException | ZkException e) {
          if (stopOnInterruption(e)) {
            return;
          }
          failedCount++;
          LOGGER
              .error("Unable to clean up leaked push status {} in cluster {}; will retry", kafkaTopic, clusterName, e);
        }
      }
    } finally {
      aggPushStatusCleanUpStats.recordLeakedPushStatusCount(leakedCount);
      aggPushStatusCleanUpStats.recordSuccessfulLeakedPushStatusCleanUpCount(successfulCount);
      aggPushStatusCleanUpStats.recordFailedLeakedPushStatusCleanUpCount(failedCount);
    }
  }

  private class PushStatusCleanUpTask implements Runnable {
    @Override
    public void run() {
      PushStatusCleanUpServiceState finalState = STOPPED;
      try {
        aggPushStatusCleanUpStats.recordLeakedPushStatusCleanUpServiceState(RUNNING);
        while (!shouldStop()) {
          try {
            cleanUpLeakedPushStatuses();
          } catch (VeniceException | HelixException | ZkException e) {
            if (stopOnInterruption(e)) {
              break;
            }
            LOGGER.error("Unable to scan push statuses in cluster {}; will retry", clusterName, e);
          }
          if (!shouldStop()) {
            // No store or cluster locks are held while sleeping.
            Thread.sleep(sleepIntervalInMs);
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.info("Push status clean-up task interrupted");
      } catch (RuntimeException | Error e) {
        finalState = FAILED;
        LOGGER.error("Unexpected error in push status clean-up task", e);
        throw e;
      } finally {
        aggPushStatusCleanUpStats.recordLeakedPushStatusCleanUpServiceState(finalState);
        LOGGER.info("Push status clean-up task stopped");
      }
    }
  }
}
