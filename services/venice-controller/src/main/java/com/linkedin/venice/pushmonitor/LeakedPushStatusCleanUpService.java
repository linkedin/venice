package com.linkedin.venice.pushmonitor;

import static com.linkedin.venice.pushmonitor.PushStatusCleanUpServiceState.FAILED;
import static com.linkedin.venice.pushmonitor.PushStatusCleanUpServiceState.RUNNING;
import static com.linkedin.venice.pushmonitor.PushStatusCleanUpServiceState.STOPPED;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controller.HelixVeniceClusterResources;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreCleaner;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import com.linkedin.venice.utils.locks.ClusterLockManager;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Reclaims push statuses that servers may write after asynchronous push cancellation and controller cleanup.
 * Drops leaked Helix resources before removing residual push-status ZNodes on a later sweep.
 *
 * Runs with {@link HelixVeniceClusterResources} after metadata initialization and stops before repository teardown.
 * The shared lock manager fences eligibility and deletion against store recreation and repository clearing.
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
    this.clusterName = clusterName;
    this.offlinePushAccessor = offlinePushAccessor;
    this.metadataRepository = metadataRepository;
    this.storeCleaner = storeCleaner;
    this.clusterLockManager = clusterLockManager;
    this.aggPushStatusCleanUpStats = aggPushStatusCleanUpStats;
    this.sleepIntervalInMs = sleepIntervalInMs;
    this.leakedResourceAllowedLingerTimeInMs = leakedResourceAllowedLingerTimeInMs;
    this.cleanupThread = new Thread(new PushStatusCleanUpTask());
  }

  @Override
  public boolean startInner() throws Exception {
    cleanupThread.start();
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    // Shutdown holds the cluster write lock; joining a worker waiting for it would deadlock.
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
      if (!Version.isVersionTopic(storeVersion)) {
        LOGGER.warn("Found an invalid push status path: {}", storeVersion);
        continue;
      }
      int version = Version.parseVersionFromKafkaTopicName(storeVersion);
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
      try {
        // This snapshot only skips protected versions; candidates are rechecked under the lock.
        Store snapshot = metadataRepository.getStore(storeName);
        if (snapshot != null && entry.getValue()
            .stream()
            .noneMatch(version -> version < snapshot.getCurrentVersion() && !snapshot.containsVersion(version))) {
          continue;
        }
        VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName);
        boolean sharedMetadata = systemStoreType != null && systemStoreType.isNewMedataRepositoryAdopted();
        String userStoreName = sharedMetadata ? systemStoreType.extractRegularStoreName(storeName) : storeName;
        // When owner == cluster, the lock manager does not map the system-store lock to its owner.
        // The cluster write lock fences both names without nesting store locks.
        boolean requiresClusterWriteLock = sharedMetadata && userStoreName.equals(clusterName);
        try (AutoCloseableLock ignored = requiresClusterWriteLock
            ? clusterLockManager.createClusterWriteLock()
            : clusterLockManager.createStoreReadLock(storeName)) {
          // Shutdown may clear metadata while this worker waits for an uninterruptible lock.
          if (shouldStop()) {
            return;
          }
          Store store = metadataRepository.getStore(storeName);
          // The adapter also returns null when shared metadata is missing but the owner is alive.
          if (store == null && sharedMetadata && metadataRepository.getStore(userStoreName) != null) {
            LOGGER.warn(
                "Skipping push status cleanup for {} in cluster {}: metadata is missing but owning store {} exists",
                storeName,
                clusterName,
                userStoreName);
            continue;
          }
          // Explicit version allocation can reuse identifiers, so keep deletion fenced against metadata writes.
          cleanUpStore(storeName, store, entry.getValue());
        }
      } catch (RuntimeException e) {
        if (shouldStop()) {
          return;
        }
        LOGGER.error("Unable to check leaked push statuses for store {} in cluster {}", storeName, clusterName, e);
      }
    }
  }

  /** A null store means absence was confirmed in the locked repository. */
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
        leakedCount++;
        try {
          if (leakedCount <= MAX_LEAKED_VERSION_TO_KEEP && isRetainedForDebugging(kafkaTopic)) {
            continue;
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
        } catch (RuntimeException e) {
          if (shouldStop()) {
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

  private boolean isRetainedForDebugging(String kafkaTopic) {
    Optional<Long> creationTime = offlinePushAccessor.getOfflinePushStatusCreationTime(kafkaTopic);
    if (!creationTime.isPresent()) {
      LOGGER.warn("Retaining leaked push status {} in cluster {}: creation time is unknown", kafkaTopic, clusterName);
      return true;
    }
    long lingerTime = LatencyUtils.getElapsedTimeFromMsToMs(creationTime.get());
    if (lingerTime <= leakedResourceAllowedLingerTimeInMs) {
      LOGGER.info("Retaining leaked push status {} for investigation, linger time: {}ms", kafkaTopic, lingerTime);
      return true;
    }
    return false;
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
          } catch (RuntimeException e) {
            if (shouldStop()) {
              break;
            }
            LOGGER.error("Unable to sweep leaked push statuses in cluster {}; will retry", clusterName, e);
          }
          if (!shouldStop()) {
            Thread.sleep(sleepIntervalInMs);
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.info("Push status clean-up task interrupted");
      } catch (Error e) {
        finalState = FAILED;
        LOGGER.error("Unexpected error in push status clean-up task", e);
      } finally {
        aggPushStatusCleanUpStats.recordLeakedPushStatusCleanUpServiceState(finalState);
        LOGGER.info("Push status clean-up task stopped");
      }
    }
  }
}
