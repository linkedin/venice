package com.linkedin.venice.controller.systemstore;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.UserSystemStoreLifeCycleHelper;
import com.linkedin.venice.controller.VeniceParentHelixAdmin;
import com.linkedin.venice.controller.stats.SystemStoreHealthCheckStats;
import com.linkedin.venice.controller.systemstore.SystemStoreHealthChecker.HealthCheckResult;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.LogContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class tries to scan all cluster which current parent controller is the leader controller.
 * It will perform the following action for each system store of each cluster:
 * 1. Check system store is created.
 * 2. Send heartbeat to system store and check if heartbeat is received after certain wait period.
 * 3. If system store failed any of the check in (1) / (2), it will try to run empty push to repair the system store.
 * It will emit metrics to indicate bad system store counts per cluster and how many stores are not fixable by the task.
 *
 * A pluggable {@link SystemStoreHealthChecker} is used to determine system store health. Stores returning
 * {@link HealthCheckResult#UNKNOWN} are treated as unhealthy and will be repaired.
 */
public class SystemStoreRepairTask implements Runnable {
  public static final Logger LOGGER = LogManager.getLogger(SystemStoreRepairTask.class);
  public static final String SYSTEM_STORE_REPAIR_JOB_PREFIX = "CONTROLLER_SYSTEM_STORE_REPAIR_JOB_";
  private static final int DEFAULT_SKIP_NEWLY_CREATED_STORE_SYSTEM_STORE_HEALTH_CHECK_IN_HOURS = 2;
  private static final int DEFAULT_REPAIR_JOB_CHECK_TIMEOUT_IN_SECONDS = 3600;
  private static final int DEFAULT_REPAIR_JOB_CHECK_INTERVAL_IN_SECONDS = 30;

  private final int versionRefreshThresholdInDays;
  private final int maxRepairPerRound;
  private final VeniceParentHelixAdmin parentAdmin;
  private final AtomicBoolean isRunning;
  private final Map<String, SystemStoreHealthCheckStats> clusterToSystemStoreHealthCheckStatsMap;
  private final SystemStoreHealthChecker healthChecker;

  public SystemStoreRepairTask(
      VeniceParentHelixAdmin parentAdmin,
      Map<String, SystemStoreHealthCheckStats> clusterToSystemStoreHealthCheckStatsMap,
      int versionRefreshThresholdInDays,
      int maxRepairPerRound,
      AtomicBoolean isRunning,
      SystemStoreHealthChecker healthChecker) {
    this.parentAdmin = parentAdmin;
    this.clusterToSystemStoreHealthCheckStatsMap = clusterToSystemStoreHealthCheckStatsMap;
    this.versionRefreshThresholdInDays = versionRefreshThresholdInDays;
    this.maxRepairPerRound = maxRepairPerRound;
    this.isRunning = isRunning;
    this.healthChecker = healthChecker;
  }

  @Override
  public void run() {
    LogContext.setLogContext(getParentAdmin().getLogContext());
    for (String clusterName: getParentAdmin().getClustersLeaderOf()) {
      if (!getClusterToSystemStoreHealthCheckStatsMap().containsKey(clusterName)) {
        continue;
      }
      LOGGER.info("Starting system store repair task for cluster: {}", clusterName);
      Set<String> unhealthySystemStoreSet = new HashSet<>();
      // Iterate all system stores and get unhealthy system stores.
      checkSystemStoresHealth(clusterName, unhealthySystemStoreSet);
      // Try repair all bad system stores.
      repairBadSystemStore(clusterName, unhealthySystemStoreSet);
      LOGGER.info("Completed system store repair task for cluster: {}", clusterName);
    }
  }

  void repairBadSystemStore(String clusterName, Set<String> unhealthySystemStoreSet) {
    int configLimit = getMaxRepairPerRound();
    boolean unlimited = configLimit < 0;
    if (!unlimited && unhealthySystemStoreSet.size() > configLimit) {
      LOGGER.info(
          "Cluster {} has {} unhealthy system stores but max repair per round is {}. Deferring {} stores to next round.",
          clusterName,
          unhealthySystemStoreSet.size(),
          configLimit,
          unhealthySystemStoreSet.size() - configLimit);
    }
    Map<String, Integer> systemStoreToRepairJobVersionMap = new HashMap<>();
    Set<String> attemptedStores = new HashSet<>();
    int repairCount = 0;
    for (String systemStoreName: unhealthySystemStoreSet) {
      if (!unlimited && repairCount >= configLimit) {
        break;
      }
      if (!shouldContinue(clusterName)) {
        return;
      }
      attemptedStores.add(systemStoreName);
      String pushJobId = SYSTEM_STORE_REPAIR_JOB_PREFIX + System.currentTimeMillis();
      try {
        Version version = getNewSystemStoreVersion(clusterName, systemStoreName, pushJobId);
        systemStoreToRepairJobVersionMap.put(systemStoreName, version.getNumber());
        repairCount++;
        LOGGER.info(
            "Kick off an repair empty push job for store: {} in cluster: {} with expected version number: {}",
            systemStoreName,
            clusterName,
            version.getNumber());
      } catch (Exception e) {
        LOGGER.warn("Unable to run empty push job for store: {} in cluster: {}", systemStoreName, clusterName, e);
      }
    }
    // After sending all repair request, periodically poll the push status until terminal status or timeout.
    pollSystemStorePushStatus(
        clusterName,
        systemStoreToRepairJobVersionMap,
        unhealthySystemStoreSet,
        getRepairJobCheckTimeoutInSeconds());
    // After repairing system stores, update stats again.
    updateBadSystemStoreCount(clusterName, unhealthySystemStoreSet);
    // Only count stores that were actually attempted but could not be repaired.
    // Stores deferred due to maxRepairPerRound are excluded.
    Set<String> notRepairableStores = new HashSet<>(unhealthySystemStoreSet);
    notRepairableStores.retainAll(attemptedStores);
    updateNotRepairableSystemStoreCount(clusterName, notRepairableStores);
  }

  /**
   * Check health of all system stores in the cluster:
   * 1. Pre-filter to get candidate stores (stores already known unhealthy are added directly).
   * 2. Run the health checker on candidates. HEALTHY stores are skipped; UNHEALTHY and UNKNOWN are repaired.
   */
  void checkSystemStoresHealth(String clusterName, Set<String> unhealthySystemStoreSet) {
    Set<String> candidates = preFilterSystemStores(clusterName, unhealthySystemStoreSet);

    if (candidates.isEmpty()) {
      updateBadSystemStoreCount(clusterName, unhealthySystemStoreSet);
      return;
    }

    Map<String, HealthCheckResult> results = getHealthChecker().checkHealth(clusterName, candidates);
    int healthyCount = 0;
    int unhealthyCount = 0;
    int unknownCount = 0;
    for (String storeName: candidates) {
      HealthCheckResult result = results.getOrDefault(storeName, HealthCheckResult.UNKNOWN);
      if (result == HealthCheckResult.HEALTHY) {
        healthyCount++;
      } else {
        // UNHEALTHY and UNKNOWN are both treated as unhealthy
        unhealthySystemStoreSet.add(storeName);
        if (result == HealthCheckResult.UNHEALTHY) {
          unhealthyCount++;
        } else {
          unknownCount++;
        }
      }
    }
    LOGGER.info(
        "Health checker for cluster {} returned: {} healthy, {} unhealthy, {} unknown (treated as unhealthy)",
        clusterName,
        healthyCount,
        unhealthyCount,
        unknownCount);

    updateBadSystemStoreCount(clusterName, unhealthySystemStoreSet);
  }

  /**
   * Pre-filter system stores to identify candidates for health checking. This extracts the filtering logic that was
   * previously part of {@code checkAndSendHeartbeatToSystemStores}:
   * 1. Check user stores and add non-created system stores directly to unhealthySet.
   * 2. For created system stores, check if version needs refresh; stale versions go directly to unhealthySet.
   * 3. Remaining stores become candidates for health checking (heartbeat or metrics).
   *
   * @return the set of candidate system store names that need health checking
   */
  Set<String> preFilterSystemStores(String clusterName, Set<String> unhealthySystemStoreSet) {
    Set<String> candidates = new HashSet<>();
    Map<String, Long> userStoreToCreationTimestampMap = new HashMap<>();
    List<Store> storeList = getParentAdmin().getAllStores(clusterName);

    // First pass: check user stores for missing system stores
    for (Store store: storeList) {
      if (!shouldContinue(clusterName)) {
        return candidates;
      }

      if (!VeniceSystemStoreUtils.isSystemStore(store.getName())) {
        userStoreToCreationTimestampMap
            .put(VeniceSystemStoreType.META_STORE.getSystemStoreName(store.getName()), store.getCreatedTime());
        userStoreToCreationTimestampMap.put(
            VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(store.getName()),
            store.getCreatedTime());
        if (isStoreNewlyCreated(store.getCreatedTime())) {
          continue;
        }
        if (store.isMigrating()) {
          LOGGER.info("Store: {} is being migrated, will skip it for now.", store.getName());
          continue;
        }
        if (!store.isDaVinciPushStatusStoreEnabled()) {
          unhealthySystemStoreSet
              .add(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(store.getName()));
        }
        if (!store.isStoreMetaSystemStoreEnabled()) {
          unhealthySystemStoreSet.add(VeniceSystemStoreType.META_STORE.getSystemStoreName(store.getName()));
        }
      }
    }

    // Second pass: check system stores for version staleness; remaining become candidates
    for (Store store: storeList) {
      if (!shouldContinue(clusterName)) {
        return candidates;
      }

      if (!(VeniceSystemStoreUtils.isSystemStore(store.getName())
          && VeniceSystemStoreUtils.isUserSystemStore(store.getName()))) {
        continue;
      }

      if (store.isMigrating()) {
        LOGGER.info("Store: {} is being migrated, will skip it for now.", store.getName());
        continue;
      }

      if (isStoreNewlyCreated(userStoreToCreationTimestampMap.getOrDefault(store.getName(), 0L))) {
        continue;
      }

      if (unhealthySystemStoreSet.contains(store.getName())) {
        continue;
      }

      List<Version> storeVersionList = store.getVersions();
      long latestCreatedTime = storeVersionList.stream().mapToLong(Version::getCreatedTime).max().orElse(0);
      boolean versionTooOldOrMissing = LatencyUtils.getElapsedTimeFromMsToMs(latestCreatedTime) > TimeUnit.DAYS
          .toMillis(getVersionRefreshThresholdInDays());

      if (versionTooOldOrMissing) {
        if (latestCreatedTime == 0) {
          LOGGER.info("Adding the system store: {} to the repair set as there is no version.", store.getName());
        } else {
          long versionAgeInMs = System.currentTimeMillis() - latestCreatedTime;
          LOGGER.info(
              "Adding the system store: {} to the repair set as the version age: {} exceeds threshold.",
              store.getName(),
              versionAgeInMs);
        }
        unhealthySystemStoreSet.add(store.getName());
        continue;
      }

      candidates.add(store.getName());
    }

    return candidates;
  }

  /**
   * Poll the system store push status until it reaches terminal status.
   * If push job completes in given check period, it will remove from unhealthy store set.
   */
  void pollSystemStorePushStatus(
      String clusterName,
      Map<String, Integer> systemStoreToRepairJobVersionMap,
      Set<String> unhealthySystemStoreSet,
      int repairJobCheckTimeoutInSeconds) {
    long startCheckingTime = System.currentTimeMillis();
    periodicCheckTask(clusterName, repairJobCheckTimeoutInSeconds, getRepairJobCheckIntervalInSeconds(), () -> {
      List<String> keyToRemove = new ArrayList<>();
      for (Map.Entry<String, Integer> entry: systemStoreToRepairJobVersionMap.entrySet()) {
        try {
          String kafkaTopic = Version.composeKafkaTopic(entry.getKey(), entry.getValue());
          Admin.OfflinePushStatusInfo pushStatus = getParentAdmin().getOffLinePushStatus(clusterName, kafkaTopic);
          if (pushStatus.getExecutionStatus().isTerminal()) {
            keyToRemove.add(entry.getKey());
            long elapsedTimeInMillis = System.currentTimeMillis() - startCheckingTime;
            if (pushStatus.getExecutionStatus().equals(ExecutionStatus.COMPLETED)) {
              LOGGER.info(
                  "Repair job successful for store: {}, version: {} in: {} ms.",
                  entry.getKey(),
                  entry.getValue(),
                  elapsedTimeInMillis);
              unhealthySystemStoreSet.remove(entry.getKey());
            } else {
              LOGGER.warn(
                  "Repair job failed for store: {}, version: {} in: {} ms.",
                  entry.getKey(),
                  entry.getValue(),
                  elapsedTimeInMillis);
            }
          }
        } catch (Exception e) {
          LOGGER.warn("Caught exception in polling push status for system store: {}", entry.getKey(), e);
        }
      }
      for (String key: keyToRemove) {
        systemStoreToRepairJobVersionMap.remove(key);
      }
      return systemStoreToRepairJobVersionMap.isEmpty();
    });
    if (systemStoreToRepairJobVersionMap.isEmpty()) {
      LOGGER.info("Finish all repair job.");
    } else {
      LOGGER.warn("Time out waiting repair job for: {}", systemStoreToRepairJobVersionMap.keySet());
    }
  }

  void periodicCheckTask(
      String clusterName,
      int maxWaitTimeInSeconds,
      int checkIntervalInSeconds,
      BooleanSupplier checkTask) {
    long startCheckingTime = System.currentTimeMillis();
    while ((System.currentTimeMillis() - startCheckingTime) <= TimeUnit.SECONDS.toMillis(maxWaitTimeInSeconds)) {
      if (!shouldContinue(clusterName)) {
        return;
      }
      boolean result = checkTask.getAsBoolean();
      if (result) {
        LOGGER.info("Check task completed for {} ms", System.currentTimeMillis() - startCheckingTime);
        return;
      }
      // Wait for certain period for next round of check.
      LatencyUtils.sleep(TimeUnit.SECONDS.toMillis(checkIntervalInSeconds));
    }
  }

  void updateBadSystemStoreCount(String clusterName, Set<String> newUnhealthySystemStoreSet) {
    long newBadMetaSystemStoreCount = newUnhealthySystemStoreSet.stream()
        .filter(x -> VeniceSystemStoreType.META_STORE.equals(VeniceSystemStoreType.getSystemStoreType(x)))
        .count();
    getBadMetaStoreCount(clusterName).set(newBadMetaSystemStoreCount);
    getBadPushStatusStoreCount(clusterName).set(newUnhealthySystemStoreSet.size() - newBadMetaSystemStoreCount);
    LOGGER.info(
        "Cluster: {} collected unhealthy system stores: {}. Meta system store count: {}, push status system store count: {}",
        clusterName,
        newUnhealthySystemStoreSet.toString(),
        getBadMetaStoreCount(clusterName).get(),
        getBadPushStatusStoreCount(clusterName).get());
  }

  void updateNotRepairableSystemStoreCount(String clusterName, Set<String> notRepairableSystemStoreSet) {
    getNotRepairableSystemStoreCounter(clusterName).set(notRepairableSystemStoreSet.size());
    LOGGER.info("Cluster: {} has not repairable system stores: {}", clusterName, notRepairableSystemStoreSet);
  }

  AtomicBoolean getIsRunning() {
    return isRunning;
  }

  boolean isStoreNewlyCreated(long creationTimestamp) {
    return LatencyUtils.getElapsedTimeFromMsToMs(creationTimestamp) < TimeUnit.HOURS
        .toMillis(DEFAULT_SKIP_NEWLY_CREATED_STORE_SYSTEM_STORE_HEALTH_CHECK_IN_HOURS);
  }

  Map<String, SystemStoreHealthCheckStats> getClusterToSystemStoreHealthCheckStatsMap() {
    return clusterToSystemStoreHealthCheckStatsMap;
  }

  SystemStoreHealthCheckStats getClusterSystemStoreHealthCheckStats(String clusterName) {
    return getClusterToSystemStoreHealthCheckStatsMap().get(clusterName);
  }

  AtomicLong getBadMetaStoreCount(String clusterName) {
    return getClusterSystemStoreHealthCheckStats(clusterName).getBadMetaSystemStoreCounter();
  }

  AtomicLong getBadPushStatusStoreCount(String clusterName) {
    return getClusterSystemStoreHealthCheckStats(clusterName).getBadPushStatusSystemStoreCounter();
  }

  AtomicLong getNotRepairableSystemStoreCounter(String clusterName) {
    return getClusterSystemStoreHealthCheckStats(clusterName).getNotRepairableSystemStoreCounter();
  }

  boolean shouldContinue(String clusterName) {
    if (!getIsRunning().get()) {
      return false;
    }
    return getParentAdmin().isLeaderControllerFor(clusterName);
  }

  VeniceParentHelixAdmin getParentAdmin() {
    return parentAdmin;
  }

  Version getNewSystemStoreVersion(String clusterName, String systemStoreName, String pushJobId) {
    return UserSystemStoreLifeCycleHelper
        .materializeSystemStore(getParentAdmin(), clusterName, systemStoreName, pushJobId);
  }

  int getRepairJobCheckTimeoutInSeconds() {
    return DEFAULT_REPAIR_JOB_CHECK_TIMEOUT_IN_SECONDS;
  }

  int getRepairJobCheckIntervalInSeconds() {
    return DEFAULT_REPAIR_JOB_CHECK_INTERVAL_IN_SECONDS;
  }

  int getMaxRepairPerRound() {
    return maxRepairPerRound;
  }

  int getVersionRefreshThresholdInDays() {
    return versionRefreshThresholdInDays;
  }

  SystemStoreHealthChecker getHealthChecker() {
    return healthChecker;
  }
}
