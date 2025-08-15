package com.linkedin.venice.controller.systemstore;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.UserSystemStoreLifeCycleHelper;
import com.linkedin.venice.controller.VeniceParentHelixAdmin;
import com.linkedin.venice.controller.stats.SystemStoreHealthCheckStats;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.LogContext;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
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
 */
public class SystemStoreRepairTask implements Runnable {
  public static final Logger LOGGER = LogManager.getLogger(SystemStoreRepairTask.class);
  public static final String SYSTEM_STORE_REPAIR_JOB_PREFIX = "CONTROLLER_SYSTEM_STORE_REPAIR_JOB_";
  private static final int DEFAULT_SKIP_NEWLY_CREATED_STORE_SYSTEM_STORE_HEALTH_CHECK_IN_HOURS = 2;
  private static final int DEFAULT_REPAIR_JOB_CHECK_TIMEOUT_IN_SECONDS = 3600;
  private static final int DEFAULT_REPAIR_JOB_CHECK_INTERVAL_IN_SECONDS = 30;
  private static final int DEFAULT_HEARTBEAT_CHECK_INTERVAL_IN_SECONDS = 30;
  private static final int DEFAULT_PER_SYSTEM_STORE_HEARTBEAT_CHECK_INTERVAL_IN_MS = 100;

  private final int heartbeatWaitTimeInSeconds;
  private final int versionRefreshThresholdInDays;
  private final VeniceParentHelixAdmin parentAdmin;
  private final AtomicBoolean isRunning;
  private final Map<String, SystemStoreHealthCheckStats> clusterToSystemStoreHealthCheckStatsMap;

  public SystemStoreRepairTask(
      VeniceParentHelixAdmin parentAdmin,
      Map<String, SystemStoreHealthCheckStats> clusterToSystemStoreHealthCheckStatsMap,
      int heartbeatWaitTimeInSeconds,
      int versionRefreshThresholdInDays,
      AtomicBoolean isRunning) {
    this.parentAdmin = parentAdmin;
    this.clusterToSystemStoreHealthCheckStatsMap = clusterToSystemStoreHealthCheckStatsMap;
    this.heartbeatWaitTimeInSeconds = heartbeatWaitTimeInSeconds;
    this.versionRefreshThresholdInDays = versionRefreshThresholdInDays;
    this.isRunning = isRunning;
  }

  @Override
  public void run() {
    LogContext.setLogContext(getParentAdmin().getLogContext());
    for (String clusterName: getParentAdmin().getClustersLeaderOf()) {
      if (!getClusterToSystemStoreHealthCheckStatsMap().containsKey(clusterName)) {
        continue;
      }
      Set<String> unhealthySystemStoreSet = new HashSet<>();
      Set<String> unreachableSystemStoreSet = new HashSet<>();
      Map<String, Long> systemStoreToHeartbeatTimestampMap = new VeniceConcurrentHashMap<>();
      // Iterate all system stores and get unhealthy system stores.
      checkSystemStoresHealth(
          clusterName,
          unhealthySystemStoreSet,
          unreachableSystemStoreSet,
          systemStoreToHeartbeatTimestampMap);
      // Try repair all bad system stores.
      repairBadSystemStore(clusterName, unhealthySystemStoreSet);
    }
  }

  void repairBadSystemStore(String clusterName, Set<String> unhealthySystemStoreSet) {
    Map<String, Integer> systemStoreToRepairJobVersionMap = new HashMap<>();
    for (String systemStoreName: unhealthySystemStoreSet) {
      if (!shouldContinue(clusterName)) {
        return;
      }
      String pushJobId = SYSTEM_STORE_REPAIR_JOB_PREFIX + System.currentTimeMillis();
      try {
        Version version = getNewSystemStoreVersion(clusterName, systemStoreName, pushJobId);
        systemStoreToRepairJobVersionMap.put(systemStoreName, version.getNumber());
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
    updateNotRepairableSystemStoreCount(clusterName, unhealthySystemStoreSet);
  }

  /**
   * This method iterates over all system stores in the given cluster by sending heartbeat and validate if heartbeat is
   * consumed successfully by system store.
   * At the end, it will update the count of bad meta system stores and bad DaVinci push status stores respectively.
   */
  void checkSystemStoresHealth(
      String clusterName,
      Set<String> unhealthySystemStoreSet,
      Set<String> unreachableSystemStoreSet,
      Map<String, Long> systemStoreToHeartbeatTimestampMap) {
    checkAndSendHeartbeatToSystemStores(clusterName, unhealthySystemStoreSet, systemStoreToHeartbeatTimestampMap);
    checkHeartbeatFromSystemStores(
        clusterName,
        unhealthySystemStoreSet,
        unreachableSystemStoreSet,
        systemStoreToHeartbeatTimestampMap,
        getHeartbeatWaitTimeInSeconds());
    updateBadSystemStoreCount(clusterName, unhealthySystemStoreSet);
  }

  /**
   *  Here we scan the store repository for two passes:
   *  1. We check user stores and see if system stores are created or not.
   *  2. For created system stores, we will check if version needs refresh based on creation time. For stores with stale
   *  versions, it will directly be added into repair set. It will send heartbeat request to the remaining system stores.
   *  During the two passes, stores they are undergoing migration will be skipped.
   */
  void checkAndSendHeartbeatToSystemStores(
      String clusterName,
      Set<String> newUnhealthySystemStoreSet,
      Map<String, Long> systemStoreToHeartbeatTimestampMap) {

    Map<String, Long> userStoreToCreationTimestampMap = new HashMap<>();
    List<Store> storeList = getParentAdmin().getAllStores(clusterName);
    // First pass only check user store and directly add non-existed system stores.
    for (Store store: storeList) {
      if (!shouldContinue(clusterName)) {
        return;
      }

      // For user store, if corresponding system store flag is not true, it indicates system store is not created.
      if (!VeniceSystemStoreUtils.isSystemStore(store.getName())) {
        userStoreToCreationTimestampMap
            .put(VeniceSystemStoreType.META_STORE.getSystemStoreName(store.getName()), store.getCreatedTime());
        userStoreToCreationTimestampMap.put(
            VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(store.getName()),
            store.getCreatedTime());
        // We will not check newly created system stores.
        if (isStoreNewlyCreated(store.getCreatedTime())) {
          continue;
        }

        // Skipping migration store.
        if (store.isMigrating()) {
          LOGGER.info("Store: {} is being migrated, will skip it for now.", store.getName());
          continue;
        }

        if (!store.isDaVinciPushStatusStoreEnabled()) {
          newUnhealthySystemStoreSet
              .add(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(store.getName()));
        }
        if (!store.isStoreMetaSystemStoreEnabled()) {
          newUnhealthySystemStoreSet.add(VeniceSystemStoreType.META_STORE.getSystemStoreName(store.getName()));
        }
      }
    }
    // Second pass only check system store and potentially send heartbeat message to validate health.
    for (Store store: storeList) {
      if (!shouldContinue(clusterName)) {
        return;
      }

      // This pass we only scan user system store.
      if (!(VeniceSystemStoreUtils.isSystemStore(store.getName())
          && VeniceSystemStoreUtils.isUserSystemStore(store.getName()))) {
        continue;
      }

      // Skipping migration store.
      if (store.isMigrating()) {
        LOGGER.info("Store: {} is being migrated, will skip it for now.", store.getName());
        continue;
      }

      // We will not check newly created system stores.
      if (isStoreNewlyCreated(userStoreToCreationTimestampMap.getOrDefault(store.getName(), 0L))) {
        continue;
      }

      // Skip checking store that is already added in the unhealthy list in the previous scan.
      if (newUnhealthySystemStoreSet.contains(store.getName())) {
        continue;
      }

      /**
       * Checking the largest version's age and add stale version to the repair service. Note that in parent region,
       * there is no current version information, here we use the latest version by the creation time to compute the
       * version age.
       */
      List<Version> storeVersionList = store.getVersions();

      // If no version is found, we will default to 0.
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
        newUnhealthySystemStoreSet.add(store.getName());
        continue;
      }

      // Send heartbeat to system store in all child regions.
      long currentTimestamp = System.currentTimeMillis();
      sendHeartbeatToSystemStore(clusterName, store.getName(), currentTimestamp);
      systemStoreToHeartbeatTimestampMap.put(store.getName(), currentTimestamp);

      // Sleep to throttle heartbeat send rate.
      LatencyUtils.sleep(DEFAULT_PER_SYSTEM_STORE_HEARTBEAT_CHECK_INTERVAL_IN_MS);
    }
  }

  /**
   * This method iterates over all system stores and validate if heartbeat has been received.
   */
  void checkHeartbeatFromSystemStores(
      String clusterName,
      Set<String> newUnhealthySystemStoreSet,
      Set<String> unreachableSystemStoreSet,
      Map<String, Long> systemStoreToHeartbeatTimestampMap,
      int heartbeatCheckWaitTimeoutInSeconds) {
    periodicCheckTask(clusterName, heartbeatCheckWaitTimeoutInSeconds, getHeartbeatCheckIntervalInSeconds(), () -> {
      List<String> listToRemove = new ArrayList<>();
      for (Map.Entry<String, Long> entry: systemStoreToHeartbeatTimestampMap.entrySet()) {
        if (!shouldContinue(clusterName)) {
          return true;
        }

        long retrievedHeartbeatTimestamp = getHeartbeatFromSystemStore(clusterName, entry.getKey());
        if (retrievedHeartbeatTimestamp < entry.getValue()) {
          newUnhealthySystemStoreSet.add(entry.getKey());
          if (retrievedHeartbeatTimestamp == -1) {
            unreachableSystemStoreSet.add(entry.getKey());
            LOGGER.warn(
                "System store: {} in cluster: {} is not reachable for heartbeat request.",
                entry.getKey(),
                clusterName);
          } else {
            LOGGER.warn(
                "Expect heartbeat: {} from system store: {} in cluster: {}, got stale heartbeat: {}.",
                entry.getValue(),
                clusterName,
                entry.getKey(),
                retrievedHeartbeatTimestamp);
          }
        } else {
          // Retrieved fresh heartbeat message, will not check anymore.
          listToRemove.add(entry.getKey());
        }
      }
      for (String key: listToRemove) {
        systemStoreToHeartbeatTimestampMap.remove(key);
        newUnhealthySystemStoreSet.remove(key);
        unreachableSystemStoreSet.remove(key);
      }
      return systemStoreToHeartbeatTimestampMap.isEmpty();
    });
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
        .filter(x -> VeniceSystemStoreType.getSystemStoreType(x).equals(VeniceSystemStoreType.META_STORE))
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

  public Map<String, ControllerClient> getControllerClientMap(String clusterName) {
    return getParentAdmin().getVeniceHelixAdmin().getControllerClientMap(clusterName);
  }

  void sendHeartbeatToSystemStore(String clusterName, String systemStoreName, long heartbeatTimestamp) {
    for (Map.Entry<String, ControllerClient> entry: getControllerClientMap(clusterName).entrySet()) {
      entry.getValue().sendHeartbeatToSystemStore(systemStoreName, heartbeatTimestamp);
    }
  }

  long getHeartbeatFromSystemStore(String clusterName, String systemStoreName) {
    long oldestHeartbeatTimestamp = Long.MAX_VALUE;
    for (Map.Entry<String, ControllerClient> entry: getControllerClientMap(clusterName).entrySet()) {
      long timestamp = entry.getValue().getHeartbeatFromSystemStore(systemStoreName).getHeartbeatTimestamp();
      if (oldestHeartbeatTimestamp > timestamp) {
        oldestHeartbeatTimestamp = timestamp;
      }
    }
    return oldestHeartbeatTimestamp;
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
      LOGGER.warn("Finish all repair job.");
    } else {
      LOGGER.warn("Time out waiting repair job for: {}", systemStoreToRepairJobVersionMap.keySet());
    }
  }

  boolean isStoreNewlyCreated(long creationTimestamp) {
    // Since system store is just created, we can skip checking its system store.
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

  int getHeartbeatCheckIntervalInSeconds() {
    return DEFAULT_HEARTBEAT_CHECK_INTERVAL_IN_SECONDS;
  }

  int getHeartbeatWaitTimeInSeconds() {
    return heartbeatWaitTimeInSeconds;
  }

  int getVersionRefreshThresholdInDays() {
    return versionRefreshThresholdInDays;
  }
}
