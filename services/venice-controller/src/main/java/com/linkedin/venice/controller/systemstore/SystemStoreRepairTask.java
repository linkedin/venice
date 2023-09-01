package com.linkedin.venice.controller.systemstore;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.UserSystemStoreLifeCycleHelper;
import com.linkedin.venice.controller.VeniceParentHelixAdmin;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class tries to scan all cluster which current parent controller is the leader controller.
 * It will perform the following action for each system store of each cluster:
 * 1. Check system store is created / has current version.
 * 2. Send heartbeat to system store and check if heartbeat is received.
 * 3. If system store failed any of the check in (1) / (2), it will try to run empty push to repair the system store,
 * until maximum retry of repair is reached.
 * It will emit metrics to indicate bad system store counts per cluster and how many stores are not fixable by the task.
 */
public class SystemStoreRepairTask implements Runnable {
  public static final Logger LOGGER = LogManager.getLogger(SystemStoreRepairTask.class);
  public static final String SYSTEM_STORE_REPAIR_JOB_PREFIX = "CONTROLLER_SYSTEM_STORE_REPAIR_JOB_";
  private static final int SKIP_NEWLY_CREATED_STORE_SYSTEM_STORE_HEALTH_CHECK_HOURS = 2;
  private static final int SYSTEM_STORE_PUSH_STATUS_POLL_INTERVAL_IN_SECONDS = 30;
  private static final int PER_SYSTEM_STORE_HEARTBEAT_CHECK_INTERVAL_IN_MS = 100;

  private final int heartbeatWaitTimeSeconds;
  private final int maxRepairRetry;
  private final VeniceParentHelixAdmin parentAdmin;
  private final AtomicLong badMetaStoreCount = new AtomicLong(0);
  private final AtomicLong badPushStatusStoreCount = new AtomicLong(0);
  private final AtomicBoolean isRunning;

  public SystemStoreRepairTask(
      VeniceParentHelixAdmin parentAdmin,
      int maxRepairRetry,
      int heartbeatWaitTimeSeconds,
      AtomicBoolean isRunning) {
    this.parentAdmin = parentAdmin;
    this.maxRepairRetry = maxRepairRetry;
    this.heartbeatWaitTimeSeconds = heartbeatWaitTimeSeconds;
    this.isRunning = isRunning;
  }

  @Override
  public void run() {
    for (String clusterName: parentAdmin.getClustersLeaderOf()) {
      Set<String> newUnhealthySystemStoreSet = new HashSet<>();
      Map<String, Long> systemStoreToHeartbeatTimestampMap = new VeniceConcurrentHashMap<>();
      // Iterate all system stores and get unhealthy system stores.
      checkSystemStoresHealth(clusterName, newUnhealthySystemStoreSet, systemStoreToHeartbeatTimestampMap);
      // Try repair all bad system stores.
      repairBadSystemStore(clusterName, newUnhealthySystemStoreSet, maxRepairRetry);
    }
  }

  void repairBadSystemStore(String clusterName, Set<String> newUnhealthySystemStoreSet, int maxRepairRetry) {
    for (int i = 0; i < maxRepairRetry; i++) {
      Map<String, Integer> systemStoreToRepairJobVersionMap = new HashMap<>();
      for (String systemStoreName: newUnhealthySystemStoreSet) {
        if (!shouldContinue(clusterName)) {
          return;
        }
        String pushJobId = SYSTEM_STORE_REPAIR_JOB_PREFIX + System.currentTimeMillis();
        try {
          Version version = UserSystemStoreLifeCycleHelper
              .materializeSystemStore(getParentAdmin(), clusterName, systemStoreName, pushJobId);
          systemStoreToRepairJobVersionMap.put(systemStoreName, version.getNumber());
        } catch (Exception e) {
          LOGGER.warn("Unable to run empty push job for store: {} in cluster: {}", systemStoreName, clusterName, e);
        }
      }
      for (Map.Entry<String, Integer> entry: systemStoreToRepairJobVersionMap.entrySet()) {
        if (pollSystemStorePushStatusUntilCompleted(clusterName, entry.getKey(), entry.getValue())) {
          newUnhealthySystemStoreSet.remove(entry.getKey());
          LOGGER.info("System store: {} in cluster: {} has been fixed by repair job.", entry.getKey(), clusterName);
        }
      }
    }
  }

  /**
   * This method iterates over all system stores in the given cluster by sending heartbeat and validate if heartbeat is
   * consumed successfully by system store.
   * At the end, it will update the count of bad meta system stores and bad DaVinci push status stores respectively.
   */
  void checkSystemStoresHealth(
      String clusterName,
      Set<String> newUnhealthySystemStoreSet,
      Map<String, Long> systemStoreToHeartbeatTimestampMap) {
    checkAndSendHeartbeatToSystemStores(clusterName, newUnhealthySystemStoreSet, systemStoreToHeartbeatTimestampMap);
    try {
      // Sleep for enough time for system store to consume heartbeat messages.
      Thread.sleep(TimeUnit.SECONDS.toMillis(heartbeatWaitTimeSeconds));
    } catch (InterruptedException e) {
      LOGGER.info("Caught interrupted exception, will exit now.");
      return;
    }
    checkHeartbeatFromSystemStores(clusterName, newUnhealthySystemStoreSet, systemStoreToHeartbeatTimestampMap);
    updateBadSystemStoreCount(newUnhealthySystemStoreSet);
  }

  /**
   *  Here we scan the store repository for two passes:
   *  1. We check user stores
   */
  void checkAndSendHeartbeatToSystemStores(
      String clusterName,
      Set<String> newUnhealthySystemStoreSet,
      Map<String, Long> systemStoreToHeartbeatTimestampMap) {

    Map<String, Long> userStoreToCreationTimestampMap = new HashMap<>();
    for (Store store: getParentAdmin().getAllStores(clusterName)) {
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
        if (!store.isDaVinciPushStatusStoreEnabled()) {
          newUnhealthySystemStoreSet
              .add(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(store.getName()));
        }
        if (!store.isStoreMetaSystemStoreEnabled()) {
          newUnhealthySystemStoreSet.add(VeniceSystemStoreType.META_STORE.getSystemStoreName(store.getName()));
        }

      }
    }

    for (Store store: getParentAdmin().getAllStores(clusterName)) {
      if (!shouldContinue(clusterName)) {
        return;
      }
      // This pass we only scan user system store.
      if (!(VeniceSystemStoreUtils.isSystemStore(store.getName())
          && VeniceSystemStoreUtils.isUserSystemStore(store.getName()))) {
        continue;
      }
      // We will not check newly created system stores.
      if (isStoreNewlyCreated(userStoreToCreationTimestampMap.getOrDefault(store.getName(), 0L))) {
        continue;
      }
      // System store does not have an online serving version.
      if (store.getCurrentVersion() == 0) {
        newUnhealthySystemStoreSet.add(store.getName());
        continue;
      }

      // Send heartbeat to system store in all child regions.
      long currentTimestamp = System.currentTimeMillis();
      sendHeartbeatToSystemStore(clusterName, store.getName(), currentTimestamp);
      systemStoreToHeartbeatTimestampMap.put(store.getName(), currentTimestamp);

      // Sleep to throttle heartbeat send rate.
      try {
        Thread.sleep(PER_SYSTEM_STORE_HEARTBEAT_CHECK_INTERVAL_IN_MS);
      } catch (InterruptedException e) {
        LOGGER.info("Caught interrupted exception, will exit now.");
        return;
      }
    }
  }

  /**
   * This method iterates over all system stores and validate if heartbeat has been received.
   */
  void checkHeartbeatFromSystemStores(
      String clusterName,
      Set<String> newUnhealthySystemStoreSet,
      Map<String, Long> systemStoreToHeartbeatTimestampMap) {
    for (Map.Entry<String, Long> entry: systemStoreToHeartbeatTimestampMap.entrySet()) {
      if (!shouldContinue(clusterName)) {
        return;
      }
      if (!isHeartbeatReceivedBySystemStore(clusterName, entry.getKey(), entry.getValue())) {
        newUnhealthySystemStoreSet.add(entry.getKey());
      }
    }
  }

  void updateBadSystemStoreCount(Set<String> newUnhealthySystemStoreSet) {
    long newBadMetaSystemStoreCount = newUnhealthySystemStoreSet.stream()
        .filter(x -> VeniceSystemStoreType.getSystemStoreType(x).equals(VeniceSystemStoreType.META_STORE))
        .count();
    getBadMetaStoreCount().set(newBadMetaSystemStoreCount);
    getBadPushStatusStoreCount().set(newUnhealthySystemStoreSet.size() - newBadMetaSystemStoreCount);
    LOGGER.info(
        "Collected unhealthy system stores:{}. Meta system store count: {}, push status system store count: {}",
        newUnhealthySystemStoreSet.toString(),
        getBadMetaStoreCount().get(),
        getBadPushStatusStoreCount().get());
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

  boolean isHeartbeatReceivedBySystemStore(
      String clusterName,
      String systemStoreName,
      long expectedHeartbeatTimestamp) {
    for (Map.Entry<String, ControllerClient> entry: getControllerClientMap(clusterName).entrySet()) {
      long heartbeatFromChildController =
          entry.getValue().getHeartbeatFromSystemStore(systemStoreName).getHeartbeatTimestamp();
      if (heartbeatFromChildController < expectedHeartbeatTimestamp) {
        LOGGER.warn(
            "Expect heartbeat: {} from system store: {} in region: {}, got stale heartbeat: {}.",
            expectedHeartbeatTimestamp,
            systemStoreName,
            entry.getKey(),
            expectedHeartbeatTimestamp);
        return false;
      }
    }
    return true;
  }

  /**
   * Poll the system store push status until it reaches terminal status.
   * Based on system store setup, stuck job should time out in 1h.
   * If push job completes, it will return true, otherwise return false.
   */
  boolean pollSystemStorePushStatusUntilCompleted(String clusterName, String systemStoreName, int version) {
    String kafkaTopic = Version.composeKafkaTopic(systemStoreName, version);
    while (true) {
      if (!shouldContinue(clusterName)) {
        return false;
      }
      Admin.OfflinePushStatusInfo pushStatus = getParentAdmin().getOffLinePushStatus(clusterName, kafkaTopic);
      if (pushStatus.getExecutionStatus().isTerminal()) {
        return pushStatus.getExecutionStatus().equals(ExecutionStatus.COMPLETED);
      }
      try {
        // Sleep for enough time for system store complete ingestion.
        Thread.sleep(TimeUnit.SECONDS.toMillis(SYSTEM_STORE_PUSH_STATUS_POLL_INTERVAL_IN_SECONDS));
      } catch (InterruptedException e) {
        LOGGER.info("Caught interrupted exception, will exit now.");
        return false;
      }
    }
  }

  boolean isStoreNewlyCreated(long creationTimestamp) {
    // Since system store is just created, we can skip checking its system store.
    return (System.currentTimeMillis() - creationTimestamp) < TimeUnit.HOURS
        .toMillis(SKIP_NEWLY_CREATED_STORE_SYSTEM_STORE_HEALTH_CHECK_HOURS);
  }

  AtomicLong getBadMetaStoreCount() {
    return badMetaStoreCount;
  }

  AtomicLong getBadPushStatusStoreCount() {
    return badPushStatusStoreCount;
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
}
