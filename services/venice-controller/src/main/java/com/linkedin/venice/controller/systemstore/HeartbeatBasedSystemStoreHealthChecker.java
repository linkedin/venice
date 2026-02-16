package com.linkedin.venice.controller.systemstore;

import com.linkedin.venice.controller.VeniceParentHelixAdmin;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.utils.LatencyUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Default {@link SystemStoreHealthChecker} implementation that uses the heartbeat write+read cycle to determine
 * system store health. This logic was extracted from {@link SystemStoreRepairTask}.
 *
 * For each store, it sends a heartbeat timestamp to all child regions, then polls periodically until the heartbeat
 * is read back or a timeout is reached. Stores that return a fresh heartbeat are HEALTHY; all others are UNHEALTHY.
 * This implementation never returns UNKNOWN.
 */
public class HeartbeatBasedSystemStoreHealthChecker implements SystemStoreHealthChecker {
  private static final Logger LOGGER = LogManager.getLogger(HeartbeatBasedSystemStoreHealthChecker.class);
  private static final int DEFAULT_HEARTBEAT_CHECK_INTERVAL_IN_SECONDS = 30;
  private static final int DEFAULT_PER_SYSTEM_STORE_HEARTBEAT_CHECK_INTERVAL_IN_MS = 100;
  private static final int DEFAULT_CHECK_LOGGING_COUNT = 100;

  private final VeniceParentHelixAdmin parentAdmin;
  private final int heartbeatWaitTimeInSeconds;
  private final AtomicBoolean isRunning;

  public HeartbeatBasedSystemStoreHealthChecker(
      VeniceParentHelixAdmin parentAdmin,
      int heartbeatWaitTimeInSeconds,
      AtomicBoolean isRunning) {
    this.parentAdmin = parentAdmin;
    this.heartbeatWaitTimeInSeconds = heartbeatWaitTimeInSeconds;
    this.isRunning = isRunning;
  }

  @Override
  public Map<String, HealthCheckResult> checkHealth(String clusterName, Set<String> systemStoreNames) {
    if (systemStoreNames.isEmpty()) {
      return new HashMap<>();
    }

    // Phase 1: Send heartbeats to all stores
    Map<String, Long> storeToHeartbeatTimestamp = new HashMap<>();
    int count = 0;
    long startTimestamp = System.currentTimeMillis();
    for (String storeName: systemStoreNames) {
      if (!shouldContinue(clusterName)) {
        break;
      }
      long currentTimestamp = System.currentTimeMillis();
      sendHeartbeatToSystemStore(clusterName, storeName, currentTimestamp);
      storeToHeartbeatTimestamp.put(storeName, currentTimestamp);
      count++;
      if ((count % DEFAULT_CHECK_LOGGING_COUNT) == 0) {
        LOGGER.info(
            "Sent heartbeat to {} system stores, took: {} ms",
            count,
            LatencyUtils.getElapsedTimeFromMsToMs(startTimestamp));
      }
      LatencyUtils.sleep(DEFAULT_PER_SYSTEM_STORE_HEARTBEAT_CHECK_INTERVAL_IN_MS);
    }

    // Phase 2: Poll for heartbeat reads
    Map<String, HealthCheckResult> results = new HashMap<>();
    // Initialize all sent stores as UNHEALTHY; will flip to HEALTHY if heartbeat comes back
    for (String storeName: storeToHeartbeatTimestamp.keySet()) {
      results.put(storeName, HealthCheckResult.UNHEALTHY);
    }

    checkHeartbeatFromSystemStores(clusterName, storeToHeartbeatTimestamp, results);

    return results;
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

  Map<String, ControllerClient> getControllerClientMap(String clusterName) {
    return parentAdmin.getVeniceHelixAdmin().getControllerClientMap(clusterName);
  }

  void checkHeartbeatFromSystemStores(
      String clusterName,
      Map<String, Long> storeToHeartbeatTimestamp,
      Map<String, HealthCheckResult> results) {
    // Make a mutable copy for polling
    Map<String, Long> pendingStores = new HashMap<>(storeToHeartbeatTimestamp);

    periodicCheckTask(clusterName, heartbeatWaitTimeInSeconds, getHeartbeatCheckIntervalInSeconds(), () -> {
      List<String> listToRemove = new ArrayList<>();
      int checkCount = 0;
      long checkStartTimestamp = System.currentTimeMillis();
      for (Map.Entry<String, Long> entry: pendingStores.entrySet()) {
        if (!shouldContinue(clusterName)) {
          return true;
        }

        long retrievedHeartbeatTimestamp = getHeartbeatFromSystemStore(clusterName, entry.getKey());
        if (retrievedHeartbeatTimestamp >= entry.getValue()) {
          // Fresh heartbeat received
          listToRemove.add(entry.getKey());
          results.put(entry.getKey(), HealthCheckResult.HEALTHY);
        } else {
          if (retrievedHeartbeatTimestamp == -1) {
            LOGGER.warn(
                "System store: {} in cluster: {} is not reachable for heartbeat request.",
                entry.getKey(),
                clusterName);
          } else {
            LOGGER.warn(
                "Expect heartbeat: {} from system store: {} in cluster: {}, got stale heartbeat: {}.",
                entry.getValue(),
                entry.getKey(),
                clusterName,
                retrievedHeartbeatTimestamp);
          }
        }
        checkCount++;
        if ((checkCount % DEFAULT_CHECK_LOGGING_COUNT) == 0) {
          LOGGER.info(
              "Checked heartbeat for {} system stores, took: {} ms",
              checkCount,
              LatencyUtils.getElapsedTimeFromMsToMs(checkStartTimestamp));
        }
      }
      for (String key: listToRemove) {
        pendingStores.remove(key);
      }
      return pendingStores.isEmpty();
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
      LatencyUtils.sleep(TimeUnit.SECONDS.toMillis(checkIntervalInSeconds));
    }
  }

  boolean shouldContinue(String clusterName) {
    if (!isRunning.get()) {
      return false;
    }
    return parentAdmin.isLeaderControllerFor(clusterName);
  }

  int getHeartbeatCheckIntervalInSeconds() {
    return DEFAULT_HEARTBEAT_CHECK_INTERVAL_IN_SECONDS;
  }

  VeniceParentHelixAdmin getParentAdmin() {
    return parentAdmin;
  }
}
