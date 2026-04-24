package com.linkedin.venice.controller;

import com.linkedin.venice.controller.stats.DegradedModeStats;
import com.linkedin.venice.meta.DegradedDcInfo;
import com.linkedin.venice.meta.DegradedDcStates;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Periodic monitor that emits duration metrics for degraded DCs and detects orphaned
 * PARTIALLY_ONLINE versions that need recovery (e.g., after controller leader failover).
 */
class DegradedDcMonitor {
  private static final Logger LOGGER = LogManager.getLogger(DegradedDcMonitor.class);
  private static final long DC_MONITOR_INTERVAL_SECONDS = 60;

  private final Admin admin;
  private final DegradedModeStats stats;
  private final DegradedModeRecoveryService recoveryService;
  private final ScheduledExecutorService scheduler;

  DegradedDcMonitor(
      Admin admin,
      DegradedModeStats stats,
      DegradedModeRecoveryService recoveryService,
      ScheduledExecutorService scheduler) {
    this.admin = admin;
    this.stats = stats;
    this.recoveryService = recoveryService;
    this.scheduler = scheduler;
  }

  /** Start the periodic monitor that emits duration metrics for degraded DCs. */
  void start(Set<String> clusterNames) {
    start(clusterNames, DC_MONITOR_INTERVAL_SECONDS, TimeUnit.SECONDS);
  }

  // Visible for testing
  void start(Set<String> clusterNames, long interval, TimeUnit unit) {
    scheduler.scheduleAtFixedRate(() -> runMonitorCycle(clusterNames), interval, interval, unit);
    LOGGER.info("Started degraded DC duration monitor for clusters: {}", clusterNames);
  }

  private void runMonitorCycle(Set<String> clusterNames) {
    for (String clusterName: clusterNames) {
      try {
        emitDurationMetrics(clusterName);
        detectAndRecoverOrphanedVersions(clusterName);
      } catch (Exception e) {
        LOGGER.warn("Error in degraded DC monitor for cluster: {}", clusterName, e);
      }
    }
  }

  private void emitDurationMetrics(String clusterName) {
    DegradedDcStates degradedDcStates = admin.getDegradedDcStates(clusterName);
    if (degradedDcStates.isEmpty()) {
      return;
    }
    long nowMs = System.currentTimeMillis();
    for (Map.Entry<String, DegradedDcInfo> entry: degradedDcStates.getDegradedDatacenters().entrySet()) {
      String dcName = entry.getKey();
      DegradedDcInfo info = entry.getValue();
      double durationMinutes = (nowMs - info.getTimestamp()) / 60_000.0;

      if (stats != null) {
        stats.recordDegradedDcDurationMinutes(clusterName, dcName, durationMinutes);
      }

      if (durationMinutes > info.getTimeoutMinutes()) {
        LOGGER.warn(
            "ALERT: DC {} in cluster {} degraded for {} min (timeout: {} min). Operator: {}",
            dcName,
            clusterName,
            (long) durationMinutes,
            info.getTimeoutMinutes(),
            info.getOperatorId());
      }
    }
  }

  /**
   * Scans for PARTIALLY_ONLINE versions with no active recovery (e.g., after leader failover).
   * Re-triggers recovery — the flow is idempotent so re-triggering is always safe.
   */
  void detectAndRecoverOrphanedVersions(String clusterName) {
    List<RecoveryProgress.StoreVersionPair> orphaned = recoveryService.findPartiallyOnlineStores(clusterName);
    if (orphaned.isEmpty()) {
      return;
    }

    Map<String, String> allRegions = admin.getChildDataCenterControllerUrlMap(clusterName);
    for (String regionName: allRegions.keySet()) {
      RecoveryProgress existing = recoveryService.getRecoveryProgress(clusterName, regionName);
      if (existing != null && !existing.isComplete()) {
        continue;
      }

      boolean regionNeedsRecovery = false;
      for (RecoveryProgress.StoreVersionPair sv: orphaned) {
        try {
          if (!admin.isVersionCurrentInRegion(clusterName, sv.storeName, sv.version, regionName)) {
            regionNeedsRecovery = true;
            break;
          }
        } catch (Exception e) {
          regionNeedsRecovery = true;
          break;
        }
      }

      if (regionNeedsRecovery) {
        LOGGER.warn(
            "Orphaned PARTIALLY_ONLINE in cluster {} region {}. Re-triggering recovery for {} stores.",
            clusterName,
            regionName,
            orphaned.size());
        recoveryService.triggerRecovery(clusterName, regionName);
      }
    }
  }
}
