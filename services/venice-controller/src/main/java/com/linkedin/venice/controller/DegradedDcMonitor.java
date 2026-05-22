package com.linkedin.venice.controller;

import com.linkedin.venice.controller.stats.DegradedModeStats;
import com.linkedin.venice.meta.DegradedDcInfo;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Periodic monitor that emits duration metrics for degraded DCs and detects orphaned
 * PARTIALLY_ONLINE versions that need recovery (e.g., after controller leader failover).
 *
 * <p>Duration metrics always emit regardless of auto-recovery config, because alerting on
 * how long a DC has been degraded is valuable even without automatic recovery.
 * Orphan detection and recovery triggering only runs when auto-recovery is enabled.
 */
class DegradedDcMonitor {
  private static final Logger LOGGER = LogManager.getLogger(DegradedDcMonitor.class);
  private static final long DC_MONITOR_INTERVAL_SECONDS = 60;
  private static final long TIMEOUT_ALERT_INTERVAL_MS = TimeUnit.MINUTES.toMillis(10);

  private final Admin admin;
  private final DegradedModeStats stats;
  private final DegradedModeRecoveryService recoveryService;
  private final ScheduledExecutorService scheduler;
  private final VeniceControllerMultiClusterConfig multiClusterConfigs;
  private final AtomicLong lastTimeoutAlertMs = new AtomicLong(0);

  DegradedDcMonitor(
      Admin admin,
      DegradedModeStats stats,
      DegradedModeRecoveryService recoveryService,
      ScheduledExecutorService scheduler,
      VeniceControllerMultiClusterConfig multiClusterConfigs) {
    this.admin = admin;
    this.stats = stats;
    this.recoveryService = recoveryService;
    this.scheduler = scheduler;
    this.multiClusterConfigs = multiClusterConfigs;
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
        // Orphan detection only runs when auto-recovery is enabled for this cluster.
        // Without this gate, clusters with auto.recovery.enabled=false would still get
        // automatic recovery triggered, contradicting the opt-in guarantee.
        VeniceControllerClusterConfig config = multiClusterConfigs.getControllerConfig(clusterName);
        if (config.isDegradedModeAutoRecoveryEnabled()) {
          detectAndRecoverOrphanedVersions(clusterName);
        }
      } catch (Exception e) {
        LOGGER.warn("Error in degraded DC monitor for cluster: {}", clusterName, e);
      }
    }
  }

  private void emitDurationMetrics(String clusterName) {
    Map<String, DegradedDcInfo> degradedDcs = admin.getDegradedDatacenters(clusterName);
    if (degradedDcs.isEmpty()) {
      return;
    }
    long nowMs = System.currentTimeMillis();
    for (Map.Entry<String, DegradedDcInfo> entry: degradedDcs.entrySet()) {
      String dcName = entry.getKey();
      DegradedDcInfo info = entry.getValue();
      double durationMinutes = (nowMs - info.getTimestamp()) / (double) TimeUnit.MINUTES.toMillis(1);

      if (stats != null) {
        stats.recordDegradedDcDurationMinutes(clusterName, dcName, durationMinutes);
      }

      if (durationMinutes > info.getTimeoutMinutes()) {
        // Rate-limit timeout alerts to once every 10 minutes to avoid log spam
        long lastAlert = lastTimeoutAlertMs.get();
        if (nowMs - lastAlert >= TIMEOUT_ALERT_INTERVAL_MS && lastTimeoutAlertMs.compareAndSet(lastAlert, nowMs)) {
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
          // Transient failure (ZK blip, network timeout) — skip this region rather than
          // triggering an expensive recovery based on inconclusive evidence.
          LOGGER.warn(
              "Failed to check version currency for store {} v{} in region {}. Skipping region.",
              sv.storeName,
              sv.version,
              regionName,
              e);
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
