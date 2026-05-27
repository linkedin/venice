package com.linkedin.venice.controller;

import com.linkedin.venice.controller.stats.DegradedModeStats;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Orchestrates bulk data recovery for PARTIALLY_ONLINE stores when a degraded DC is unmarked.
 * Uses prepare → poll readiness → initiate flow with bounded concurrency and retry.
 */
public class DegradedModeRecoveryService implements Closeable {
  private static final Logger LOGGER = LogManager.getLogger(DegradedModeRecoveryService.class);

  static final int DEFAULT_RECOVERY_THREAD_POOL_SIZE = 5;

  private final Admin admin;
  private final DegradedModeStats stats;
  private final Map<String, RecoveryProgress> activeRecoveries = new VeniceConcurrentHashMap<>();
  private final ExecutorService recoveryExecutor;
  private final ExecutorService monitorExecutor;
  private final ScheduledExecutorService degradedDcMonitor;
  private final DegradedDcMonitor dcMonitor;
  private final StoreRecoveryExecutor storeRecoveryExecutor;

  public DegradedModeRecoveryService(Admin admin, DegradedModeStats stats) {
    this(admin, stats, DEFAULT_RECOVERY_THREAD_POOL_SIZE, null);
  }

  public DegradedModeRecoveryService(
      Admin admin,
      DegradedModeStats stats,
      int threadPoolSize,
      VeniceControllerMultiClusterConfig multiClusterConfigs) {
    this.admin = admin;
    this.stats = stats;
    this.storeRecoveryExecutor = new StoreRecoveryExecutor(admin, stats);
    int effectivePoolSize = Math.max(1, threadPoolSize);
    this.recoveryExecutor = Executors.newFixedThreadPool(effectivePoolSize, runnable -> {
      Thread t = new Thread(runnable);
      t.setDaemon(true);
      t.setName("degraded-mode-recovery-" + t.getId());
      return t;
    });
    // Bounded pool for recovery monitor tasks (one per concurrent DC recovery)
    this.monitorExecutor = Executors.newFixedThreadPool(Math.max(1, effectivePoolSize), runnable -> {
      Thread t = new Thread(runnable);
      t.setDaemon(true);
      t.setName("degraded-mode-monitor-" + t.getId());
      return t;
    });
    this.degradedDcMonitor = Executors.newSingleThreadScheduledExecutor(runnable -> {
      Thread t = new Thread(runnable);
      t.setDaemon(true);
      t.setName("degraded-dc-duration-monitor");
      return t;
    });
    this.dcMonitor = new DegradedDcMonitor(admin, stats, this, this.degradedDcMonitor, multiClusterConfigs);
  }

  /** Start the periodic monitor that emits duration metrics for degraded DCs. */
  public void startDegradedDcMonitor(Set<String> clusterNames) {
    dcMonitor.start(clusterNames);
  }

  // Visible for testing
  void startDegradedDcMonitor(Set<String> clusterNames, long interval, TimeUnit unit) {
    dcMonitor.start(clusterNames, interval, unit);
  }

  /** Trigger async recovery for all PARTIALLY_ONLINE stores in the given datacenter. */
  public void triggerRecovery(String clusterName, String datacenterName) {
    // Atomic check-and-replace: allows re-trigger after completion, prevents concurrent duplicates.
    String recoveryKey = clusterName + "/" + datacenterName;
    RecoveryProgress[] holder = new RecoveryProgress[1];
    activeRecoveries.compute(recoveryKey, (key, existing) -> {
      if (existing != null && !existing.isComplete()) {
        // Recovery already in progress — keep the existing entry
        holder[0] = null;
        return existing;
      }
      // No entry or completed entry — create a new one
      RecoveryProgress progress = new RecoveryProgress(datacenterName);
      holder[0] = progress;
      return progress;
    });
    RecoveryProgress progress = holder[0];
    if (progress == null) {
      LOGGER.warn("Recovery already in progress for datacenter: {}", datacenterName);
      return;
    }

    List<RecoveryProgress.StoreVersionPair> affected = findPartiallyOnlineStores(clusterName);
    progress.setTotalStores(affected.size());

    if (affected.isEmpty()) {
      LOGGER.info("No PARTIALLY_ONLINE stores found for datacenter: {}. Recovery complete.", datacenterName);
      progress.markComplete();
      logPostRecoveryActions(clusterName, datacenterName, progress);
      return;
    }

    LOGGER.info(
        "Triggering recovery for {} stores in datacenter: {} (cluster: {})",
        affected.size(),
        datacenterName,
        clusterName);
    List<Future<?>> futures = new ArrayList<>();
    for (RecoveryProgress.StoreVersionPair sv: affected) {
      futures.add(
          recoveryExecutor
              .submit(() -> storeRecoveryExecutor.recoverSingleStore(clusterName, datacenterName, sv, progress)));
    }

    // Submit monitor task to bounded pool instead of spawning raw threads
    monitorExecutor.submit(() -> {
      try {
        for (Future<?> f: futures) {
          try {
            f.get();
          } catch (Exception e) {
            LOGGER.error("Unexpected error waiting for recovery future in datacenter: {}", datacenterName, e);
          }
        }
        LOGGER.info(
            "Recovery initiations complete for datacenter: {}. Recovered: {}, Failed: {}, Total: {}",
            datacenterName,
            progress.getRecoveredStores(),
            progress.getFailedStores(),
            progress.getTotalStores());

        // Phase 2: Wait for child DC to confirm recovery completion, then transition versions
        confirmRecoveryAndTransitionVersions(clusterName, datacenterName, progress);
      } finally {
        progress.markComplete();
        if (stats != null) {
          stats.recordRecoveryProgress(clusterName, datacenterName, progress.getProgressFraction());
        }
        logPostRecoveryActions(clusterName, datacenterName, progress);
      }
    });
  }

  /** Phase 2: Poll child DC to confirm recovery, then transition PARTIALLY_ONLINE → ONLINE. */
  void confirmRecoveryAndTransitionVersions(String clusterName, String datacenterName, RecoveryProgress progress) {
    // Snapshot: Phase 1 futures are complete but list is shared mutable state.
    List<RecoveryProgress.StoreVersionPair> initiatedStores = new ArrayList<>(progress.getInitiatedStores());
    if (initiatedStores.isEmpty()) {
      return;
    }

    LOGGER.info(
        "Starting recovery completion monitoring for {} stores in datacenter: {}",
        initiatedStores.size(),
        datacenterName);
    List<Future<?>> confirmFutures = new ArrayList<>();
    for (RecoveryProgress.StoreVersionPair sv: initiatedStores) {
      confirmFutures.add(recoveryExecutor.submit(() -> {
        try {
          StoreRecoveryExecutor.VersionPollResult result =
              storeRecoveryExecutor.pollUntilVersionCurrent(clusterName, sv, datacenterName);
          switch (result) {
            case CURRENT:
              admin.updateStoreVersionStatus(clusterName, sv.storeName, sv.version, VersionStatus.ONLINE);
              progress.incrementVersionsTransitioned();
              if (stats != null) {
                stats.recordRecoveryVersionTransitioned(clusterName, sv.storeName);
                stats.recordRecoveryProgress(clusterName, datacenterName, progress.getProgressFraction());
              }
              LOGGER.info(
                  "Transitioned store {} v{} from PARTIALLY_ONLINE to ONLINE after recovery in datacenter: {}",
                  sv.storeName,
                  sv.version,
                  datacenterName);
              break;
            case SUPERSEDED:
              progress.incrementVersionsTransitioned();
              LOGGER.info(
                  "Store {} v{} superseded by newer version in datacenter: {}. Skipping version transition.",
                  sv.storeName,
                  sv.version,
                  datacenterName);
              break;
            case TIMED_OUT:
              if (stats != null) {
                stats.recordRecoveryStoreFailure(clusterName, sv.storeName);
              }
              LOGGER.warn(
                  "Recovery completion timed out for store {} v{} in datacenter: {}. "
                      + "Version remains PARTIALLY_ONLINE. Manual intervention may be needed.",
                  sv.storeName,
                  sv.version,
                  datacenterName);
              break;
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          LOGGER.warn(
              "Recovery confirmation interrupted for store {} v{} in datacenter: {}",
              sv.storeName,
              sv.version,
              datacenterName);
        } catch (Exception e) {
          LOGGER.error(
              "Error confirming recovery for store {} v{} in datacenter: {}",
              sv.storeName,
              sv.version,
              datacenterName,
              e);
        }
      }));
    }

    // Wait for all confirmation tasks
    for (Future<?> f: confirmFutures) {
      try {
        f.get();
      } catch (Exception e) {
        LOGGER.error("Unexpected error in recovery confirmation for datacenter: {}", datacenterName, e);
      }
    }
    progress.getInitiatedStores().clear();
  }

  // Visible for testing — forwards to the executor.
  void setRecoveryCompletionPollParameters(long intervalMs, int maxAttempts) {
    storeRecoveryExecutor.setRecoveryCompletionPollParameters(intervalMs, maxAttempts);
  }

  List<RecoveryProgress.StoreVersionPair> findPartiallyOnlineStores(String clusterName) {
    List<RecoveryProgress.StoreVersionPair> result = new ArrayList<>();
    List<Store> allStores = admin.getAllStores(clusterName);
    for (Store store: allStores) {
      // Only recover the highest PARTIALLY_ONLINE version per store to avoid
      // conflicting recovery attempts on the same store
      int highestPartiallyOnlineVersion = -1;
      for (Version version: store.getVersions()) {
        if (version.getStatus() == VersionStatus.PARTIALLY_ONLINE
            && version.getNumber() > highestPartiallyOnlineVersion) {
          highestPartiallyOnlineVersion = version.getNumber();
        }
      }
      if (highestPartiallyOnlineVersion > 0) {
        result.add(new RecoveryProgress.StoreVersionPair(store.getName(), highestPartiallyOnlineVersion));
      }
    }
    return result;
  }

  private void logPostRecoveryActions(String clusterName, String datacenterName, RecoveryProgress progress) {
    LOGGER.info(
        "Recovery summary for datacenter: {} in cluster: {} — Total: {}, Initiated: {}, Failed: {}, "
            + "Versions transitioned to ONLINE: {}",
        datacenterName,
        clusterName,
        progress.getTotalStores(),
        progress.getRecoveredStores(),
        progress.getFailedStores(),
        progress.getVersionsTransitioned());

    if (progress.getFailedStores() == 0 && progress.getTotalStores() > 0) {
      Optional<String> emergencySourceRegion = admin.getEmergencySourceRegion(clusterName);
      if (emergencySourceRegion.isPresent() && !emergencySourceRegion.get().isEmpty()) {
        LOGGER.warn(
            "ACTION REQUIRED: All stores recovered successfully for datacenter: {}. "
                + "The cluster-level emergencySourceRegion is still set to '{}'. "
                + "An operator should clear this config to restore normal NR source fabric selection.",
            datacenterName,
            emergencySourceRegion.get());
      }
    }
  }

  public RecoveryProgress getRecoveryProgress(String clusterName, String datacenterName) {
    return activeRecoveries.get(clusterName + "/" + datacenterName);
  }

  @Override
  public void close() {
    degradedDcMonitor.shutdownNow();
    monitorExecutor.shutdownNow();
    recoveryExecutor.shutdownNow();
    try {
      degradedDcMonitor.awaitTermination(30, TimeUnit.SECONDS);
      monitorExecutor.awaitTermination(30, TimeUnit.SECONDS);
      recoveryExecutor.awaitTermination(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

}
