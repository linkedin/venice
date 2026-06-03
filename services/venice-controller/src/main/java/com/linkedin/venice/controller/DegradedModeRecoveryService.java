package com.linkedin.venice.controller;

import com.linkedin.venice.controller.stats.DegradedModeStats;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.utils.RegionUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
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
  // Single-thread executor for Phase 2 (recovery completion polling). Phase 2 polls per-store
  // every recoveryCompletionPollIntervalMs for up to recoveryCompletionPollMaxAttempts attempts
  // (default 6 hours per store). Using a dedicated single thread — rather than per-store futures
  // on the shared recoveryExecutor — keeps Phase 1 of subsequent DC recoveries unblocked even
  // when one DC's Phase 2 is dragging on.
  private final ExecutorService phase2Executor;
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
    this.phase2Executor = Executors.newSingleThreadExecutor(runnable -> {
      Thread t = new Thread(runnable);
      t.setDaemon(true);
      t.setName("degraded-mode-phase2-poller");
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

    List<RecoveryProgress.StoreVersionPair> affected = findPartiallyOnlineStores(clusterName, datacenterName);
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

    // Submit monitor task to bounded pool instead of spawning raw threads.
    // Phase 1 waits here for all per-store initiations to complete. Phase 2 is then handed off
    // to the dedicated single-thread phase2Executor and finalizes the progress asynchronously.
    monitorExecutor.submit(() -> {
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

      // Phase 2: confirm recovery completion + transition versions. Runs asynchronously on the
      // dedicated phase2Executor; it owns progress.markComplete() and the post-recovery actions
      // when it finishes. If there's nothing to confirm, finalize here.
      List<RecoveryProgress.StoreVersionPair> initiatedSnapshot = new ArrayList<>(progress.getInitiatedStores());
      if (initiatedSnapshot.isEmpty()) {
        finalizeRecovery(clusterName, datacenterName, progress);
      } else {
        confirmRecoveryAndTransitionVersions(clusterName, datacenterName, progress);
      }
    });
  }

  /**
   * Phase 2: confirm recovery completion + transition versions. Schedules a single task on the
   * dedicated {@link #phase2Executor} that iterates all initiated stores per tick:
   * <ul>
   *   <li>If the child DC reports the version is current, transition the parent version to
   *       ONLINE and drop the store from the pending set.</li>
   *   <li>If the child DC reports a newer version (superseded), count it as transitioned and
   *       drop it.</li>
   *   <li>Otherwise increment that store's attempt count; if it hits the max, record failure
   *       and drop it.</li>
   * </ul>
   * Loops until the pending set is empty, sleeping {@code recoveryCompletionPollIntervalMs}
   * between ticks. When the loop finishes (or is interrupted by service shutdown), the progress
   * is finalized.
   *
   * <p>Previously this method ran one Future per store on the shared {@code recoveryExecutor},
   * which held a thread for up to 6 hours per store. The reviewer asked us to consolidate to a
   * single thread so Phase 1 of subsequent DC recoveries is not blocked by Phase 2 stragglers.
   */
  void confirmRecoveryAndTransitionVersions(String clusterName, String datacenterName, RecoveryProgress progress) {
    // Snapshot: Phase 1 futures are complete but the list is shared mutable state.
    List<RecoveryProgress.StoreVersionPair> initiatedStores = new ArrayList<>(progress.getInitiatedStores());
    if (initiatedStores.isEmpty()) {
      finalizeRecovery(clusterName, datacenterName, progress);
      return;
    }

    LOGGER.info(
        "Starting recovery completion monitoring for {} stores in datacenter: {}",
        initiatedStores.size(),
        datacenterName);
    phase2Executor.submit(() -> runPhase2Loop(clusterName, datacenterName, progress, initiatedStores));
  }

  private void runPhase2Loop(
      String clusterName,
      String datacenterName,
      RecoveryProgress progress,
      List<RecoveryProgress.StoreVersionPair> initiatedStores) {
    long pollStartMs = System.currentTimeMillis();
    Map<RecoveryProgress.StoreVersionPair, Integer> attemptCounts = new HashMap<>();
    List<RecoveryProgress.StoreVersionPair> pending = new ArrayList<>(initiatedStores);
    try {
      while (!pending.isEmpty()) {
        Iterator<RecoveryProgress.StoreVersionPair> it = pending.iterator();
        while (it.hasNext()) {
          RecoveryProgress.StoreVersionPair sv = it.next();
          int attempts = attemptCounts.merge(sv, 1, Integer::sum);
          try {
            StoreRecoveryExecutor.VersionPollResult result = storeRecoveryExecutor
                .checkAndMaybeTransition(clusterName, sv, datacenterName, progress, attempts, pollStartMs);
            if (result != StoreRecoveryExecutor.VersionPollResult.PENDING) {
              it.remove();
            }
          } catch (Exception e) {
            // Per-store failure must not kill the whole loop. Leave in pending; next tick retries
            // and the attempt counter still advances toward TIMED_OUT.
            LOGGER.error(
                "Error during Phase 2 check for store {} v{} in datacenter: {}",
                sv.storeName,
                sv.version,
                datacenterName,
                e);
          }
        }
        if (pending.isEmpty()) {
          break;
        }
        try {
          Thread.sleep(storeRecoveryExecutor.getRecoveryCompletionPollIntervalMs());
        } catch (InterruptedException e) {
          // Service shutdown: do NOT mark as failure for the remaining stores. Re-interrupt and
          // bail; the next leader (or a controller restart) will retrigger via the
          // DegradedDcMonitor orphan-detection path. Recovery for in-flight stores stays at
          // PARTIALLY_ONLINE and is correctly attributable.
          Thread.currentThread().interrupt();
          LOGGER.info(
              "Phase 2 poll loop interrupted for datacenter: {}; {} stores left unprocessed and will be "
                  + "re-detected by the orphan monitor or on leader failover",
              datacenterName,
              pending.size());
          return;
        }
      }
    } finally {
      progress.getInitiatedStores().clear();
      finalizeRecovery(clusterName, datacenterName, progress);
    }
  }

  /** Mark the recovery complete, emit the final progress metric, and log post-recovery actions. */
  private void finalizeRecovery(String clusterName, String datacenterName, RecoveryProgress progress) {
    progress.markComplete();
    if (stats != null) {
      stats.recordRecoveryProgress(clusterName, datacenterName, progress.getProgressFraction());
    }
    logPostRecoveryActions(clusterName, datacenterName, progress);
  }

  // Visible for testing — forwards to the executor.
  void setRecoveryCompletionPollParameters(long intervalMs, int maxAttempts) {
    storeRecoveryExecutor.setRecoveryCompletionPollParameters(intervalMs, maxAttempts);
  }

  /**
   * Returns the highest PARTIALLY_ONLINE version per store that needs recovery in {@code datacenterName}.
   * A version qualifies only when:
   * <ul>
   *   <li>{@link Version#isDegradedPush()} is true (the parent auto-converted this push during
   *       degraded mode), AND</li>
   *   <li>{@code datacenterName} was excluded from the push, i.e. it is NOT in
   *       {@link Version#getTargetSwapRegion()}.</li>
   * </ul>
   * This filters out PARTIALLY_ONLINE versions that came from other sources (DVSS partial
   * rollforward, rollbacks) and degraded-mode pushes that already include the recovering DC.
   */
  List<RecoveryProgress.StoreVersionPair> findPartiallyOnlineStores(String clusterName, String datacenterName) {
    List<RecoveryProgress.StoreVersionPair> result = new ArrayList<>();
    List<Store> allStores = admin.getAllStores(clusterName);
    for (Store store: allStores) {
      int highestPartiallyOnlineVersion = -1;
      for (Version version: store.getVersions()) {
        if (version.getStatus() != VersionStatus.PARTIALLY_ONLINE) {
          continue;
        }
        if (!version.isDegradedPush()) {
          continue;
        }
        String targetSwapRegion = version.getTargetSwapRegion();
        if (targetSwapRegion == null || targetSwapRegion.isEmpty()) {
          continue;
        }
        Set<String> includedDcs = RegionUtils.parseRegionsFilterList(targetSwapRegion);
        if (includedDcs.contains(datacenterName)) {
          continue;
        }
        if (version.getNumber() > highestPartiallyOnlineVersion) {
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
    phase2Executor.shutdownNow();
    try {
      degradedDcMonitor.awaitTermination(30, TimeUnit.SECONDS);
      monitorExecutor.awaitTermination(30, TimeUnit.SECONDS);
      recoveryExecutor.awaitTermination(30, TimeUnit.SECONDS);
      phase2Executor.awaitTermination(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

}
