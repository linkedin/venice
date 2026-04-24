package com.linkedin.venice.controller;

import com.linkedin.venice.controller.stats.DegradedModeStats;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.utils.Pair;
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

  static final int MAX_RETRIES = 3;
  static final long READINESS_POLL_INTERVAL_MS = 5000;
  static final int READINESS_POLL_MAX_ATTEMPTS = 60; // 5 min max
  static final long DEFAULT_RECOVERY_COMPLETION_POLL_INTERVAL_MS = 30_000; // 30 seconds
  static final int DEFAULT_RECOVERY_COMPLETION_POLL_MAX_ATTEMPTS = 720; // 6 hours max
  static final int DEFAULT_RECOVERY_THREAD_POOL_SIZE = 5;

  private final Admin admin;
  private final DegradedModeStats stats;
  private final Map<String, RecoveryProgress> activeRecoveries = new VeniceConcurrentHashMap<>();
  private final ExecutorService recoveryExecutor;
  private final ExecutorService monitorExecutor;
  private final ScheduledExecutorService degradedDcMonitor;
  private final DegradedDcMonitor dcMonitor;
  private long recoveryCompletionPollIntervalMs = DEFAULT_RECOVERY_COMPLETION_POLL_INTERVAL_MS;
  private int recoveryCompletionPollMaxAttempts = DEFAULT_RECOVERY_COMPLETION_POLL_MAX_ATTEMPTS;
  private long retryBackoffBaseMs = READINESS_POLL_INTERVAL_MS;

  public DegradedModeRecoveryService(Admin admin, DegradedModeStats stats) {
    this(admin, stats, DEFAULT_RECOVERY_THREAD_POOL_SIZE);
  }

  public DegradedModeRecoveryService(Admin admin, DegradedModeStats stats, int threadPoolSize) {
    this.admin = admin;
    this.stats = stats;
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
    this.dcMonitor = new DegradedDcMonitor(admin, stats, this, this.degradedDcMonitor);
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
        "Triggering recovery for {} stores in datacenter: {} for cluster: {}",
        affected.size(),
        datacenterName,
        clusterName);

    List<Future<?>> futures = new ArrayList<>();
    for (RecoveryProgress.StoreVersionPair sv: affected) {
      futures.add(recoveryExecutor.submit(() -> recoverSingleStore(clusterName, datacenterName, sv, progress)));
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
            "All recovery initiations complete for datacenter: {}. Recovered: {}, Failed: {}, Total: {}",
            datacenterName,
            progress.getRecoveredStores(),
            progress.getFailedStores(),
            progress.getTotalStores());

        // Phase 2: Wait for child DC to confirm recovery completion, then transition versions
        confirmRecoveryAndTransitionVersions(clusterName, datacenterName, progress);
      } finally {
        progress.markComplete();
        if (stats != null) {
          stats.recordRecoveryProgress(progress.getProgressFraction());
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
          VersionPollResult result = pollUntilVersionCurrent(clusterName, sv, datacenterName);
          switch (result) {
            case CURRENT:
              admin.updateStoreVersionStatus(clusterName, sv.storeName, sv.version, VersionStatus.ONLINE);
              progress.incrementVersionsTransitioned();
              if (stats != null) {
                stats.recordRecoveryVersionTransitioned(clusterName, sv.storeName);
                stats.recordRecoveryProgress(progress.getProgressFraction());
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

  /** Polls until recovered version is current, superseded by a newer version, or timed out. */
  VersionPollResult pollUntilVersionCurrent(
      String clusterName,
      RecoveryProgress.StoreVersionPair storeVersion,
      String datacenterName) throws InterruptedException {
    for (int i = 0; i < recoveryCompletionPollMaxAttempts; i++) {
      int currentVersionInRegion = admin.getCurrentVersionInRegion(clusterName, storeVersion.storeName, datacenterName);
      if (currentVersionInRegion == storeVersion.version) {
        return VersionPollResult.CURRENT;
      }
      if (currentVersionInRegion > storeVersion.version) {
        LOGGER.info(
            "Store {} v{} in datacenter {} superseded by newer version v{}. Recovery is moot.",
            storeVersion.storeName,
            storeVersion.version,
            datacenterName,
            currentVersionInRegion);
        return VersionPollResult.SUPERSEDED;
      }
      if (i % 20 == 0 && i > 0) {
        LOGGER.debug(
            "Waiting for store {} v{} to become current in datacenter: {} (poll {}/{})",
            storeVersion.storeName,
            storeVersion.version,
            datacenterName,
            i,
            recoveryCompletionPollMaxAttempts);
      }
      Thread.sleep(recoveryCompletionPollIntervalMs);
    }
    return VersionPollResult.TIMED_OUT;
  }

  enum VersionPollResult {
    CURRENT, SUPERSEDED, TIMED_OUT
  }

  // Visible for testing
  void setRecoveryCompletionPollParameters(long intervalMs, int maxAttempts) {
    this.recoveryCompletionPollIntervalMs = intervalMs;
    this.recoveryCompletionPollMaxAttempts = maxAttempts;
    this.retryBackoffBaseMs = intervalMs;
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

  void recoverSingleStore(
      String clusterName,
      String datacenterName,
      RecoveryProgress.StoreVersionPair storeVersion,
      RecoveryProgress progress) {
    // M3: Pre-check that the version still exists and is still PARTIALLY_ONLINE.
    // Between findPartiallyOnlineStores() and now, the version could have been deleted,
    // transitioned by DeferredVersionSwapService, or superseded by a newer push.
    Store currentStore = admin.getStore(clusterName, storeVersion.storeName);
    if (currentStore == null) {
      LOGGER.warn("Store {} no longer exists. Skipping recovery.", storeVersion.storeName);
      progress.incrementFailed();
      return;
    }
    Version currentVersion = currentStore.getVersion(storeVersion.version);
    if (currentVersion == null || currentVersion.getStatus() != VersionStatus.PARTIALLY_ONLINE) {
      LOGGER.info(
          "Store {} v{} is no longer PARTIALLY_ONLINE (current status: {}). Skipping recovery.",
          storeVersion.storeName,
          storeVersion.version,
          currentVersion == null ? "deleted" : currentVersion.getStatus());
      return;
    }

    long recoveryStartMs = System.currentTimeMillis();
    for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
      try {
        String sourceFabric = resolveSourceFabric(clusterName, storeVersion.storeName);
        LOGGER.debug(
            "Recovering store {} v{} in datacenter {} from source fabric {} (attempt {}/{})",
            storeVersion.storeName,
            storeVersion.version,
            datacenterName,
            sourceFabric,
            attempt + 1,
            MAX_RETRIES);

        admin.prepareDataRecovery(
            clusterName,
            storeVersion.storeName,
            storeVersion.version,
            sourceFabric,
            datacenterName,
            Optional.empty());

        pollUntilReady(clusterName, sourceFabric, datacenterName, storeVersion);

        admin.initiateDataRecovery(
            clusterName,
            storeVersion.storeName,
            storeVersion.version,
            sourceFabric,
            datacenterName,
            false,
            Optional.empty());

        progress.incrementRecovered();
        progress.addInitiatedStore(storeVersion);
        if (stats != null) {
          stats.recordRecoveryStoreSuccess(clusterName, storeVersion.storeName);
          stats.recordRecoveryStoreDurationMs(
              clusterName,
              storeVersion.storeName,
              System.currentTimeMillis() - recoveryStartMs);
        }
        LOGGER.info(
            "Successfully initiated recovery for store {} v{} in datacenter {}",
            storeVersion.storeName,
            storeVersion.version,
            datacenterName);
        return;
      } catch (Exception e) {
        LOGGER.warn(
            "Attempt {}/{} failed for store {} v{} in datacenter {}: {}",
            attempt + 1,
            MAX_RETRIES,
            storeVersion.storeName,
            storeVersion.version,
            datacenterName,
            e.getMessage());
        if (attempt == MAX_RETRIES - 1) {
          LOGGER.error(
              "All {} retries exhausted for store {} v{} in datacenter {}",
              MAX_RETRIES,
              storeVersion.storeName,
              storeVersion.version,
              datacenterName,
              e);
        } else {
          // Exponential backoff between retries
          try {
            Thread.sleep(retryBackoffBaseMs * (attempt + 1));
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            break;
          }
        }
      }
    }
    progress.incrementFailed();
    if (stats != null) {
      stats.recordRecoveryStoreFailure(clusterName, storeVersion.storeName);
    }
  }

  private void pollUntilReady(
      String clusterName,
      String sourceFabric,
      String datacenterName,
      RecoveryProgress.StoreVersionPair storeVersion) throws InterruptedException {
    for (int i = 0; i < READINESS_POLL_MAX_ATTEMPTS; i++) {
      Pair<Boolean, String> readiness = admin.isStoreVersionReadyForDataRecovery(
          clusterName,
          storeVersion.storeName,
          storeVersion.version,
          sourceFabric,
          datacenterName,
          Optional.empty());
      if (readiness.getFirst()) {
        return;
      }
      LOGGER.debug(
          "Store {} v{} not ready for recovery in datacenter {} (attempt {}/{}): {}",
          storeVersion.storeName,
          storeVersion.version,
          datacenterName,
          i + 1,
          READINESS_POLL_MAX_ATTEMPTS,
          readiness.getSecond());
      Thread.sleep(READINESS_POLL_INTERVAL_MS);
    }
    throw new RuntimeException(
        "Timed out waiting for store " + storeVersion.storeName + " v" + storeVersion.version
            + " to be ready for data recovery in datacenter " + datacenterName);
  }

  String resolveSourceFabric(String clusterName, String storeName) {
    Store store = admin.getStore(clusterName, storeName);
    Optional<String> emergencySourceRegion = admin.getEmergencySourceRegion(clusterName);
    return admin.getNativeReplicationSourceFabric(clusterName, store, Optional.empty(), emergencySourceRegion, null);
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
      monitorExecutor.awaitTermination(30, TimeUnit.SECONDS);
      recoveryExecutor.awaitTermination(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

}
