package com.linkedin.venice.controller;

import com.linkedin.venice.controller.stats.DegradedModeStats;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Executes per-store data-recovery operations on behalf of {@link DegradedModeRecoveryService}.
 *
 * <p>Two phases are exposed here:
 * <ul>
 *   <li>{@link #recoverSingleStore} — prepare → poll readiness → initiate, with retry/backoff.</li>
 *   <li>{@link #pollUntilVersionCurrent} — wait for the recovered version to become current in the
 *       target datacenter, or detect a superseding push.</li>
 * </ul>
 *
 * <p>Concurrency / scheduling (thread pools, per-store futures, progress tracking) is the
 * orchestrator's concern; this class is stateless w.r.t. cross-store coordination.
 */
class StoreRecoveryExecutor {
  private static final Logger LOGGER = LogManager.getLogger(StoreRecoveryExecutor.class);
  private static final RedundantExceptionFilter REDUNDANT_LOG_FILTER =
      new RedundantExceptionFilter(RedundantExceptionFilter.DEFAULT_BITSET_SIZE, TimeUnit.MINUTES.toMillis(5));

  static final int MAX_RETRIES = 3;
  static final long READINESS_POLL_INTERVAL_MS = 5000;
  static final int READINESS_POLL_MAX_ATTEMPTS = 60; // 5 min max
  static final long DEFAULT_RECOVERY_COMPLETION_POLL_INTERVAL_MS = 30_000; // 30 seconds
  static final int DEFAULT_RECOVERY_COMPLETION_POLL_MAX_ATTEMPTS = 720; // 6 hours max
  private static final long SLOW_RECOVERY_THRESHOLD_MS = TimeUnit.MINUTES.toMillis(30);

  private final Admin admin;
  private final DegradedModeStats stats;
  private long recoveryCompletionPollIntervalMs = DEFAULT_RECOVERY_COMPLETION_POLL_INTERVAL_MS;
  private int recoveryCompletionPollMaxAttempts = DEFAULT_RECOVERY_COMPLETION_POLL_MAX_ATTEMPTS;
  private long retryBackoffBaseMs = READINESS_POLL_INTERVAL_MS;

  StoreRecoveryExecutor(Admin admin, DegradedModeStats stats) {
    this.admin = admin;
    this.stats = stats;
  }

  long getRecoveryCompletionPollIntervalMs() {
    return recoveryCompletionPollIntervalMs;
  }

  int getRecoveryCompletionPollMaxAttempts() {
    return recoveryCompletionPollMaxAttempts;
  }

  /** Visible for testing — also adjusts the retry backoff base so test runs are quick. */
  void setRecoveryCompletionPollParameters(long intervalMs, int maxAttempts) {
    this.recoveryCompletionPollIntervalMs = intervalMs;
    this.recoveryCompletionPollMaxAttempts = maxAttempts;
    this.retryBackoffBaseMs = intervalMs;
  }

  void recoverSingleStore(
      String clusterName,
      String datacenterName,
      RecoveryProgress.StoreVersionPair storeVersion,
      RecoveryProgress progress) {
    // Pre-check that the version still exists and is still PARTIALLY_ONLINE.
    // PARTIALLY_ONLINE is also set by DeferredVersionSwapService and rollbacks, not just
    // degraded-mode pushes. This check ensures we only recover versions that are genuinely
    // stuck — DeferredVersionSwapService will have already transitioned its versions to ONLINE.
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
      // Count as recovered so progressFraction reaches 1.0 — the version no longer needs recovery
      progress.incrementRecovered();
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
          // Linear backoff between retries
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

  /**
   * Single-shot per-store check + transition. Returns the disposition for one poll attempt:
   * <ul>
   *   <li>{@link VersionPollResult#CURRENT}: child DC reports this version is current — the
   *       parent's version status has been transitioned to ONLINE.</li>
   *   <li>{@link VersionPollResult#SUPERSEDED}: child DC's current version is newer than this
   *       version — recovery is moot and counts as transitioned.</li>
   *   <li>{@link VersionPollResult#PENDING}: not yet current and attempt count below the max —
   *       caller should retry on the next tick.</li>
   *   <li>{@link VersionPollResult#TIMED_OUT}: attempt count has reached the max — caller should
   *       stop polling and record failure.</li>
   * </ul>
   * Caller is responsible for sleeping between ticks and for tracking the attempt count.
   */
  VersionPollResult checkAndMaybeTransition(
      String clusterName,
      RecoveryProgress.StoreVersionPair storeVersion,
      String datacenterName,
      RecoveryProgress progress,
      int attemptCount,
      long pollStartMs) {
    int currentVersionInRegion = admin.getCurrentVersionInRegion(clusterName, storeVersion.storeName, datacenterName);
    if (currentVersionInRegion == storeVersion.version) {
      admin.updateStoreVersionStatus(clusterName, storeVersion.storeName, storeVersion.version, VersionStatus.ONLINE);
      progress.incrementVersionsTransitioned();
      if (stats != null) {
        stats.recordRecoveryVersionTransitioned(clusterName, storeVersion.storeName);
        stats.recordRecoveryProgress(clusterName, datacenterName, progress.getProgressFraction());
      }
      LOGGER.info(
          "Transitioned store {} v{} from PARTIALLY_ONLINE to ONLINE after recovery in datacenter: {}",
          storeVersion.storeName,
          storeVersion.version,
          datacenterName);
      return VersionPollResult.CURRENT;
    }
    if (currentVersionInRegion > storeVersion.version) {
      progress.incrementVersionsTransitioned();
      LOGGER.info(
          "Store {} v{} in datacenter {} superseded by newer version v{}. Recovery is moot.",
          storeVersion.storeName,
          storeVersion.version,
          datacenterName,
          currentVersionInRegion);
      return VersionPollResult.SUPERSEDED;
    }
    if (attemptCount >= recoveryCompletionPollMaxAttempts) {
      progress.incrementFailed();
      if (stats != null) {
        stats.recordRecoveryStoreFailure(clusterName, storeVersion.storeName);
        stats.recordRecoveryProgress(clusterName, datacenterName, progress.getProgressFraction());
      }
      LOGGER.warn(
          "Recovery completion timed out for store {} v{} in datacenter: {}. "
              + "Version remains PARTIALLY_ONLINE. Manual intervention may be needed.",
          storeVersion.storeName,
          storeVersion.version,
          datacenterName);
      return VersionPollResult.TIMED_OUT;
    }
    long elapsedMs = System.currentTimeMillis() - pollStartMs;
    // Progress log — REDUNDANT_LOG_FILTER suppresses identical (store,version,dc) entries within
    // the filter's time window (5 min), so this naturally fires roughly every 5 min per store.
    logIfNotRedundant(
        "Waiting for store " + storeVersion.storeName + " v" + storeVersion.version + " to become current in "
            + "datacenter: " + datacenterName);
    if (elapsedMs > SLOW_RECOVERY_THRESHOLD_MS) {
      warnIfNotRedundant(
          "SLOW RECOVERY: Store " + storeVersion.storeName + " v" + storeVersion.version + " in datacenter "
              + datacenterName + " has been polling for " + TimeUnit.MILLISECONDS.toMinutes(elapsedMs) + " min");
    }
    return VersionPollResult.PENDING;
  }

  private static void logIfNotRedundant(String message) {
    if (!REDUNDANT_LOG_FILTER.isRedundantException(message)) {
      LOGGER.info(message);
    }
  }

  private static void warnIfNotRedundant(String message) {
    if (!REDUNDANT_LOG_FILTER.isRedundantException(message)) {
      LOGGER.warn(message);
    }
  }

  enum VersionPollResult {
    CURRENT, SUPERSEDED, PENDING, TIMED_OUT
  }
}
