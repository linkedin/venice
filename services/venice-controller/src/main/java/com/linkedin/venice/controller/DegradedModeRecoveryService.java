package com.linkedin.venice.controller;

import com.linkedin.venice.controller.stats.DegradedModeStats;
import com.linkedin.venice.meta.DegradedDcInfo;
import com.linkedin.venice.meta.DegradedDcStates;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Orchestrates bulk data recovery for stores with PARTIALLY_ONLINE versions when a degraded DC is unmarked.
 *
 * Uses the existing data recovery flow (prepare → poll readiness → initiate) via the Admin interface,
 * running recoveries in parallel with bounded concurrency. After initiation, monitors child DC
 * completion and transitions parent version status from PARTIALLY_ONLINE → ONLINE.
 */
public class DegradedModeRecoveryService implements Closeable {
  private static final Logger LOGGER = LogManager.getLogger(DegradedModeRecoveryService.class);

  static final int MAX_RETRIES = 3;
  static final long READINESS_POLL_INTERVAL_MS = 5000;
  static final int READINESS_POLL_MAX_ATTEMPTS = 60; // 5 min max
  static final long DEFAULT_RECOVERY_COMPLETION_POLL_INTERVAL_MS = 30_000; // 30 seconds
  static final int DEFAULT_RECOVERY_COMPLETION_POLL_MAX_ATTEMPTS = 720; // 6 hours max
  static final int DEFAULT_RECOVERY_THREAD_POOL_SIZE = 5;
  private static final long DC_MONITOR_INTERVAL_SECONDS = 60;

  private final Admin admin;
  private final DegradedModeStats stats;
  private final Map<String, RecoveryProgress> activeRecoveries = new VeniceConcurrentHashMap<>();
  private final ExecutorService recoveryExecutor;
  private final ExecutorService monitorExecutor;
  private final ScheduledExecutorService degradedDcMonitor;
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
  }

  /**
   * Start the periodic monitor that emits duration metrics for degraded DCs.
   * Must be called after the Admin is fully initialized.
   *
   * @param clusterNames the set of cluster names to monitor
   */
  public void startDegradedDcMonitor(Set<String> clusterNames) {
    startDegradedDcMonitor(clusterNames, DC_MONITOR_INTERVAL_SECONDS, TimeUnit.SECONDS);
  }

  // Visible for testing
  void startDegradedDcMonitor(Set<String> clusterNames, long interval, TimeUnit unit) {
    degradedDcMonitor.scheduleAtFixedRate(() -> emitDegradedDcDurationMetrics(clusterNames), interval, interval, unit);
    LOGGER.info("Started degraded DC duration monitor for clusters: {}", clusterNames);
  }

  private void emitDegradedDcDurationMetrics(Set<String> clusterNames) {
    for (String clusterName: clusterNames) {
      try {
        DegradedDcStates degradedDcStates = admin.getDegradedDcStates(clusterName);
        if (!degradedDcStates.isEmpty()) {
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
                  "ALERT: Datacenter {} in cluster {} has been degraded for {} minutes, "
                      + "exceeding the configured timeout of {} minutes. Operator: {}",
                  dcName,
                  clusterName,
                  (long) durationMinutes,
                  info.getTimeoutMinutes(),
                  info.getOperatorId());
            }
          }
        }

        // Detect orphaned PARTIALLY_ONLINE versions that have no active recovery.
        // This handles leader failover: the new leader detects stranded versions and
        // re-triggers recovery. The recovery flow (prepare -> readiness -> initiate) is
        // idempotent, so re-triggering is safe even if the old leader partially completed.
        detectAndRecoverOrphanedVersions(clusterName);
      } catch (Exception e) {
        LOGGER.warn("Error in degraded DC monitor for cluster: {}", clusterName, e);
      }
    }
  }

  /**
   * Scans for stores with PARTIALLY_ONLINE versions that have no active recovery in progress.
   * This covers the case where a controller leader fails over mid-recovery — the new leader
   * will detect orphaned versions and re-trigger recovery.
   *
   * <p>The full recovery sequence (prepare -> readiness check -> initiate) is idempotent:
   * prepare cleans up any partial state, readiness waits for cleanup, and initiate uses a
   * new push job ID. So re-triggering is always safe.
   */
  private void detectAndRecoverOrphanedVersions(String clusterName) {
    List<StoreVersionPair> orphaned = findPartiallyOnlineStores(clusterName);
    if (orphaned.isEmpty()) {
      return;
    }

    // Determine which DCs might need recovery by checking which regions are NOT serving
    // these versions. We use the child DC controller URL map as the set of all regions.
    Map<String, String> allRegions = admin.getChildDataCenterControllerUrlMap(clusterName);
    for (String regionName: allRegions.keySet()) {
      // Skip if there's already an active recovery for this region
      RecoveryProgress existing = activeRecoveries.get(clusterName + "/" + regionName);
      if (existing != null && !existing.isComplete()) {
        continue;
      }

      // Check if any PARTIALLY_ONLINE store's version is NOT current in this region
      boolean regionNeedsRecovery = false;
      for (StoreVersionPair sv: orphaned) {
        try {
          if (!admin.isVersionCurrentInRegion(clusterName, sv.storeName, sv.version, regionName)) {
            regionNeedsRecovery = true;
            break;
          }
        } catch (Exception e) {
          // If we can't check, assume this region might need recovery
          regionNeedsRecovery = true;
          break;
        }
      }

      if (regionNeedsRecovery) {
        LOGGER.warn(
            "Detected orphaned PARTIALLY_ONLINE versions in cluster {} that may need recovery in region {}. "
                + "Triggering recovery (idempotent). {} stores affected.",
            clusterName,
            regionName,
            orphaned.size());
        triggerRecovery(clusterName, regionName);
      }
    }
  }

  /**
   * Trigger recovery for all stores with PARTIALLY_ONLINE versions in the given datacenter.
   * Returns immediately — recovery runs asynchronously.
   */
  public void triggerRecovery(String clusterName, String datacenterName) {
    // Atomically check-and-replace using compute() to avoid TOCTOU race.
    // Allows replacement of completed entries so the map doesn't grow unbounded.
    // Key by cluster+dc to avoid collisions in multi-cluster deployments.
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

    List<StoreVersionPair> affected = findPartiallyOnlineStores(clusterName);
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
    for (StoreVersionPair sv: affected) {
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

  /**
   * After all recovery initiations are complete, poll the child DC to confirm that each
   * recovered store version is now serving traffic. Once confirmed, transition the parent
   * version status from PARTIALLY_ONLINE → ONLINE.
   */
  void confirmRecoveryAndTransitionVersions(String clusterName, String datacenterName, RecoveryProgress progress) {
    // Snapshot the list to avoid ConcurrentModificationException — Phase 1 executor threads
    // have all completed by this point (monitor thread waited on all futures), but we snapshot
    // defensively since the list is shared mutable state.
    List<StoreVersionPair> initiatedStores = new ArrayList<>(progress.getInitiatedStores());
    if (initiatedStores.isEmpty()) {
      return;
    }

    LOGGER.info(
        "Starting recovery completion monitoring for {} stores in datacenter: {}",
        initiatedStores.size(),
        datacenterName);

    // Poll all stores in parallel using the recovery executor to avoid O(stores * timeout) bottleneck
    List<Future<?>> confirmFutures = new ArrayList<>();
    for (StoreVersionPair sv: initiatedStores) {
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
              // A newer version is current — recovery is moot. Count as success since the DC is healthy.
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
    // Clear initiated stores to free memory after confirmation phase
    progress.getInitiatedStores().clear();
  }

  /**
   * Polls until the recovered version becomes current in the child DC, OR a newer version
   * supersedes it (in which case recovery is moot — the newer push already healed the DC).
   *
   * @return {@link VersionPollResult#CURRENT} if the version became current,
   *         {@link VersionPollResult#SUPERSEDED} if a newer version is now current,
   *         {@link VersionPollResult#TIMED_OUT} if polling exhausted max attempts
   */
  VersionPollResult pollUntilVersionCurrent(String clusterName, StoreVersionPair storeVersion, String datacenterName)
      throws InterruptedException {
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

  List<StoreVersionPair> findPartiallyOnlineStores(String clusterName) {
    List<StoreVersionPair> result = new ArrayList<>();
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
        result.add(new StoreVersionPair(store.getName(), highestPartiallyOnlineVersion));
      }
    }
    return result;
  }

  void recoverSingleStore(
      String clusterName,
      String datacenterName,
      StoreVersionPair storeVersion,
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
      StoreVersionPair storeVersion) throws InterruptedException {
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

  /**
   * Tracks progress of a bulk recovery operation for a datacenter.
   */
  public static class RecoveryProgress {
    private final String datacenterName;
    private final AtomicInteger totalStores = new AtomicInteger(0);
    private final AtomicInteger recoveredStores = new AtomicInteger(0);
    private final AtomicInteger failedStores = new AtomicInteger(0);
    private final AtomicInteger versionsTransitioned = new AtomicInteger(0);
    private final List<StoreVersionPair> initiatedStores = Collections.synchronizedList(new ArrayList<>());
    private volatile boolean complete = false;

    public RecoveryProgress(String datacenterName) {
      this.datacenterName = datacenterName;
    }

    public String getDatacenterName() {
      return datacenterName;
    }

    public int getTotalStores() {
      return totalStores.get();
    }

    public void setTotalStores(int total) {
      totalStores.set(total);
    }

    public int getRecoveredStores() {
      return recoveredStores.get();
    }

    public void incrementRecovered() {
      recoveredStores.incrementAndGet();
    }

    public int getFailedStores() {
      return failedStores.get();
    }

    public void incrementFailed() {
      failedStores.incrementAndGet();
    }

    public int getVersionsTransitioned() {
      return versionsTransitioned.get();
    }

    public void incrementVersionsTransitioned() {
      versionsTransitioned.incrementAndGet();
    }

    public void addInitiatedStore(StoreVersionPair sv) {
      initiatedStores.add(sv);
    }

    public List<StoreVersionPair> getInitiatedStores() {
      return initiatedStores;
    }

    public boolean isComplete() {
      return complete;
    }

    public void markComplete() {
      complete = true;
    }

    public double getProgressFraction() {
      int total = totalStores.get();
      if (total == 0) {
        return complete ? 1.0 : 0.0;
      }
      return (double) (recoveredStores.get() + failedStores.get()) / total;
    }
  }

  static class StoreVersionPair {
    final String storeName;
    final int version;

    StoreVersionPair(String storeName, int version) {
      this.storeName = storeName;
      this.version = version;
    }
  }
}
