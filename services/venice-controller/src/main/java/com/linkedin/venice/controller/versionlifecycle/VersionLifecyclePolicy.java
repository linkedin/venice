package com.linkedin.venice.controller.versionlifecycle;

import static com.linkedin.venice.meta.Store.NON_EXISTING_VERSION;
import static com.linkedin.venice.meta.VersionStatus.ONLINE;
import static com.linkedin.venice.meta.VersionStatus.PARTIALLY_ONLINE;
import static com.linkedin.venice.meta.VersionStatus.ROLLED_BACK;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.meta.BackupStrategy;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushmonitor.OfflinePushStatus;
import com.linkedin.venice.pushmonitor.PushMonitor;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Pure-decision helpers governing the store-version lifecycle: capacity guards run before
 * starting a new push, version selection over a store's version history, status aggregation
 * across child regions, and the small bookkeeping flags ({@code ttlRepushEnabled}) self-managed
 * via the push id. All methods are deterministic given their inputs and perform no I/O against
 * the controller's own state (PushMonitor and ReadWriteStoreRepository are passed in
 * explicitly).
 */
public final class VersionLifecyclePolicy {
  private static final Logger LOGGER = LogManager.getLogger(VersionLifecyclePolicy.class);

  /**
   * Aggregation order used to pick the "worst" status when collapsing per-region push statuses
   * into one aggregate. The first match in this list wins, so transient states (PROGRESS,
   * STARTED) take precedence over terminal ones (COMPLETED, ARCHIVED) — a push is only
   * considered finished once every region agrees.
   */
  static final List<ExecutionStatus> STATUS_PRIORITIES = Arrays.asList(
      ExecutionStatus.PROGRESS,
      ExecutionStatus.STARTED,
      ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED,
      ExecutionStatus.UNKNOWN,
      ExecutionStatus.NEW,
      ExecutionStatus.NOT_CREATED,
      ExecutionStatus.END_OF_PUSH_RECEIVED,
      ExecutionStatus.DVC_INGESTION_ERROR_OTHER,
      ExecutionStatus.DVC_INGESTION_ERROR_DISK_FULL,
      ExecutionStatus.DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED,
      ExecutionStatus.DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES,
      ExecutionStatus.ERROR,
      ExecutionStatus.WARNING,
      ExecutionStatus.COMPLETED,
      ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED,
      ExecutionStatus.ARCHIVED);

  private VersionLifecyclePolicy() {
  }

  // ---------- New-push capacity guards ----------

  /**
   * Throws if starting a new push would exceed the store's version budget because existing backup
   * versions are pending deletion but still within the min cleanup delay (e.g., after a rollback).
   * Preserve count is {@code N-1} for {@code DELETE_ON_NEW_PUSH_START}, {@code N} otherwise.
   * Clamps to 1 because {@link Store#retrieveVersionsToDelete(int)} rejects values below 1.
   */
  public static void checkBackupVersionCleanupCapacityForNewPush(
      String clusterName,
      String storeName,
      Store store,
      BackupStrategy backupStrategy,
      int minNumberOfStoreVersionsToPreserve,
      long minBackupVersionCleanupDelay,
      long currentTimeMs) {
    int numVersionToPreserve = Math.max(
        1,
        backupStrategy == BackupStrategy.DELETE_ON_NEW_PUSH_START
            ? minNumberOfStoreVersionsToPreserve - 1
            : minNumberOfStoreVersionsToPreserve);
    List<Version> versionsToDelete = store.retrieveVersionsToDelete(numVersionToPreserve);
    if (versionsToDelete.isEmpty()) {
      return;
    }
    long minRetentionThreshold = store.getLatestVersionPromoteToCurrentTimestamp() + minBackupVersionCleanupDelay;
    if (currentTimeMs <= minRetentionThreshold) {
      throw new VeniceException(
          String.format(
              "Cannot start new push for store %s in cluster %s: %d backup version(s) pending deletion "
                  + "but still within min cleanup delay (%dms) of latest version promotion. "
                  + "Retry after min delay has elapsed.",
              storeName,
              clusterName,
              versionsToDelete.size(),
              minBackupVersionCleanupDelay));
    }
  }

  /**
   * Throws if starting a new push would violate the retention window for a rollback-origin version.
   * Blocks when a {@code ROLLED_BACK} or {@code PARTIALLY_ONLINE} version sits strictly above the
   * current version — the rollback-origin invariant, since rollback decrements currentVersion below
   * the rolled-back-from version's number on both parent and child controllers. Stale entries
   * lingering below currentVersion (e.g., after a subsequent push promoted higher) are skipped.
   * The block also lifts once {@code latestVersionPromoteToCurrentTimestamp + rolledBackVersionRetentionMs}
   * elapses; past that point, the version will be swept on the next SOP deletion pass.
   *
   * <p>This evaluates a single store snapshot (one region). The parent controller drives the block
   * from LIVE child-region snapshots rather than its own metadata (see
   * {@code VeniceParentHelixAdmin.checkRollbackOriginVersionCapacityFromChildren}), because parent
   * version status can go stale and falsely block pushes for the whole retention window. The
   * enforcement runs only on the parent; running it during child admin-message consumption would
   * fail the message and wedge the admin channel.
   */
  public static void checkRollbackOriginVersionCapacityForNewPush(
      String clusterName,
      String storeName,
      Store store,
      long rolledBackVersionRetentionMs,
      long currentTimeMs) {
    checkRollbackOriginVersionCapacityForNewPush(
        clusterName,
        storeName,
        null,
        store.getVersions(),
        store.getCurrentVersion(),
        store.getLatestVersionPromoteToCurrentTimestamp(),
        rolledBackVersionRetentionMs,
        currentTimeMs);
  }

  /**
   * Field-level overload of {@link #checkRollbackOriginVersionCapacityForNewPush(String, String, Store, long, long)}
   * so callers can evaluate a child {@code StoreInfo} snapshot (which is not a {@link Store}) without
   * a conversion. {@code regionName} is used only to enrich the rejection message; pass {@code null}
   * for a region-agnostic (parent-metadata) evaluation.
   */
  public static void checkRollbackOriginVersionCapacityForNewPush(
      String clusterName,
      String storeName,
      String regionName,
      List<Version> versions,
      int currentVersion,
      long latestVersionPromoteToCurrentTimestamp,
      long rolledBackVersionRetentionMs,
      long currentTimeMs) {
    long retentionExpiresAt = latestVersionPromoteToCurrentTimestamp + rolledBackVersionRetentionMs;
    if (currentTimeMs > retentionExpiresAt) {
      // Past retention — SOP deletion will clean up any rollback-origin versions.
      return;
    }
    for (Version v: versions) {
      VersionStatus status = v.getStatus();
      // A rollback-origin version is one that was promoted then rolled back to a lower version,
      // so number > currentVersion holds (rollback decrements currentVersion on both parent and
      // child controllers). Once a subsequent push promotes higher than the rolled-back version,
      // the retention contract is satisfied/superseded and the entry — which can linger in a store
      // snapshot that retains more versions — is correctly aged out by this filter.
      // Push-origin PARTIALLY_ONLINE (number == currentVersion) is correctly excluded.
      boolean isRollbackOrigin =
          (status == ROLLED_BACK || status == PARTIALLY_ONLINE) && v.getNumber() > currentVersion;
      if (isRollbackOrigin) {
        throw new VeniceException(
            String.format(
                "Cannot start new push for store %s in cluster %s: version %d is %s from a rollback%s; "
                    + "retention expires in %dms. Retry after the rolled-back version is cleaned up.",
                storeName,
                clusterName,
                v.getNumber(),
                status,
                regionName == null ? "" : " in region " + regionName,
                retentionExpiresAt - currentTimeMs));
      }
    }
  }

  /**
   * Cheap boolean pre-check mirroring {@link #checkRollbackOriginVersionCapacityForNewPush} without
   * throwing: returns {@code true} if the given store snapshot has a rollback-origin version
   * ({@code ROLLED_BACK}/{@code PARTIALLY_ONLINE} above {@code currentVersion}) still within its
   * retention window. The parent uses this against its own metadata to decide whether it is even
   * worth paying for live child verification before enforcing the block.
   */
  public static boolean hasRollbackOriginVersionWithinRetention(
      List<Version> versions,
      int currentVersion,
      long latestVersionPromoteToCurrentTimestamp,
      long rolledBackVersionRetentionMs,
      long currentTimeMs) {
    if (currentTimeMs > latestVersionPromoteToCurrentTimestamp + rolledBackVersionRetentionMs) {
      return false;
    }
    for (Version v: versions) {
      VersionStatus status = v.getStatus();
      if ((status == ROLLED_BACK || status == PARTIALLY_ONLINE) && v.getNumber() > currentVersion) {
        return true;
      }
    }
    return false;
  }

  // ---------- Version selection ----------

  /**
   * Largest {@code ONLINE} version number strictly less than {@code currentVersion}, or
   * {@link Store#NON_EXISTING_VERSION} if none. Mutates {@code versions} (sort by number desc).
   */
  public static int getBackupVersionNumber(List<Version> versions, int currentVersion) {
    versions.sort(Comparator.comparingInt(Version::getNumber).reversed());
    for (Version v: versions) {
      if (v.getNumber() < currentVersion && ONLINE.equals(v.getStatus())) {
        return v.getNumber();
      }
    }
    return NON_EXISTING_VERSION;
  }

  // ---------- Push status aggregation ----------

  /**
   * Aggregate per-region statuses into one return status using {@link #STATUS_PRIORITIES}. If
   * fewer than a strict majority of {@code childRegions} reported successfully, downgrades to
   * {@code PROGRESS} so the caller keeps polling. If the aggregate is terminal but any region
   * failed to report, downgrades to {@code ERROR} and appends a "{n}/{total} DCs unreachable"
   * note to {@code currentReturnStatusDetails} so VPJ reports failure even when the push
   * succeeds asynchronously in the reachable DCs.
   */
  public static ExecutionStatus getFinalReturnStatus(
      Map<String, ExecutionStatus> statuses,
      Set<String> childRegions,
      int numChildRegionsFailedToFetchStatus,
      StringBuilder currentReturnStatusDetails) {
    ExecutionStatus currentReturnStatus = ExecutionStatus.NEW;

    // Sort the per-datacenter status in this order, and return the first one in the list.
    // Edge case example: if one cluster is stuck in NOT_CREATED, then as another cluster goes
    // from PROGRESS to COMPLETED the aggregate status will go from PROGRESS back down to
    // NOT_CREATED.
    List<ExecutionStatus> sortedStatuses = statuses.values()
        .stream()
        .sorted(Comparator.comparingInt(STATUS_PRIORITIES::indexOf))
        .collect(Collectors.toList());

    if (!sortedStatuses.isEmpty()) {
      currentReturnStatus = sortedStatuses.get(0);
    }

    int successCount = childRegions.size() - numChildRegionsFailedToFetchStatus;
    if (successCount < (childRegions.size() / 2) + 1) {
      // Strict majority must be reachable, otherwise keep polling.
      currentReturnStatus = ExecutionStatus.PROGRESS;
    }

    if (currentReturnStatus.isTerminal()) {
      // If there is a temporary datacenter connection failure, we want VPJ to report failure
      // while allowing the push to succeed in remaining datacenters. If we want to allow the
      // push to succeed in async in the remaining datacenter, then put the topic delete into an
      // else block under `if (numChildRegionsFailedToFetchStatus > 0)`.
      if (numChildRegionsFailedToFetchStatus > 0) {
        currentReturnStatus = ExecutionStatus.ERROR;
        currentReturnStatusDetails.append(numChildRegionsFailedToFetchStatus)
            .append("/")
            .append(childRegions.size())
            .append(" DCs unreachable. ");
      }
    }

    return currentReturnStatus;
  }

  /**
   * Merge the Venice-server status and the Da Vinci status into a single overall status using
   * {@link #STATUS_PRIORITIES}. Used to roll up the two replica families into the push status
   * a client polls.
   */
  public static ExecutionStatus getOverallPushStatus(ExecutionStatus veniceStatus, ExecutionStatus daVinciStatus) {
    List<ExecutionStatus> statuses = Arrays.asList(veniceStatus, daVinciStatus);
    statuses.sort(Comparator.comparingInt(STATUS_PRIORITIES::indexOf));
    return statuses.get(0);
  }

  // ---------- Misc lifecycle predicates ----------

  /**
   * Read the offline push status from {@code pushMonitor} and return whether the push reported a
   * fatal Data Integrity Validation error. Returns {@code false} when the push entry no longer
   * exists (logged at WARN).
   */
  public static boolean hasFatalDataValidationError(PushMonitor pushMonitor, String topicName) {
    try {
      OfflinePushStatus offlinePushStatus = pushMonitor.getOfflinePushOrThrow(topicName);
      return offlinePushStatus.hasFatalDataValidationError();
    } catch (VeniceException e) {
      LOGGER.warn("Failed to get offline push status for topic: {}. It might not exist anymore.", topicName);
      return false;
    }
  }

  /**
   * Self-manages the store's {@code ttlRepushEnabled} property based on push job id prefix.
   * <ul>
   *   <li>TTL repush ({@code venice_ttl_re_push_*}) sets the flag to {@code true}.</li>
   *   <li>Regular push with TTL repush ({@code venice_regular_push_with_ttl_re_push_*}) sets the
   *       flag to {@code false}.</li>
   *   <li>Other push types (including compliance push) do not affect this flag.</li>
   * </ul>
   * Persists via {@link ReadWriteStoreRepository#updateStore(Store)} only when the flag is
   * actually changing.
   */
  public static void updateStoreTTLRepushFlag(String pushJobId, Store store, ReadWriteStoreRepository repository) {
    if (Version.isPushIdTTLRePush(pushJobId) && !store.isTTLRepushEnabled()) {
      store.setTTLRepushEnabled(true);
      repository.updateStore(store);
    } else if (Version.isPushIdRegularPushWithTTLRePush(pushJobId) && store.isTTLRepushEnabled()) {
      store.setTTLRepushEnabled(false);
      repository.updateStore(store);
    }
  }

  // ---------- Topic-requirement predicates ----------

  /**
   * Whether a real-time topic should exist for {@code version} of {@code store}. True iff both the
   * store and the version are hybrid and this controller is not a parent — only child regions host
   * RT topics; parent regions route writes through them via the child fabrics.
   */
  public static boolean isRealTimeTopicRequired(Store store, Version version, boolean isParent) {
    if (!store.isHybrid() || !version.isHybrid()) {
      return false;
    }
    return !isParent;
  }

  // ---------- Version-deletion preconditions ----------

  /**
   * Validate that {@code versionNum} can be individually deleted from {@code store}. Throws
   * {@link VeniceNoStoreException} if the store is missing, or
   * {@link VeniceUnsupportedOperationException} if {@code versionNum} is the current version of a
   * non-system store (system stores are exempt because their current-version delete is part of the
   * tear-down path).
   */
  public static void checkPreConditionForSingleVersionDeletion(
      String clusterName,
      String storeName,
      Store store,
      int versionNum) {
    if (store == null) {
      String errorMessage = "Store:" + storeName + " does not exist in cluster:" + clusterName;
      LOGGER.error(errorMessage);
      throw new VeniceNoStoreException(storeName, clusterName);
    }
    // Current version of regular stores serve read traffic and should not be deleted.
    if (!store.isSystemStore() && store.getCurrentVersion() == versionNum) {
      String errorMsg = "Unable to delete the version: " + versionNum
          + ". The current version could not be deleted from store: " + storeName;
      LOGGER.error(errorMsg);
      throw new VeniceUnsupportedOperationException("delete single version", errorMsg);
    }
  }
}
