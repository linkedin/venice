package com.linkedin.venice.controller;

import static com.linkedin.venice.meta.Store.NON_EXISTING_VERSION;

import com.linkedin.venice.controller.versionlifecycle.VersionLifecyclePolicy;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreGraveyard;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.pushmonitor.OfflinePushStatus;
import com.linkedin.venice.pushmonitor.PushMonitor;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import com.linkedin.venice.utils.locks.ClusterLockManager;
import java.util.List;
import java.util.Optional;


/**
 * Owns the per-cluster store version metadata behavior that previously lived inline in {@link VeniceHelixAdmin}.
 *
 * <p>Unlike {@link ParentVersionOrchestrator} (a thin delegating shim that forwards back to its host), this is a real
 * component extraction: it holds no back-reference to {@link VeniceHelixAdmin}. Its collaborators -- the cluster lock
 * manager, the store metadata repository, the push monitor, and the store graveyard -- are injected explicitly at
 * construction time. One instance is created per cluster by {@link HelixVeniceClusterResources}, which owns the
 * concrete per-cluster {@link ClusterLockManager} and {@link ReadWriteStoreRepository}; the controller-wide
 * {@link StoreGraveyard} is passed down from {@link VeniceHelixAdmin}.
 *
 * <p>Leadership ({@code checkControllerLeadershipFor}) is asserted by the {@link VeniceHelixAdmin} forwarders before
 * the per-cluster resources (and therefore this manager) are resolved, so this class assumes it is only reached when
 * the controller leads the cluster.
 */
class StoreVersionManager {
  // TODO: this is PR 1 (reads) of a staged extraction. Remaining moves -- lifecycle setters,
  // rollForward/rollback, and deletion-metadata -- are tracked in the plan at the repo root:
  // StoreVersionManager-extraction-plan.md
  private final ClusterLockManager clusterLockManager;
  private final ReadWriteStoreRepository storeRepository;
  private final PushMonitor pushMonitor;
  private final StoreGraveyard storeGraveyard;

  StoreVersionManager(
      ClusterLockManager clusterLockManager,
      ReadWriteStoreRepository storeRepository,
      PushMonitor pushMonitor,
      StoreGraveyard storeGraveyard) {
    this.clusterLockManager = clusterLockManager;
    this.storeRepository = storeRepository;
    this.pushMonitor = pushMonitor;
    this.storeGraveyard = storeGraveyard;
  }

  /**
   * @return the current version number of the store, or {@link Store#NON_EXISTING_VERSION} if reads are disabled.
   */
  int getCurrentVersion(String storeName) {
    Store store = getStoreForReadOnly(storeName);
    if (store.isEnableReads()) {
      return store.getCurrentVersion();
    }
    return NON_EXISTING_VERSION;
  }

  /**
   * @return the online (completed but not yet swapped) or in-flight future version, else
   * {@link Store#NON_EXISTING_VERSION}.
   */
  int getFutureVersion(String storeName) {
    // Find all ongoing offline pushes at first.
    List<OfflinePushStatus> offlinePushStatuses = pushMonitor.getOfflinePushStatusForStore(storeName);
    if (offlinePushStatuses.isEmpty()) {
      return NON_EXISTING_VERSION;
    }
    Optional<String> offlinePush = offlinePushStatuses.stream()
        .filter(status -> !status.getCurrentStatus().isTerminal())
        .map(OfflinePushStatus::getKafkaTopic)
        .findFirst();
    if (offlinePush.isPresent()) {
      return Version.parseVersionFromKafkaTopicName(offlinePush.get());
    }
    // If the push status is finished, then return the largest online version greater than current version.
    int onlineFutureVersion = getFutureVersionWithStatus(storeName, VersionStatus.ONLINE);
    int pushedFutureVersion = getFutureVersionWithStatus(storeName, VersionStatus.PUSHED);
    return pushedFutureVersion != NON_EXISTING_VERSION ? pushedFutureVersion : onlineFutureVersion;
  }

  int getBackupVersion(String storeName) {
    try (AutoCloseableLock ignore = clusterLockManager.createStoreReadLock(storeName)) {
      Store store = storeRepository.getStore(storeName);
      return VersionLifecyclePolicy.getBackupVersionNumber(store.getVersions(), store.getCurrentVersion());
    }
  }

  int getFutureVersionWithStatus(String storeName, VersionStatus status) {
    Store store = getStoreForReadOnly(storeName);
    if (store.getVersions().isEmpty()) {
      return NON_EXISTING_VERSION;
    }

    Version version = store.getVersions().stream().max(Comparable::compareTo).get();
    if (version.getNumber() != store.getCurrentVersion() && version.getStatus().equals(status)) {
      return version.getNumber();
    }
    return NON_EXISTING_VERSION;
  }

  /**
   * @return all versions of the specified store.
   */
  List<Version> versionsForStore(String storeName) {
    try (AutoCloseableLock ignore = clusterLockManager.createStoreReadLock(storeName)) {
      Store store = storeRepository.getStore(storeName);
      if (store == null) {
        throw new VeniceNoStoreException(storeName);
      }
      return store.getVersions();
    }
  }

  int getLargestUsedVersion(String storeName) {
    Store store = storeRepository.getStore(storeName);
    // If the store does not exist, check the store graveyard.
    if (store == null) {
      return storeGraveyard.getLargestUsedVersionNumber(storeName);
    }
    return Math.max(store.getLargestUsedVersionNumber(), storeGraveyard.getLargestUsedVersionNumber(storeName));
  }

  private Store getStoreForReadOnly(String storeName) {
    try (AutoCloseableLock ignore = clusterLockManager.createStoreReadLock(storeName)) {
      Store store = storeRepository.getStore(storeName);
      if (store == null) {
        throw new VeniceNoStoreException(storeName);
      }
      return store; /* is a clone */
    }
  }
}
