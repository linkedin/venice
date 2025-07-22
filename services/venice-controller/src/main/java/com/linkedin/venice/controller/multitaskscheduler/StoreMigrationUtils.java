package com.linkedin.venice.controller.multitaskscheduler;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import java.util.List;
import java.util.OptionalInt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class StoreMigrationUtils {
  /**
   * Private constructor to prevent instantiation of this utility class.
   * This class is not meant to be instantiated, as it only contains static methods.
   */
  private StoreMigrationUtils() {
    throw new AssertionError("No instances of " + getClass().getSimpleName());
  }

  private static final Logger LOGGER = LogManager.getLogger(StoreMigrationUtils.class);

  protected static boolean isClonedStoreOnline(
      ControllerClient srcControllerClient,
      ControllerClient destControllerClient,
      MigrationRecord record) {
    String storeName = record.getStoreName();
    StoreResponse storeResponse = srcControllerClient.getStore(storeName);
    StoreInfo srcStoreInfo = storeResponse.getStore();
    if (srcStoreInfo == null) {
      throw new VeniceException("Store " + storeName + " does not exist in the original cluster!");
    }

    StoreInfo destStoreInfo = destControllerClient.getStore(storeName).getStore();
    if (destStoreInfo == null) {
      LOGGER.info(
          "WARN: Cloned store : {} has not been created in the destination cluster : {}!",
          storeName,
          record.getDestinationCluster());
      return false;
    }
    List<Version> srcVersions = srcStoreInfo.getVersions();
    List<Version> destVersions = destStoreInfo.getVersions();

    OptionalInt srcLatestOnlineVersionNum = getLatestOnlineVersionNum(srcVersions);
    OptionalInt destLatestOnlineVersionNum = getLatestOnlineVersionNum(destVersions);

    if (!isUpToDate(srcLatestOnlineVersionNum, destLatestOnlineVersionNum)) {
      LOGGER.info(
          "Store {} is not ready in the destination cluster: {}, Online version in dest cluster: {} and "
              + " Online version in src Cluster: {}",
          storeName,
          record.getDestinationCluster(),
          destLatestOnlineVersionNum,
          srcLatestOnlineVersionNum);
      return false;
    } else {
      LOGGER.info(
          "Store {} is ready in the destination cluster: {}, Online version in dest cluster: {} and "
              + " Online version in src Cluster: {}",
          storeName,
          record.getDestinationCluster(),
          destLatestOnlineVersionNum,
          srcLatestOnlineVersionNum);
    }
    /**
     * The following logic is to check whether the corresponding meta system store and DaVinci push status system store is fully migrated or not.
     */
    return checkSystemStore(
        srcStoreInfo.isStoreMetaSystemStoreEnabled(),
        VeniceSystemStoreType.META_STORE,
        storeName,
        srcControllerClient,
        destControllerClient)
        && checkSystemStore(
            srcStoreInfo.isDaVinciPushStatusStoreEnabled(),
            VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE,
            storeName,
            srcControllerClient,
            destControllerClient);
  }

  /**
   * Check if the system store is ready in the destination cluster.
   *
   * @param enabled          Whether the feature flag for the system store is enabled on the source store.
   * @param type             The type of the system store to check (e.g., META_STORE, DAVINCI_PUSH_STATUS_STORE).
   * @param primaryStoreName  The name of the primary store for which the system store is being checked.
   * @param srcClient        The controller client for the source cluster.
   * @param destClient       The controller client for the destination cluster.
   * @return                 True if the system store is ready, false otherwise.
   */
  protected static boolean checkSystemStore(
      boolean enabled, // feature flag on the source store
      VeniceSystemStoreType type, // META_STORE, DAVINCI_PUSH_STATUS_STORE, …
      String primaryStoreName,
      ControllerClient srcClient,
      ControllerClient destClient) {

    if (!enabled) { // not enabled → trivially “ready”
      return true;
    }
    String systemStoreName = type.getSystemStoreName(primaryStoreName);

    StoreInfo srcSystemStore = srcClient.getStore(systemStoreName).getStore();
    StoreInfo destSystemStore = destClient.getStore(systemStoreName).getStore();

    OptionalInt srcLatest = getLatestOnlineVersionNum(srcSystemStore.getVersions());
    OptionalInt destLatest = getLatestOnlineVersionNum(destSystemStore.getVersions());

    boolean upToDate = isUpToDate(srcLatest, destLatest);
    if (!upToDate) {
      LOGGER.info(
          "{} is not ready. Online version in dest cluster: {}. Online version in src cluster: {}",
          systemStoreName,
          destLatest,
          srcLatest);
    }
    return upToDate;
  }

  protected static OptionalInt getLatestOnlineVersionNum(List<Version> versions) {
    if (versions == null) { // (optional) null-safety
      return OptionalInt.empty();
    }

    return versions.stream()
        .filter(v -> v.getStatus() == VersionStatus.ONLINE)
        .mapToInt(Version::getNumber) // primitive stream → no boxing
        .max(); // O(n), single pass
  }

  protected static boolean isUpToDate(OptionalInt srcVersion, OptionalInt destVersion) {
    int NO_VERSION = Integer.MIN_VALUE;
    int src = srcVersion.orElse(NO_VERSION);
    int dest = destVersion.orElse(NO_VERSION);
    return dest >= src;
  }

  /**
   * Checks whether this migration run has to be paused / resumed.
   *
   * @param rec  current migration rec
   * @return        true  -> rec is (now) paused, caller should abort further work
   *                false -> rec is not paused
   */
  protected static boolean applyPauseIfNeeded(MigrationRecord rec) {
    MigrationRecord.Step pauseAfter = rec.getPauseAfter();
    if (pauseAfter == MigrationRecord.Step.NONE) { // No pausing configured
      return false;
    }

    MigrationRecord.Step current = rec.getCurrentStepEnum();
    boolean shouldBePaused = current.compareTo(pauseAfter) > 0; // past the limit
    boolean isPaused = rec.isPaused();

    // transition to “paused” state
    if (shouldBePaused && !isPaused) {
      LOGGER.info(
          "Migration for store {} is set to pause after step {}, paused at step {} for further execution",
          rec.getStoreName(),
          pauseAfter,
          current);
      rec.setPaused(true);
      return true;
    }

    // transition to “resumed” state
    if (shouldBePaused && isPaused) {
      LOGGER.info("Migration for store {} is resumed from step {}.", rec.getStoreName(), rec.getCurrentStep());
      rec.setPaused(false);
      rec.setPauseAfter(MigrationRecord.Step.NONE);// reset pauseAfter
    }
    return rec.isPaused(); // unchanged state
  }
}
