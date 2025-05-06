package com.linkedin.venice.controller;

import static com.linkedin.venice.meta.VersionStatus.ERROR;
import static com.linkedin.venice.meta.VersionStatus.ONLINE;
import static com.linkedin.venice.meta.VersionStatus.PARTIALLY_ONLINE;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.linkedin.venice.controller.stats.DeferredVersionSwapStats;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.exceptions.VeniceNoClusterException;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.LogContext;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import com.linkedin.venice.utils.RegionUtils;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This service is in charge of swapping to a new version after a specified wait time in the remaining regions of a target region push if enabled.
 * The wait time is specified through a store/version level config (target_swap_region_wait_time) and the default wait time is 60m.
 * This service also updates the parent version status of a store w/ a terminal push status after performing the swap
 * or deeming it ineligible for a version swap and the statuses mean:
 * 1. ONLINE - all regions are serving the target version
 * 2. PARTIALLY_ONLINE - 1+ regions are serving the target version, but 1+ regions' pushes failed & are serving the old version
 * 3. ERROR - all regions are serving the old version. the push failed in target regions
 */
public class DeferredVersionSwapService extends AbstractVeniceService {
  private final AtomicBoolean stop = new AtomicBoolean(false);
  private final VeniceControllerMultiClusterConfig veniceControllerMultiClusterConfig;
  private final VeniceParentHelixAdmin veniceParentHelixAdmin;
  private final ScheduledExecutorService deferredVersionSwapExecutor = Executors.newSingleThreadScheduledExecutor();
  private final DeferredVersionSwapStats deferredVersionSwapStats;
  private static final RedundantExceptionFilter REDUNDANT_EXCEPTION_FILTER =
      new RedundantExceptionFilter(RedundantExceptionFilter.DEFAULT_BITSET_SIZE, TimeUnit.MINUTES.toMillis(10));
  private static final Logger LOGGER = LogManager.getLogger(DeferredVersionSwapService.class);
  private Cache<String, Map<String, Long>> storePushCompletionTimeCache =
      Caffeine.newBuilder().expireAfterWrite(2, TimeUnit.HOURS).build();
  private HashMap<String, Integer> fetchNonTargetRegionStoreRetryCounter = new HashMap<>();
  private static final int maxFetchNonTargetRegionStoreRetryCounter = 5;

  public DeferredVersionSwapService(
      VeniceParentHelixAdmin admin,
      VeniceControllerMultiClusterConfig multiClusterConfig,
      DeferredVersionSwapStats deferredVersionSwapStats) {
    this.veniceParentHelixAdmin = admin;
    this.veniceControllerMultiClusterConfig = multiClusterConfig;
    this.deferredVersionSwapStats = deferredVersionSwapStats;
  }

  @Override
  public boolean startInner() throws Exception {
    deferredVersionSwapExecutor.scheduleAtFixedRate(
        getRunnableForDeferredVersionSwap(),
        0,
        veniceControllerMultiClusterConfig.getDeferredVersionSwapSleepMs(),
        TimeUnit.MILLISECONDS);
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    stop.set(true);
    deferredVersionSwapExecutor.shutdown();
  }

  private Set<String> getRegionsForVersionSwap(Map<String, Integer> candidateRegions, Set<String> targetRegions) {
    Set<String> remainingRegions = new HashSet<>(candidateRegions.keySet());
    remainingRegions.removeAll(targetRegions);
    return remainingRegions;
  }

  private StoreResponse getStoreForRegion(String clusterName, String targetRegion, String storeName) {
    Map<String, ControllerClient> controllerClientMap =
        veniceParentHelixAdmin.getVeniceHelixAdmin().getControllerClientMap(clusterName);
    ControllerClient targetRegionControllerClient = controllerClientMap.get(targetRegion);
    return targetRegionControllerClient.getStore(storeName);
  }

  /**
   * Checks whether the wait time has passed since the push completion time in the list of regions
   * @param completionTimes a mapping of region to push completion time
   * @param targetRegions the list of regions to check if wait time has elapsed
   * @param store the store to check if the push wait time has elapsed
   * @param targetVersionNum the version to check if the push wait time has elapsed
   * @return
   */
  private boolean didWaitTimeElapseInTargetRegions(
      Map<String, Long> completionTimes,
      Set<String> targetRegions,
      Store store,
      int targetVersionNum) {
    for (String targetRegion: targetRegions) {
      if (!completionTimes.containsKey(targetRegion)) {
        continue;
      }

      long completionTime = completionTimes.get(targetRegion);
      long storeWaitTime = TimeUnit.MINUTES.toSeconds(store.getTargetSwapRegionWaitTime());
      long currentTime = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);
      if ((completionTime + storeWaitTime) > currentTime) {
        String message = "Skipping version swap for store: " + store.getName() + " on version: " + targetVersionNum
            + " as wait time: " + store.getTargetSwapRegionWaitTime() + " has not passed";
        logMessageIfNotRedundant(message);
        return false;
      }
    }

    return true;
  }

  /**
   * Checks whether the wait time has passed since the cached push completion time in the list of regions
   * @param targetRegions the list of regions to check if wait time has elapsed
   * @param store the store to check if the push wait time has elapsed
   * @param targetVersionNum the version to check if the push wait time has elapsed
   * @param kafkaTopicName the name of the kafka topic for this target version
   * @return
   */
  private boolean didCachedWaitTimeElapseInTargetRegions(
      Set<String> targetRegions,
      Store store,
      int targetVersionNum,
      String kafkaTopicName) {
    Map<String, Long> storePushCompletionTimes = storePushCompletionTimeCache.getIfPresent(kafkaTopicName);

    // If there is no cached completion time, we should let the service continue the checks for the store as:
    // 1. It could be a new push that we haven't checked for yet
    // 2. The existing cached wait time expired
    if (storePushCompletionTimes == null) {
      return true;
    }

    return didWaitTimeElapseInTargetRegions(storePushCompletionTimes, targetRegions, store, targetVersionNum);
  }

  private void logMessageIfNotRedundant(String message) {
    if (!REDUNDANT_EXCEPTION_FILTER.isRedundantException(message)) {
      LOGGER.info(message);
    }
  }

  /**
   * Gets the specified version for a store in a specific region
   * @param clusterName name of the cluster the store is in
   * @param region name of the region to get the store from
   * @param storeName name of the store
   * @param targetVersionNum the version number to get
   * @return
   */
  private Version getVersionFromStoreInRegion(
      String clusterName,
      String region,
      String storeName,
      int targetVersionNum) {
    StoreResponse targetRegionStoreResponse = getStoreForRegion(clusterName, region, storeName);

    if (targetRegionStoreResponse.isError()) {
      String message = "Got error when fetching targetRegionStore: " + targetRegionStoreResponse.getStore();
      logMessageIfNotRedundant(message);
      return null;
    }

    StoreInfo targetRegionStore = targetRegionStoreResponse.getStore();
    Optional<Version> version = targetRegionStore.getVersion(targetVersionNum);
    if (!version.isPresent()) {
      String message =
          "Unable to find version " + targetVersionNum + " for store: " + storeName + " in region " + region;
      logMessageIfNotRedundant(message);
      return null;
    }

    return version.get();
  }

  /**
   * Roll forward to the specified version for a list of regions. Once the roll forward is done, traffic will be served from
   * that version and the version status will be updated to ONLINE or PARTIALLY_ONLINE
   * @param regions the list of regions to update the version status
   * @param store the store of the version to roll forward in
   * @param targetVersion the version to start serving traffic in
   * @param cluster the cluster the store is in
   */
  private void rollForwardToTargetVersion(
      Set<String> regions,
      Store store,
      Version targetVersion,
      String cluster,
      ReadWriteStoreRepository repository) {
    String regionsToRollForward = RegionUtils.composeRegionList(regions);
    String storeName = store.getName();
    int targetVersionNum = targetVersion.getNumber();
    LOGGER.info(
        "Issuing roll forward message for store: {} in regions: {} for version: {}",
        storeName,
        regionsToRollForward,
        targetVersionNum);
    veniceParentHelixAdmin.rollForwardToFutureVersion(cluster, storeName, regionsToRollForward);

    // Update parent version status after roll forward, so we don't check this store version again
    // If push was successful (version status is PUSHED), the parent version is marked as ONLINE
    // if push was successful in some regions (version status is KILLED), the parent version is marked PARTIALLY_ONLINE
    if (targetVersion.getStatus() == VersionStatus.PUSHED) {
      store.updateVersionStatus(targetVersionNum, ONLINE);
      repository.updateStore(store);
      LOGGER.info("Updated parent version status to ONLINE for version: {} in store: {}", targetVersionNum, storeName);
    } else {
      store.updateVersionStatus(targetVersionNum, VersionStatus.PARTIALLY_ONLINE);
      repository.updateStore(store);
      LOGGER.info(
          "Updated parent version status to PARTIALLY_ONLINE for version: {} in store: {}",
          targetVersionNum,
          storeName);
    }
  }

  /**
   * Checks if a store version is in a terminal state. It is in a terminal state & eligible for a version swap if targetSwapRegion is not
   * empty and the push job for the current version is completed. It is completed if the version status is either PUSHED or
   * KILLED (see VeniceParentHelixAdmin.getOfflinePushStatus)
   * @param targetVersion the version to check eligibility for
   * @return
   */
  private boolean isPushInTerminalState(Version targetVersion) {
    if (targetVersion == null) {
      return false;
    }

    String targetRegionsString = targetVersion.getTargetSwapRegion();
    if (StringUtils.isEmpty(targetRegionsString)) {
      return false;
    }

    // The store is eligible for a version swap if its push job is in terminal status. For a target region
    // push, the parent version status is set to PUSHED in getOfflinePushStatus when this happens or KILLED if the push
    // failed. PUSHED represents when a push successfully completes in all regions and KILLED represents when a push
    // fails in
    // 1+ regions. KILLED is still eligible for a version swap because some non target regions may have succeeded, and
    // we
    // need to perform a version swap for those regions
    if (targetVersion.getStatus() != VersionStatus.PUSHED && targetVersion.getStatus() != VersionStatus.KILLED) {
      return false;
    }

    return true;
  }

  /**
   * Given a list of regions and push statuses and an expected status, return the number of regions in that status
   * @param regions list of regions to find the push status for
   * @param pushStatusInfo wrapper containing push status info
   * @param status expected status to search for
   * @return
   */
  private int getRegionsWithPushStatusCount(
      Set<String> regions,
      Admin.OfflinePushStatusInfo pushStatusInfo,
      ExecutionStatus status) {
    int regionsWithStatus = 0;
    for (String region: regions) {
      String executionStatus = pushStatusInfo.getExtraInfo().get(region);
      if (executionStatus.equals(status.toString())) {
        regionsWithStatus += 1;
      }
    }
    return regionsWithStatus;
  }

  /**
   * Checks if a push completed in all target regions.
   * If the push failed in a majority of target regions, mark the parent version status as ERROR.
   * If all target regions have not reached a terminal push status yet, do not proceed yet
   * @param targetRegions list of regions to check the push status for
   * @param pushStatusInfo wrapper containing push status information
   * @return
   */
  private boolean didPushCompleteInTargetRegions(
      Set<String> targetRegions,
      Admin.OfflinePushStatusInfo pushStatusInfo,
      ReadWriteStoreRepository repository,
      Store store,
      int targetVersionNum,
      String kafkaTopicName) {
    int numCompletedTargetRegions =
        getRegionsWithPushStatusCount(targetRegions, pushStatusInfo, ExecutionStatus.COMPLETED);
    int numFailedTargetRegions = getRegionsWithPushStatusCount(targetRegions, pushStatusInfo, ExecutionStatus.ERROR);
    if (numFailedTargetRegions > 0) {
      String message = "Skipping version swap for store: " + store.getName() + " on version: " + targetVersionNum
          + " as push failed in 1+ target regions. Completed target regions: " + numCompletedTargetRegions
          + " , failed target regions: " + numFailedTargetRegions + ", target regions: " + targetRegions;
      logMessageIfNotRedundant(message);
      store.updateVersionStatus(targetVersionNum, ERROR);
      repository.updateStore(store);
      return false;
    } else if (numCompletedTargetRegions + numFailedTargetRegions != targetRegions.size()) {
      return false;
    }

    return true;
  }

  /**
   * Gets a list of eligible regions to roll forward in. A region is eligible to be rolled forward if it's push status is
   * COMPLETED. If there are no eligible regions to roll forward in or if not all regions have reached a terminal status, null is
   * returned and the version status is marked as PARTIALLY_ONLINE as only the target regions are serving traffic from the new version
   * @param nonTargetRegions list of regions to check eligibility for
   * @param repository repository to update store
   * @param store store to update
   * @param targetVersionNum target version to roll forward in
   * @param clusterName cluster the store is in
   * @return
   */
  private Set<String> getRegionsToRollForward(
      Set<String> nonTargetRegions,
      ReadWriteStoreRepository repository,
      Store store,
      int targetVersionNum,
      String clusterName,
      String kafkaTopicName) {

    Set<String> completedNonTargetRegions = new HashSet<>();
    Set<String> failedNonTargetRegions = new HashSet<>();
    for (String nonTargetRegion: nonTargetRegions) {
      Version version = getVersionFromStoreInRegion(clusterName, nonTargetRegion, store.getName(), targetVersionNum);

      // When a push is killed or errored out, the topic may have been cleaned up or controller is temporarily
      // unreachable so we will allow upto 5 retries before marking it as failed
      if (version == null) {
        String regionKafkaTopicName = nonTargetRegion + "_" + kafkaTopicName;
        int attemptedRetries = fetchNonTargetRegionStoreRetryCounter.compute(regionKafkaTopicName, (k, v) -> {
          if (v == null) {
            return 1;
          }
          return v + 1;
        });

        if (attemptedRetries == maxFetchNonTargetRegionStoreRetryCounter) {
          failedNonTargetRegions.add(nonTargetRegion);
          fetchNonTargetRegionStoreRetryCounter.remove(regionKafkaTopicName);
        }

        continue;
      }

      if (version.getStatus().equals(VersionStatus.PUSHED)) {
        completedNonTargetRegions.add(nonTargetRegion);
      } else if (version.getStatus().equals(ERROR) || version.getStatus().equals(VersionStatus.KILLED)) {
        failedNonTargetRegions.add(nonTargetRegion);
      }
    }

    if (failedNonTargetRegions.size() == nonTargetRegions.size()) {
      String message = "Skipping version swap for store: " + store.getName() + " on version: " + targetVersionNum
          + "as push failed in all non target regions. Failed non target regions: " + failedNonTargetRegions
          + ", non target regions: " + nonTargetRegions;
      logMessageIfNotRedundant(message);
      store.updateVersionStatus(targetVersionNum, PARTIALLY_ONLINE);
      repository.updateStore(store);
      return Collections.emptySet();
    } else if ((failedNonTargetRegions.size() + completedNonTargetRegions.size()) != nonTargetRegions.size()) {
      String message = "Skipping version swap for store: " + store.getName() + " on version: " + targetVersionNum
          + "as push is not in terminal status in all non target regions. Completed non target regions: "
          + completedNonTargetRegions + ", failed non target regions: " + failedNonTargetRegions
          + ", non target regions: " + nonTargetRegions;
      logMessageIfNotRedundant(message);
      return Collections.emptySet();
    }

    return completedNonTargetRegions;
  }

  private Runnable getRunnableForDeferredVersionSwap() {
    return () -> {
      LogContext.setStructuredLogContext(veniceControllerMultiClusterConfig.getLogContext());
      if (stop.get()) {
        return;
      }

      try {
        for (String cluster: veniceParentHelixAdmin.getClustersLeaderOf()) {
          if (!veniceParentHelixAdmin.isLeaderControllerFor(cluster)) {
            continue;
          }

          List<Store> parentStores;
          try {
            parentStores = veniceParentHelixAdmin.getAllStores(cluster);
          } catch (VeniceNoClusterException e) {
            LOGGER.warn("Leadership changed during getAllStores call for cluster: {}", cluster, e);
            break;
          }

          for (Store parentStore: parentStores) {
            int targetVersionNum = parentStore.getLargestUsedVersionNumber();
            Version targetVersion = parentStore.getVersion(targetVersionNum);

            // Check if the target version is in a terminal state (push job completed or failed)
            if (!isPushInTerminalState(targetVersion)) {
              continue;
            }

            // Check if the cached waitTime for the target version has elapsed
            String storeName = parentStore.getName();
            String kafkaTopicName = Version.composeKafkaTopic(storeName, targetVersionNum);
            Set<String> targetRegions = RegionUtils.parseRegionsFilterList(targetVersion.getTargetSwapRegion());
            if (!didCachedWaitTimeElapseInTargetRegions(targetRegions, parentStore, targetVersionNum, kafkaTopicName)) {
              continue;
            }

            Map<String, Integer> coloToVersions =
                veniceParentHelixAdmin.getCurrentVersionsForMultiColos(cluster, storeName);
            Admin.OfflinePushStatusInfo pushStatusInfo =
                veniceParentHelixAdmin.getOffLinePushStatus(cluster, kafkaTopicName);
            HelixVeniceClusterResources resources =
                veniceParentHelixAdmin.getVeniceHelixAdmin().getHelixVeniceClusterResources(cluster);
            ReadWriteStoreRepository repository = resources.getStoreMetadataRepository();
            Set<String> remainingRegions = getRegionsForVersionSwap(coloToVersions, targetRegions);

            // If version status is marked as KILLED (push timeout, user killed push job, etc), check if target
            // regions failed
            if (targetVersion.getStatus() == VersionStatus.KILLED) {
              if (!didPushCompleteInTargetRegions(
                  targetRegions,
                  pushStatusInfo,
                  repository,
                  parentStore,
                  targetVersionNum,
                  kafkaTopicName)) {
                continue;
              }
            }

            // Get eligible non target regions to roll forward in
            Set<String> nonTargetRegionsCompleted = getRegionsToRollForward(
                remainingRegions,
                repository,
                parentStore,
                targetVersionNum,
                cluster,
                kafkaTopicName);
            if (nonTargetRegionsCompleted.isEmpty()) {
              continue;
            }

            // Check that waitTime has elapsed in target regions
            if (!didWaitTimeElapseInTargetRegions(
                pushStatusInfo.getExtraInfoUpdateTimestamp(),
                targetRegions,
                parentStore,
                targetVersionNum)) {
              storePushCompletionTimeCache.put(kafkaTopicName, pushStatusInfo.getExtraInfoUpdateTimestamp());
              continue;
            }

            // TODO add call for postStoreVersionSwap() once it is implemented

            // Switch to the target version in the completed non target regions
            try {
              rollForwardToTargetVersion(nonTargetRegionsCompleted, parentStore, targetVersion, cluster, repository);
            } catch (Exception e) {
              LOGGER.warn("Failed to roll forward for store: {} in version: {}", storeName, targetVersionNum, e);
              deferredVersionSwapStats.recordDeferredVersionSwapFailedRollForwardSensor();

              parentStore.updateVersionStatus(targetVersionNum, VersionStatus.PARTIALLY_ONLINE);
              repository.updateStore(parentStore);
              LOGGER.info(
                  "Updated parent version status to PARTIALLY_ONLINE for version: {} in store: {} after failing to roll forward in non target regions: {}",
                  targetVersionNum,
                  storeName,
                  nonTargetRegionsCompleted);
            }
          }
        }
      } catch (Exception e) {
        LOGGER.warn("Caught exception while performing deferred version swap", e);
        deferredVersionSwapStats.recordDeferredVersionSwapErrorSensor();
      } catch (Throwable throwable) {
        LOGGER.warn("Caught a throwable while performing deferred version swap", throwable);
        deferredVersionSwapStats.recordDeferredVersionSwapThrowableSensor();
      }
    };
  }

  // Only used for testing
  Cache<String, Map<String, Long>> getStorePushCompletionTimes() {
    return storePushCompletionTimeCache;
  }
}
