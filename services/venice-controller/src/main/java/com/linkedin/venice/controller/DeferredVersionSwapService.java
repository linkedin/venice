package com.linkedin.venice.controller;

import static com.linkedin.venice.meta.VersionStatus.ERROR;
import static com.linkedin.venice.meta.VersionStatus.ONLINE;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.linkedin.venice.controller.stats.DeferredVersionSwapStats;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import com.linkedin.venice.utils.RegionUtils;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
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
 */
public class DeferredVersionSwapService extends AbstractVeniceService {
  private final AtomicBoolean stop = new AtomicBoolean(false);
  private final Set<String> allClusters;
  private final VeniceControllerMultiClusterConfig veniceControllerMultiClusterConfig;
  private final VeniceParentHelixAdmin veniceParentHelixAdmin;
  private final ScheduledExecutorService deferredVersionSwapExecutor = Executors.newSingleThreadScheduledExecutor();
  private final DeferredVersionSwapStats deferredVersionSwapStats;
  private static final RedundantExceptionFilter REDUNDANT_EXCEPTION_FILTER =
      new RedundantExceptionFilter(RedundantExceptionFilter.DEFAULT_BITSET_SIZE, TimeUnit.MINUTES.toMillis(10));
  private static final Logger LOGGER = LogManager.getLogger(DeferredVersionSwapService.class);
  private Cache<String, Map<String, Long>> storePushCompletionTimeCache =
      Caffeine.newBuilder().expireAfterWrite(2, TimeUnit.HOURS).build();

  public DeferredVersionSwapService(
      VeniceParentHelixAdmin admin,
      VeniceControllerMultiClusterConfig multiClusterConfig,
      DeferredVersionSwapStats deferredVersionSwapStats) {
    this.veniceParentHelixAdmin = admin;
    this.allClusters = multiClusterConfig.getClusters();
    this.veniceControllerMultiClusterConfig = multiClusterConfig;
    this.deferredVersionSwapStats = deferredVersionSwapStats;
  }

  @Override
  public boolean startInner() throws Exception {
    deferredVersionSwapExecutor.scheduleAtFixedRate(
        new DeferredVersionSwapTask(),
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

  private String getTargetRegion(Set<String> targetRegions) {
    return targetRegions.iterator().next();
  }

  private StoreResponse getStoreForRegion(String clusterName, String targetRegion, String storeName) {
    Map<String, ControllerClient> controllerClientMap =
        veniceParentHelixAdmin.getVeniceHelixAdmin().getControllerClientMap(clusterName);
    ControllerClient targetRegionControllerClient = controllerClientMap.get(targetRegion);
    return targetRegionControllerClient.getStore(storeName);
  }

  private boolean didWaitTimeElapseInTargetRegions(
      Map<String, Long> completionTimes,
      Set<String> targetRegions,
      int waitTime) {
    boolean didWaitTimeElapseInTargetRegions = true;
    for (String targetRegion: targetRegions) {
      long completionTime = completionTimes.get(targetRegion);
      long storeWaitTime = TimeUnit.MINUTES.toSeconds(waitTime);
      long currentTime = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);
      if ((completionTime + storeWaitTime) > currentTime) {
        didWaitTimeElapseInTargetRegions = false;
      }
    }

    return didWaitTimeElapseInTargetRegions;
  }

  private void logMessageIfNotRedundant(String message) {
    if (!REDUNDANT_EXCEPTION_FILTER.isRedundantException(message)) {
      LOGGER.debug(message);
    }
  }

  private class DeferredVersionSwapTask implements Runnable {
    @Override
    public void run() {
      while (!stop.get()) {
        try {
          for (String cluster: allClusters) {
            if (!veniceParentHelixAdmin.isLeaderControllerFor(cluster)) {
              continue;
            }

            List<Store> stores = veniceParentHelixAdmin.getAllStores(cluster);
            for (Store store: stores) {
              int targetVersionNum = store.getLargestUsedVersionNumber();
              Version targetVersion = store.getVersion(targetVersionNum);
              if (targetVersion == null) {
                continue;
              }

              String targetRegionsString = targetVersion.getTargetSwapRegion();
              if (StringUtils.isEmpty(targetRegionsString)) {
                continue;
              }

              // The store is eligible for a version swap if its push job is in terminal status. For a target region
              // push, the parent version status is set to PUSHED in getOfflinePushStatus when this happens
              if (targetVersion.getStatus() != VersionStatus.PUSHED) {
                continue;
              }

              // If we have a cached push completion for this store, check that the waitTime has elapsed before
              // proceeding further
              String storeName = store.getName();
              String kafkaTopicName = Version.composeKafkaTopic(storeName, targetVersionNum);
              Set<String> targetRegions = RegionUtils.parseRegionsFilterList(targetRegionsString);
              Map<String, Long> storePushCompletionTimes = storePushCompletionTimeCache.getIfPresent(kafkaTopicName);
              if (storePushCompletionTimes != null) {
                if (!didWaitTimeElapseInTargetRegions(
                    storePushCompletionTimes,
                    targetRegions,
                    store.getTargetSwapRegionWaitTime())) {
                  String message = "Skipping version swap for store: " + storeName + " on version: " + targetVersionNum
                      + " as wait time: " + store.getTargetSwapRegionWaitTime() + " has not passed";
                  logMessageIfNotRedundant(message);
                  continue;
                }
              }

              Map<String, Integer> coloToVersions =
                  veniceParentHelixAdmin.getCurrentVersionsForMultiColos(cluster, storeName);
              Set<String> remainingRegions = getRegionsForVersionSwap(coloToVersions, targetRegions);

              // Do not perform version swap for davinci stores
              // TODO remove this check once DVC delayed ingestion is completed
              if (veniceControllerMultiClusterConfig.isSkipDeferredVersionSwapForDVCEnabled()) {
                StoreResponse targetRegionStoreResponse =
                    getStoreForRegion(cluster, getTargetRegion(targetRegions), storeName);
                if (targetRegionStoreResponse.isError()) {
                  LOGGER.warn("Got error when fetching targetRegionStore: {}", targetRegionStoreResponse.getError());
                  continue;
                }

                StoreInfo targetRegionStore = targetRegionStoreResponse.getStore();
                Optional<Version> version = targetRegionStore.getVersion(targetVersionNum);
                if (!version.isPresent()) {
                  LOGGER.warn(
                      "Unable to find version {} for store: {} in regions: {}",
                      targetVersionNum,
                      storeName,
                      targetRegionsString);
                  continue;
                }

                if (version.get().getIsDavinciHeartbeatReported()) {
                  String message = "Skipping version swap for store: " + storeName + " on version: " + targetVersionNum
                      + " as there is a davinci heartbeat";
                  logMessageIfNotRedundant(message);
                  continue;
                }
              }

              // Check that push is completed in target regions
              Admin.OfflinePushStatusInfo pushStatusInfo =
                  veniceParentHelixAdmin.getOffLinePushStatus(cluster, kafkaTopicName);
              Set<String> targetRegionsCompleted = new HashSet<>();
              for (String targetRegion: targetRegions) {
                String executionStatus = pushStatusInfo.getExtraInfo().get(targetRegion);
                if (executionStatus.equals(ExecutionStatus.COMPLETED.toString())) {
                  targetRegionsCompleted.add(targetRegion);
                }
              }

              if (targetRegionsCompleted.size() < targetRegions.size() / 2) {
                String message = "Skipping version swap for store: " + storeName + " on version: " + targetVersionNum
                    + "as push is complete in the majority of target regions. Completed target regions: "
                    + targetRegionsCompleted + ", target regions: " + targetRegions;
                logMessageIfNotRedundant(message);
                continue;
              }

              // Check that push is complete in non target regions
              int numNonTargetRegionsFailed = 0;
              Set<String> nonTargetRegionsCompleted = new HashSet<>();
              for (String remainingRegion: remainingRegions) {
                String executionStatus = pushStatusInfo.getExtraInfo().get(remainingRegion);
                if (executionStatus.equals(ExecutionStatus.ERROR.toString())) {
                  numNonTargetRegionsFailed += 1;
                  String message = "Push has error status for store: " + storeName + " on version: " + targetVersionNum
                      + " in a non target region: " + remainingRegion;
                  logMessageIfNotRedundant(message);
                } else if (executionStatus.equals(ExecutionStatus.COMPLETED.toString())) {
                  nonTargetRegionsCompleted.add(remainingRegion);
                }
              }

              // If the majority of the remaining regions have failed their push jobs, mark the version status as ERROR
              // so that we don't check this store again for this version
              HelixVeniceClusterResources resources =
                  veniceParentHelixAdmin.getVeniceHelixAdmin().getHelixVeniceClusterResources(cluster);
              ReadWriteStoreRepository repository = resources.getStoreMetadataRepository();
              if (numNonTargetRegionsFailed > remainingRegions.size() / 2) {
                LOGGER.warn(
                    "Skipping version swap for store: {} on version: {} as majority of non target regions have failed",
                    storeName,
                    targetVersionNum);
                store.updateVersionStatus(targetVersionNum, ERROR);
                repository.updateStore(store);
                continue;
              }

              // Do not perform a version swap if:
              // 1. The majority of the remaining regions have not completed their push yet
              // 2. Any of the remaining regions have yet to reach a terminal status: COMPLETED or ERROR as we need to
              // wait for all of the
              // remaining regions to be completed to account for cases where we have 3 remaining regions and 2
              // COMPLETED, but 1 is STARTED
              int nonTargetRegionsInTerminalStatus = nonTargetRegionsCompleted.size() + numNonTargetRegionsFailed;
              if (nonTargetRegionsCompleted.size() < remainingRegions.size() / 2
                  || nonTargetRegionsInTerminalStatus != remainingRegions.size()) {
                String message = "Skipping version swap for store: " + storeName + " on version: " + targetVersionNum
                    + "as majority of non target regions have not completed their push. Completed non target regions: "
                    + nonTargetRegionsCompleted + ", non target regions: " + remainingRegions;
                logMessageIfNotRedundant(message);
                continue;
              }

              // Check that waitTime has elapsed in target regions
              boolean didWaitTimeElapseInTargetRegions = didWaitTimeElapseInTargetRegions(
                  pushStatusInfo.getExtraInfoUpdateTimestamp(),
                  targetRegions,
                  store.getTargetSwapRegionWaitTime());

              if (!didWaitTimeElapseInTargetRegions) {
                String message = "Skipping version swap for store: " + storeName + " on version: " + targetVersionNum
                    + " as wait time: " + store.getTargetSwapRegionWaitTime() + " has not passed";
                logMessageIfNotRedundant(message);
                storePushCompletionTimeCache.put(kafkaTopicName, pushStatusInfo.getExtraInfoUpdateTimestamp());
                continue;
              }

              // TODO add call for postStoreVersionSwap() once it is implemented

              String regionsToRollForward = RegionUtils.composeRegionList(nonTargetRegionsCompleted);
              LOGGER.info(
                  "Issuing roll forward message for store: {} in regions: {} for version: {}",
                  storeName,
                  regionsToRollForward,
                  targetVersionNum);
              veniceParentHelixAdmin.rollForwardToFutureVersion(cluster, storeName, regionsToRollForward);

              // Once version is swapped in the remaining regions, update parent status to ONLINE so that we don't check
              // this version for version swap again
              store.updateVersionStatus(targetVersionNum, ONLINE);
              repository.updateStore(store);
              LOGGER.info(
                  "Updated parent version status to online for version: {} in store: {}",
                  targetVersionNum,
                  storeName);
            }
          }
        } catch (Exception e) {
          LOGGER.warn("Caught exception: {} while performing deferred version swap", e.toString());
          deferredVersionSwapStats.recordDeferredVersionSwapErrorSensor();
        } catch (Throwable throwable) {
          LOGGER.warn("Caught a throwable: {} while performing deferred version swap", throwable.getMessage());
          deferredVersionSwapStats.recordDeferreredVersionSwapThrowableSensor();
        }
      }
    }
  }
}
