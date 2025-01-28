package com.linkedin.venice.controller;

import static com.linkedin.venice.meta.VersionStatus.ONLINE;

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
  private static final Logger LOGGER = LogManager.getLogger(DeferredVersionSwapService.class);

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
              if (StringUtils.isEmpty(store.getTargetSwapRegion())) {
                continue;
              }

              int targetVersionNum = store.getLargestUsedVersionNumber();
              Version targetVersion = store.getVersion(targetVersionNum);
              if (targetVersion == null) {
                continue;
              }

              // The store is eligible for a version swap if its push job is in terminal status. For a target region
              // push,
              // the parent version status is set to PUSHED in getOfflinePushStatus when this happens
              if (targetVersion.getStatus() != VersionStatus.PUSHED) {
                continue;
              }

              String storeName = store.getName();
              Map<String, Integer> coloToVersions =
                  veniceParentHelixAdmin.getCurrentVersionsForMultiColos(cluster, storeName);
              Set<String> targetRegions = RegionUtils.parseRegionsFilterList(store.getTargetSwapRegion());
              Set<String> remainingRegions = getRegionsForVersionSwap(coloToVersions, targetRegions);

              StoreResponse targetRegionStoreResponse =
                  getStoreForRegion(cluster, getTargetRegion(targetRegions), storeName);
              if (!StringUtils.isEmpty(targetRegionStoreResponse.getError())) {
                LOGGER.info("Got error when fetching targetRegionStore: {}", targetRegionStoreResponse.getError());
                continue;
              }

              StoreInfo targetRegionStore = targetRegionStoreResponse.getStore();
              Optional<Version> version = targetRegionStore.getVersion(targetVersionNum);
              if (!version.isPresent()) {
                LOGGER.info(
                    "Unable to find version {} for store: {} in regions: {}",
                    targetVersionNum,
                    storeName,
                    store.getTargetSwapRegion());
                continue;
              }

              // Do not perform version swap for davinci stores
              // TODO remove this check once DVC delayed ingestion is completed
              if (version.get().getIsDavinciHeartbeatReported()) {
                LOGGER.info(
                    "Skipping version swap for store: {} on version: {} as it is davinci",
                    storeName,
                    targetVersionNum);
                continue;
              }

              // Check that push is completed in target regions
              String kafkaTopicName = Version.composeKafkaTopic(storeName, targetVersionNum);
              Admin.OfflinePushStatusInfo pushStatusInfo =
                  veniceParentHelixAdmin.getOffLinePushStatus(cluster, kafkaTopicName);
              boolean didPushCompleteInTargetRegions = true;
              for (String targetRegion: targetRegions) {
                String executionStatus = pushStatusInfo.getExtraInfo().get(targetRegion);
                if (!executionStatus.equals(ExecutionStatus.COMPLETED.toString())) {
                  didPushCompleteInTargetRegions = false;
                  LOGGER.info(
                      "Skipping version swap for store: {} on version: {} as push is not complete in target region {}",
                      storeName,
                      targetVersionNum,
                      targetRegion);
                }
              }

              if (!didPushCompleteInTargetRegions) {
                continue;
              }

              // Check that push is complete in non target regions
              boolean didPushCompleteInNonTargetRegions = true;
              for (String remainingRegion: remainingRegions) {
                String executionStatus = pushStatusInfo.getExtraInfo().get(remainingRegion);
                if (!executionStatus.equals(ExecutionStatus.COMPLETED.toString())) {
                  didPushCompleteInNonTargetRegions = false;
                  LOGGER.info(
                      "Skipping version swap for store: {} on version: {} as push is not complete in a non target region: {}",
                      storeName,
                      targetVersionNum,
                      remainingRegion);
                }
              }

              if (!didPushCompleteInNonTargetRegions) {
                continue;
              }

              // Check that waitTime has elapsed in target regions
              boolean didWaitTimeElapseInTargetRegions = false;
              for (String targetRegion: targetRegions) {
                long completionTime = pushStatusInfo.getExtraInfoUpdateTimestamp().get(targetRegion);
                long storeWaitTime = TimeUnit.MINUTES.toSeconds(store.getTargetSwapRegionWaitTime());
                long currentTime = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);
                if ((completionTime + storeWaitTime) <= currentTime) {
                  didWaitTimeElapseInTargetRegions = true;
                }
              }

              if (!didWaitTimeElapseInTargetRegions) {
                LOGGER.info(
                    "Skipping version swap for store: {} on version: {} as wait time: {} has not passed",
                    storeName,
                    targetVersionNum,
                    store.getTargetSwapRegionWaitTime());
                continue;
              }

              // TODO add call for postStoreVersionSwap() once it is implemented

              String remainingRegionsString = String.join(",\\s*", remainingRegions);
              LOGGER
                  .info("Issuing roll forward message for store: {} in regions: {}", storeName, remainingRegionsString);
              veniceParentHelixAdmin.rollForwardToFutureVersion(cluster, storeName, remainingRegionsString);

              // Once version is swapped in the remaining regions, update parent status to ONLINE so that we don't check
              // this version
              // for version swap again
              HelixVeniceClusterResources resources =
                  veniceParentHelixAdmin.getVeniceHelixAdmin().getHelixVeniceClusterResources(cluster);
              ReadWriteStoreRepository repository = resources.getStoreMetadataRepository();
              Store parentStore = repository.getStore(storeName);
              parentStore.updateVersionStatus(targetVersionNum, ONLINE);
              repository.updateStore(parentStore);
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
