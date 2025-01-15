package com.linkedin.venice.controller;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
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
  private static final Logger LOGGER = LogManager.getLogger(DeferredVersionSwapService.class);

  public DeferredVersionSwapService(
      VeniceParentHelixAdmin admin,
      VeniceControllerMultiClusterConfig multiClusterConfig) {
    this.veniceParentHelixAdmin = admin;
    this.allClusters = multiClusterConfig.getClusters();
    this.veniceControllerMultiClusterConfig = multiClusterConfig;
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

  private StoreResponse getStoreForRegion(String clusterName, Set<String> targetRegions, String storeName) {
    Map<String, ControllerClient> controllerClientMap =
        veniceParentHelixAdmin.getVeniceHelixAdmin().getControllerClientMap(clusterName);
    ControllerClient targetRegionControllerClient = controllerClientMap.get(getTargetRegion(targetRegions));
    return targetRegionControllerClient.getStore(storeName);
  }

  private class DeferredVersionSwapTask implements Runnable {
    @Override
    public void run() {
      while (!stop.get()) {
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

            if (targetVersion.getStatus() != VersionStatus.STARTED) {
              LOGGER.info(
                  "Skipping target region swap for store {} as target version {} is already online",
                  store.getName(),
                  targetVersionNum);
              continue;
            }

            if (StringUtils.isEmpty(store.getTargetSwapRegion())) {
              continue;
            }

            Map<String, Integer> coloToVersions =
                veniceParentHelixAdmin.getCurrentVersionsForMultiColos(cluster, store.getName());
            Set<String> targetRegions = RegionUtils.parseRegionsFilterList(store.getTargetSwapRegion());
            Set<String> remainingRegions = getRegionsForVersionSwap(coloToVersions, targetRegions);

            StoreResponse targetRegionStoreResponse = getStoreForRegion(cluster, targetRegions, store.getName());
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
                  store.getName(),
                  store.getTargetSwapRegion());
              continue;
            }

            // Do not perform version swap for davinci stores
            // TODO remove this check once DVC delayed ingestion is completed
            if (version.get().getIsDavinciHeartbeatReported()) {
              LOGGER.info(
                  "Skipping version swap for store: {} on version: {} as it is davinci",
                  store.getName(),
                  targetVersionNum);
              continue;
            }

            // Check that push is completed in target regions
            String kafkaTopicName = Version.composeKafkaTopic(store.getName(), targetVersionNum);
            Admin.OfflinePushStatusInfo pushStatusInfo =
                veniceParentHelixAdmin.getOffLinePushStatus(cluster, kafkaTopicName);
            boolean didPushCompleteInTargetRegions = true;
            for (String targetRegion: targetRegions) {
              String executionStatus = pushStatusInfo.getExtraInfo().get(targetRegion);
              if (!executionStatus.equals(ExecutionStatus.COMPLETED.toString())) {
                didPushCompleteInTargetRegions = false;
                LOGGER.info(
                    "Skipping version swap for store: {} on version: {} as push is not complete in region {}",
                    store.getName(),
                    targetVersionNum,
                    targetRegion);
              }
            }

            if (!didPushCompleteInTargetRegions) {
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
                  store.getName(),
                  targetVersionNum,
                  store.getTargetSwapRegionWaitTime());
              continue;
            }

            // TODO add call for postStoreVersionSwap() once it is implemented

            String remainingRegionsString = String.join(",\\s*", remainingRegions);
            LOGGER.info(
                "Issuing roll forward message for store: {} in regions: {}",
                store.getName(),
                remainingRegionsString);
            veniceParentHelixAdmin.rollForwardToFutureVersion(cluster, store.getName(), remainingRegionsString);
          }
        }
      }
    }
  }
}
