package com.linkedin.venice.controller;

import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.RegionUtils;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
  private final Time time;
  private static final Logger LOGGER = LogManager.getLogger(DeferredVersionSwapService.class);

  public DeferredVersionSwapService(
      VeniceParentHelixAdmin veniceHelixAdmin,
      VeniceControllerMultiClusterConfig multiClusterConfig) {
    this(veniceHelixAdmin, multiClusterConfig, new SystemTime());
  }

  protected DeferredVersionSwapService(
      VeniceParentHelixAdmin admin,
      VeniceControllerMultiClusterConfig multiClusterConfig,
      Time time) {
    this.veniceParentHelixAdmin = admin;
    this.time = time;
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

  private int getTargetVersionFromTargetRegion(Map<String, Integer> coloToVersions, Set<String> targetRegions) {
    int targetVersion = 0;
    for (String targetRegion: targetRegions) {
      targetVersion = coloToVersions.get(targetRegion);
    }
    return targetVersion;
  }

  private class DeferredVersionSwapTask implements Runnable {
    @Override
    public void run() {
      while (!stop.get()) {
        for (String cluster: allClusters) {
          List<Store> stores = veniceParentHelixAdmin.getAllStores(cluster);
          for (Store store: stores) {
            if (StringUtils.isEmpty(store.getTargetSwapRegion())) {
              continue;
            }

            Map<String, Integer> coloToVersions =
                veniceParentHelixAdmin.getCurrentVersionsForMultiColos(cluster, store.getName());
            Set<String> targetRegions = RegionUtils.parseRegionsFilterList(store.getTargetSwapRegion());
            Set<String> remainingRegions = getRegionsForVersionSwap(coloToVersions, targetRegions);

            // Do not perform version swap for davinci stores
            int targetVersion = getTargetVersionFromTargetRegion(coloToVersions, targetRegions);
            Version version = store.getVersion(targetVersion);
            if (version.getIsDavinciHeartbeatReported()) {
              LOGGER.info(
                  "Skipping version swap for store: {} on version: {} as it is davinci",
                  store.getName(),
                  targetVersion);
              continue;
            }

            // Check that push is completed in target regions
            String kafkaTopicName = Version.composeKafkaTopic(store.getName(), targetVersion);
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
                    targetVersion,
                    targetRegion);
              }
            }

            if (!didPushCompleteInTargetRegions) {
              continue;
            }

            // Check that waitTime has elapsed in target regions
            boolean didWaitTimeElapseInTargetRegions = true;
            for (String targetRegion: targetRegions) {
              long completionTime = pushStatusInfo.getExtraInfoUpdateTimestamp().get(targetRegion);
              long storeWaitTime = TimeUnit.MINUTES.toMillis(store.getTargetSwapRegionWaitTime());
              if (!(completionTime + storeWaitTime <= time.getMilliseconds())) {
                didWaitTimeElapseInTargetRegions = false;
              }
            }

            if (!didWaitTimeElapseInTargetRegions) {
              LOGGER.info(
                  "Skipping version swap for store: {} on version: {} as wait time: {} has not passed",
                  store.getName(),
                  targetVersion,
                  store.getTargetSwapRegionWaitTime());
              continue;
            }

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
