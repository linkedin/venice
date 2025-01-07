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
  private final Admin admin;
  private final Thread deferredVersionSwapThread;
  private final Time time;
  private final long sleepInterval;
  private static final Logger LOGGER = LogManager.getLogger(DeferredVersionSwapService.class);

  public DeferredVersionSwapService(Admin veniceHelixAdmin, VeniceControllerMultiClusterConfig multiClusterConfig) {
    this(veniceHelixAdmin, multiClusterConfig, new SystemTime());
  }

  protected DeferredVersionSwapService(Admin admin, VeniceControllerMultiClusterConfig multiClusterConfig, Time time) {
    this.admin = admin;
    this.time = time;
    this.allClusters = multiClusterConfig.getClusters();
    this.deferredVersionSwapThread = new Thread(new DeferredVersionSwapTask(), "DeferredVersionSwapTask");
    this.veniceControllerMultiClusterConfig = multiClusterConfig;
    this.sleepInterval = multiClusterConfig.getDeferredVersionSwapSleepMs();
  }

  @Override
  public boolean startInner() throws Exception {
    deferredVersionSwapThread.start();
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    stop.set(true);
    deferredVersionSwapThread.interrupt();
  }

  private Set<String> getRegionsForVersionSwap(Map<String, Integer> candidateRegions, Set<String> targetRegions) {
    Set<String> remainingRegions = new HashSet<>(candidateRegions.keySet());
    remainingRegions.removeAll(targetRegions);
    return remainingRegions;
  }

  private class DeferredVersionSwapTask implements Runnable {
    @Override
    public void run() {
      while (!stop.get()) {
        try {
          time.sleep(sleepInterval);
        } catch (InterruptedException e) {
          LOGGER.error("Received InterruptedException during sleep in DeferredVersionSwapTask thread");
          break;
        }

        for (String cluster: allClusters) {
          List<Store> stores = admin.getAllStores(cluster);
          for (Store store: stores) {
            if (StringUtils.isEmpty(store.getTargetSwapRegion())) {
              continue;
            }

            Map<String, Integer> coloToVersions = admin.getCurrentVersionsForMultiColos(cluster, store.getName());
            Set<String> targetRegions = RegionUtils.parseRegionsFilterList(store.getTargetSwapRegion());
            Set<String> remainingRegions = getRegionsForVersionSwap(coloToVersions, targetRegions);

            // Do not perform version swap for davinci stores
            int targetVersion = store.getCurrentVersion();
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
            Admin.OfflinePushStatusInfo pushStatusInfo = admin.getOffLinePushStatus(cluster, kafkaTopicName);
            boolean didPushCompleteInTargetRegions = true;
            for (String targetRegion: targetRegions) {
              String executionStatus = pushStatusInfo.getExtraInfo().get(targetRegion);
              if (executionStatus != ExecutionStatus.COMPLETED.toString()) {
                didPushCompleteInTargetRegions = false;
              }
            }

            if (!didPushCompleteInTargetRegions) {
              LOGGER.info(
                  "Skipping version swap for store: {} on version: {} as push is not complete",
                  store.getName(),
                  targetVersion);
              continue;
            }

            // Check that waitTime has elapsed in target regions
            boolean didWaitTimeElapseInTargetRegions = true;
            for (String targetRegion: targetRegions) {
              Long completionTime = pushStatusInfo.getExtraInfoUpdateTimestamp().get(targetRegion);
              Long storeWaitTime = TimeUnit.MINUTES.toMillis(store.getTargetSwapRegionWaitTime());
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

            // Issue roll forward for remaining regions
            for (String region: remainingRegions) {
              LOGGER.info("Issuing roll forward message for store: {} in region: {}", store.getName(), region);
              admin.rollForwardToFutureVersion(cluster, store.getName(), region);
            }
          }
        }
      }
    }
  }
}
