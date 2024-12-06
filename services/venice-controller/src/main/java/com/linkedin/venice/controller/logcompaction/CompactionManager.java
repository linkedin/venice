package com.linkedin.venice.controller.logcompaction;

import com.linkedin.venice.controller.repush.RepushJobResponse;
import com.linkedin.venice.controller.repush.RepushOrchestrator;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiStoreInfoResponse;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class CompactionManager {
  public static final int DEFAULT_COMPACTION_THRESHOLD_HOURS = 24;
  private static final Logger LOGGER = LogManager.getLogger(CompactionManager.class);

  private RepushOrchestrator repushOrchestrator;
  // TODO: where to configure implementation of RepushOrchestrator to use?

  public CompactionManager(RepushOrchestrator repushOrchestrator) {
    this.repushOrchestrator = repushOrchestrator;
  }

  public List<StoreInfo> getStoresForCompaction(String clusterName, Map<String, ControllerClient> childControllers) {
    ArrayList<StoreInfo> storeInfoList = new ArrayList<>();

    // iterate through child controllers
    for (Map.Entry<String, ControllerClient> controller: childControllers.entrySet()) {

      // add all store info to storeInfoList
      MultiStoreInfoResponse response = controller.getValue().getClusterStores(clusterName);
      storeInfoList.addAll(response.getStoreInfoList());
    }

    // filter out
    return filterStoresForCompaction(storeInfoList);
  }

  // public for testing
  // TODO: make private or package protected
  public List<StoreInfo> filterStoresForCompaction(ArrayList<StoreInfo> storeInfoList) {
    ArrayList<StoreInfo> compactionReadyStores = new ArrayList<>();
    for (StoreInfo storeInfo: storeInfoList) {
      if (isCompactionReady(storeInfo)) {
        compactionReadyStores.add(storeInfo);
      }
    }
    return compactionReadyStores;
  }

  // This function abstracts the criteria for a store to be ready for compaction
  private boolean isCompactionReady(StoreInfo storeInfo) {
    boolean isHybridStore = storeInfo.getHybridStoreConfig() != null;

    return isHybridStore && isLastCompactionTimeOlderThanThresholdHours(DEFAULT_COMPACTION_THRESHOLD_HOURS, storeInfo);
  }

  /**
   * This function checks if the last compaction time is older than the threshold.
   * @param compactionThresholdHours, the number of hours that the last compaction time should be older than
   * @param storeInfo, the store to check the last compaction time for
   * @return true if the last compaction time is older than the threshold, false otherwise
   */
  private boolean isLastCompactionTimeOlderThanThresholdHours(int compactionThresholdHours, StoreInfo storeInfo) {
    // get the last compaction time
    int currentVersionNumber = storeInfo.getCurrentVersion();
    Optional<Version> currentVersion = storeInfo.getVersion(currentVersionNumber);
    if (!currentVersion.isPresent()) {
      LOGGER.warn("Couldn't find current version: {} from store: {}", currentVersionNumber, storeInfo.getName());
      return false; // invalid store because no current version, this store is not eligible for compaction
    }

    // calculate hours since last compaction
    long lastCompactionTime = currentVersion.get().getCreatedTime();
    long currentTime = System.currentTimeMillis();
    long millisecondsSinceLastCompaction = currentTime - lastCompactionTime;
    long hoursSinceLastCompaction = TimeUnit.MILLISECONDS.toHours(millisecondsSinceLastCompaction);

    return hoursSinceLastCompaction > compactionThresholdHours;
  }

  public void compactStore(String storeName) {
    try {
      RepushJobResponse response = repushOrchestrator.repush(storeName);
      LOGGER.info(
          "Repush job triggered for store: {} with job name: {} and job exec id: {}",
          response.getStoreName(),
          response.getJobName(),
          response.getJobExecId());
    } catch (Exception e) {
      LOGGER.error("Failed to compact store: {}", storeName, e);
    }
  }
}
