package com.linkedin.venice.controller;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiStoreInfoResponse;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class CompactionManager {
  public static final int COMPACTION_THRESHOLD_HOURS = 24;
  private static final Logger LOGGER = LogManager.getLogger(CompactionManager.class);
  // TODO: CompactionOrchestrator field

  public CompactionManager() {

  }

  // TODO: repush(store)
  // TODO: one CompactionManager per store repush or a persistent CompactionManager instance that handles all store
  // repushes?

  public void getStoresForCompaction(
      String clusterName,
      Map<String, ControllerClient> childControllers,
      ArrayList<StoreInfo> compactionReadyStores) {
    ArrayList<StoreInfo> storeInfoList = new ArrayList<>();

    // iterate through child controllers
    for (Map.Entry<String, ControllerClient> controller: childControllers.entrySet()) {

      // add all store info to storeInfoList
      MultiStoreInfoResponse response = controller.getValue().getClusterStores(clusterName);
      storeInfoList.addAll(response.getStoreInfoList());
    }

    // filter out
    filterStoresForCompaction(storeInfoList, compactionReadyStores);
  }

  // package exclusive for testing
  void filterStoresForCompaction(ArrayList<StoreInfo> storeInfoList, ArrayList<StoreInfo> compactionReadyStores) {
    for (StoreInfo storeInfo: storeInfoList) {
      if (isCompactionReady(storeInfo)) {
        compactionReadyStores.add(storeInfo);
      }
    }
  }

  // This function abstracts the criteria for a store to be ready for compaction
  private boolean isCompactionReady(StoreInfo storeInfo) {
    boolean isHybridStore = storeInfo.getHybridStoreConfig() != null;

    return isHybridStore && isLastCompactionTimeOlderThanThresholdHours(COMPACTION_THRESHOLD_HOURS, storeInfo);
  }

  // START isCompactionReady() helper methods: each method below encapsulates a log compaction readiness criterion
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
  // END isCompactionReady() helper methods
}
