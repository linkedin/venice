package com.linkedin.venice.controller.logcompaction;

import com.linkedin.venice.annotation.VisibleForTesting;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controller.repush.RepushJobRequest;
import com.linkedin.venice.controller.repush.RepushOrchestrator;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiStoreInfoResponse;
import com.linkedin.venice.controllerapi.RepushJobResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class contains functions used by {@link com.linkedin.venice.controller.VeniceHelixAdmin} to:
 * 1. Get stores ready for compaction based on a set of criteria. These criteria have individual functions if they involve
 * multiple steps.
 * 2. Trigger repush to compact a store with function {@link RepushOrchestrator#repush(RepushJobRequest)} & processes the status/response of the repush job.
 */
public class CompactionManager {
  private static final Logger LOGGER = LogManager.getLogger(CompactionManager.class + " [log-compaction]");

  private final RepushOrchestrator repushOrchestrator;
  private final long timeSinceLastLogCompactionThresholdMs;

  public CompactionManager(RepushOrchestrator repushOrchestrator, long timeSinceLastLogCompactionThresholdMs) {
    this.repushOrchestrator = repushOrchestrator;
    this.timeSinceLastLogCompactionThresholdMs = timeSinceLastLogCompactionThresholdMs;
  }

  /**
   * This function iterates over a list of child controllers,
   * in order to obtain the list of stores in each child controller,
   * and then filter out the stores that are ready for compaction with function {@link CompactionManager#filterStoresForCompaction}.
   * @param clusterName cluster to look for compaction-ready stores in
   * @param childControllers list of controllers to look for compaction-ready stores in
   * @return list of StoreInfo of stores ready for log compaction in clusterName
   */
  public List<StoreInfo> getStoresForCompaction(String clusterName, Map<String, ControllerClient> childControllers) {
    ArrayList<StoreInfo> storeInfoList = new ArrayList<>();

    // iterate through child controllers
    for (Map.Entry<String, ControllerClient> controller: childControllers.entrySet()) {

      // add all store info to storeInfoList
      MultiStoreInfoResponse response = controller.getValue().getClusterStores(clusterName);
      storeInfoList.addAll(response.getStoreInfoList());
    }

    // filter for stores ready for log compaction
    return filterStoresForCompaction(storeInfoList);
  }

  // public for testing
  @VisibleForTesting
  List<StoreInfo> filterStoresForCompaction(List<StoreInfo> storeInfoList) {
    return storeInfoList.stream().filter(this::isCompactionReady).collect(Collectors.toList());
  }

  /**
   * This function abstracts the criteria for a store to be ready for compaction
   *
   * public for testing in {@link com.linkedin.venice.endToEnd.TestHybrid#testHybridStoreLogCompaction}
   * TODO: move TestHybrid::testHybridStoreLogCompaction to TestCompactionManager, then make this class package private
   */
  //
  public boolean isCompactionReady(StoreInfo storeInfo) {
    boolean isHybridStore = storeInfo.getHybridStoreConfig() != null;

    return isHybridStore && isLastCompactionTimeOlderThanThreshold(timeSinceLastLogCompactionThresholdMs, storeInfo)
        && storeInfo.isActiveActiveReplicationEnabled() && !VeniceSystemStoreUtils.isSystemStore(storeInfo.getName())
        && storeInfo.isCompactionEnabled();
  }

  /**
   * This function checks if the last compaction time is older than the threshold.
   * @param compactionThresholdMs, the number of hours that the last compaction time should be older than
   * @param storeInfo, the store to check the last compaction time for
   * @return true if the last compaction time is older than the threshold, false otherwise
   */
  private boolean isLastCompactionTimeOlderThanThreshold(long compactionThresholdMs, StoreInfo storeInfo) {
    /**
     *  Reason for getting the largest version:
     *  The largest version may be larger than the current version if there is an ongoing push.
     *  The purpose of this function is to check if the last compaction time is older than the threshold.
     *  An ongoing push is regarded as the most recent compaction
     */
    Version mostRecentPushedVersion = getLargestNonFailedVersion(storeInfo);
    if (mostRecentPushedVersion == null) {
      LOGGER.warn("Store {} has never had an active version", storeInfo.getName());
      return false;
    }

    long lastCompactionTime = mostRecentPushedVersion.getCreatedTime();
    long currentTime = System.currentTimeMillis();
    long timeSinceLastCompactionMs = currentTime - lastCompactionTime;

    return timeSinceLastCompactionMs >= compactionThresholdMs;
  }

  /**
   * This function gets the most recent version that is not in ERROR or KILLED status.
   * This can be a version that is:
   * - in an ongoing push
   * - pushed but not yet online
   * - online
   * @param storeInfo
   * @return
   */
  private Version getLargestNonFailedVersion(StoreInfo storeInfo) {
    Version largestVersion = null;
    for (Version version: storeInfo.getVersions()) {
      VersionStatus versionStatus = version.getStatus();
      if (versionStatus != VersionStatus.ERROR && versionStatus != VersionStatus.KILLED) {
        if (largestVersion == null || version.getNumber() > largestVersion.getNumber()) {
          largestVersion = version;
        }
      }
    }
    return largestVersion;
  }

  /**
   * This function triggers a repush job to perform log compaction on the topic of a store.
   * <p>
   * - intermediary between {@link com.linkedin.venice.controller.VeniceHelixAdmin#repushStore} and
   * {@link RepushOrchestrator#repush} - a wrapper around repush() - handles repush job status/response
   *
   * @param repushJobRequest
   */
  public RepushJobResponse repushStore(RepushJobRequest repushJobRequest) throws Exception {
    try {
      RepushJobResponse response = repushOrchestrator.repush(repushJobRequest);

      if (response == null) {
        String nullResponseMessage = "Repush job response is null for repush request: " + repushJobRequest.toString();
        LOGGER.error(nullResponseMessage);
        throw new VeniceException(nullResponseMessage);
      }
      LOGGER.info(
          "Repush job triggered for store: {} | exec id: {} | trigger source: {}",
          response.getName(),
          response.getExecutionId(),
          repushJobRequest.getTriggerSource());
      return response;
    } catch (Exception e) {
      LOGGER.error("Failed to compact store: {}", repushJobRequest.getStoreName(), e);
      throw e;
    }
  }
}
