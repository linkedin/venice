package com.linkedin.venice.controller.logcompaction;

import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controller.VeniceControllerClusterConfig;
import com.linkedin.venice.controller.VeniceControllerMultiClusterConfig;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class StoreRepushCandidateFilter implements RepushCandidateFilter {
  private static final Logger LOGGER = LogManager.getLogger(StoreRepushCandidateFilter.class);

  private final Map<String, Boolean> isCompactionEnabledByCluster = new HashMap<>();
  private final Map<String, Long> versionStalenessThresholdsByCluster = new HashMap<>();

  public StoreRepushCandidateFilter(VeniceControllerMultiClusterConfig multiClusterConfig) {

    for (String clusterName: multiClusterConfig.getClusters()) {
      VeniceControllerClusterConfig config = multiClusterConfig.getControllerConfig(clusterName);
      this.isCompactionEnabledByCluster.put(clusterName, config.isLogCompactionEnabled());
      this.versionStalenessThresholdsByCluster.put(clusterName, config.getLogCompactionVersionStalenessThresholdMS());
    }
  }

  /**
   * Defined to implement RepushCandidateFilter interface
   * This function will filter out stores using cluster and store configs
   *
   * @param clusterName
   * @param storeInfo
   * @return true if store should be nominated
   * */
  public boolean apply(String clusterName, StoreInfo storeInfo) {

    // Cluster level config
    if (Boolean.FALSE.equals(this.isCompactionEnabledByCluster.get(clusterName))) {
      return false;
    }

    // Filter ineligible category of stores
    if (VeniceSystemStoreUtils.isSystemStore(storeInfo.getName()) || storeInfo.getHybridStoreConfig() == null
        || !storeInfo.isActiveActiveReplicationEnabled()) {
      return false;
    }

    // Store criteria
    if (!storeInfo.isCompactionEnabled() || storeInfo.isMigrating() || !storeInfo.isEnableStoreWrites()
        || !isLatestVersionStale(getVersionStalenessThresholdMs(storeInfo, clusterName), storeInfo)) {
      return false;
    }

    return true;
  }

  /**
   * This function checks if the last compaction time is older than the threshold.
   * @param thresholdMs, the number of milliseconds that the last compaction time should be older than
   * @param storeInfo, the store to check the last compaction time for
   * @return true if the last compaction time is older than the threshold, false otherwise
   */
  private boolean isLatestVersionStale(long thresholdMs, StoreInfo storeInfo) {
    /**
     *  Reason for getting the largest version:
     *  The largest version may be larger than the current version if there is an ongoing push.
     *  The purpose of this function is to check if the last compaction time is older than the threshold.
     *  An ongoing push is regarded as the most recent compaction
     */
    Version mostRecentPushedVersion = getLargestNonFailedVersion(storeInfo);
    if (mostRecentPushedVersion == null) {
      LOGGER.warn("Store {} has never had an active version, skipping", storeInfo.getName());
      return false;
    }

    long lastCompactionTime = mostRecentPushedVersion.getCreatedTime();
    long currentTime = System.currentTimeMillis();
    long timeSinceLastCompactionMs = currentTime - lastCompactionTime;

    return timeSinceLastCompactionMs >= thresholdMs;
  }

  /**
   * This function gets the most recent version that is not in ERROR or KILLED status.
   * This can be a version that is:
   * - in an ongoing push
   * - pushed but not yet online
   * - online
   * @param storeInfo
   * @return largestVersion
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
   * This function will get the version staleness threshold from the store config or cluster config
   * If default `-1`, use cluster config
   *
   * @param storeInfo
   * @param clusterName
   * @return compactionThresholdMs
   * */
  private long getVersionStalenessThresholdMs(StoreInfo storeInfo, String clusterName) {
    return storeInfo.getCompactionThreshold() > -1
        ? storeInfo.getCompactionThreshold()
        : this.versionStalenessThresholdsByCluster.get(clusterName);
  }
}
