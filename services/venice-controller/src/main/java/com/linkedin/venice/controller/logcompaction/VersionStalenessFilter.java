package com.linkedin.venice.controller.logcompaction;

import com.linkedin.venice.controller.VeniceControllerClusterConfig;
import com.linkedin.venice.controller.VeniceControllerMultiClusterConfig;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Trigger that checks whether a store's latest version is stale enough to warrant compaction.
 * As a {@link RepushCandidateTrigger}, this participates in OR logic with other triggers — if any
 * trigger passes, the store is scheduled for compaction (provided all prerequisite filters also pass).
 */
public class VersionStalenessFilter implements RepushCandidateTrigger {
  private static final Logger LOGGER = LogManager.getLogger(VersionStalenessFilter.class);

  private final Map<String, Long> versionStalenessThresholdsByCluster = new HashMap<>();

  public VersionStalenessFilter(VeniceControllerMultiClusterConfig multiClusterConfig) {
    for (String clusterName: multiClusterConfig.getClusters()) {
      VeniceControllerClusterConfig config = multiClusterConfig.getControllerConfig(clusterName);
      this.versionStalenessThresholdsByCluster.put(clusterName, config.getLogCompactionVersionStalenessThresholdMS());
    }
  }

  @Override
  public boolean apply(String clusterName, StoreInfo storeInfo) {
    return isLatestVersionStale(getVersionStalenessThresholdMs(storeInfo, clusterName), storeInfo);
  }

  /**
   * Checks if the last compaction time is older than the threshold.
   * @param thresholdMs the number of milliseconds that the last compaction time should be older than
   * @param storeInfo the store to check the last compaction time for
   * @return true if the last compaction time is older than the threshold, false otherwise
   */
  private boolean isLatestVersionStale(long thresholdMs, StoreInfo storeInfo) {
    /**
     *  Reason for getting the largest version:
     *  The largest version may be larger than the current version if there is an ongoing push.
     *  The purpose of this function is to check if the last compaction time is older than the threshold.
     *  An ongoing push is regarded as the most recent compaction.
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
   * Gets the most recent version that is not in ERROR or KILLED status.
   * This can be a version that is:
   * - in an ongoing push
   * - pushed but not yet online
   * - online
   * @param storeInfo the store to check versions for
   * @return the largest non-failed version, or null if none exists
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
   * Gets the version staleness threshold from the store config or cluster config.
   * If the store-level threshold is default ({@code -1}), uses the cluster config.
   *
   * @param storeInfo the store to check the threshold for
   * @param clusterName the cluster to get the default threshold from
   * @return compactionThresholdMs
   */
  private long getVersionStalenessThresholdMs(StoreInfo storeInfo, String clusterName) {
    return storeInfo.getCompactionThreshold() > -1
        ? storeInfo.getCompactionThreshold()
        : this.versionStalenessThresholdsByCluster.get(clusterName);
  }
}
