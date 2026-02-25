package com.linkedin.venice.controller.logcompaction;

import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controller.VeniceControllerClusterConfig;
import com.linkedin.venice.controller.VeniceControllerMultiClusterConfig;
import com.linkedin.venice.meta.StoreInfo;
import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Prerequisite filter that checks cluster-level and store-level eligibility for log compaction.
 * This filter enforces:
 * <ul>
 *   <li>Cluster-level compaction is enabled</li>
 *   <li>Store is not a system store</li>
 *   <li>Store has hybrid config and active-active replication enabled</li>
 *   <li>Store-level compaction is enabled, store is not migrating, and writes are enabled</li>
 * </ul>
 */
public class StoreRepushCandidateFilter implements RepushCandidateFilter {
  private static final Logger LOGGER = LogManager.getLogger(StoreRepushCandidateFilter.class);

  private final Map<String, Boolean> isCompactionEnabledByCluster = new HashMap<>();

  public StoreRepushCandidateFilter(VeniceControllerMultiClusterConfig multiClusterConfig) {

    for (String clusterName: multiClusterConfig.getClusters()) {
      VeniceControllerClusterConfig config = multiClusterConfig.getControllerConfig(clusterName);
      this.isCompactionEnabledByCluster.put(clusterName, config.isLogCompactionEnabled());
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
    if (!storeInfo.isCompactionEnabled() || storeInfo.isMigrating() || !storeInfo.isEnableStoreWrites()) {
      return false;
    }

    return true;
  }
}
