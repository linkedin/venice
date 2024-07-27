package com.linkedin.venice.controller.util;

import com.linkedin.venice.ConfigConstants;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceControllerClusterConfig;
import com.linkedin.venice.controller.kafka.protocol.admin.HybridStoreConfigRecord;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.Store;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class AdminUtils {
  private static final Logger LOGGER = LogManager.getLogger(AdminUtils.class);

  private AdminUtils() {
  }

  public static boolean isHybrid(HybridStoreConfigRecord hybridStoreConfigRecord) {
    HybridStoreConfig hybridStoreConfig = null;
    if (hybridStoreConfigRecord != null) {
      CharSequence realTimeTopicNameTemp = hybridStoreConfigRecord.getRealTimeTopicName();
      final String realTimeTopicName;
      if (realTimeTopicNameTemp == null) {
        realTimeTopicName = HybridStoreConfigImpl.DEFAULT_REAL_TIME_TOPIC_NAME;
      } else {
        realTimeTopicName = realTimeTopicNameTemp.toString();
      }
      hybridStoreConfig = new HybridStoreConfigImpl(
          hybridStoreConfigRecord.rewindTimeInSeconds,
          hybridStoreConfigRecord.offsetLagThresholdToGoOnline,
          hybridStoreConfigRecord.producerTimestampLagThresholdToGoOnlineInSeconds,
          DataReplicationPolicy.valueOf(hybridStoreConfigRecord.dataReplicationPolicy),
          BufferReplayPolicy.valueOf(hybridStoreConfigRecord.bufferReplayPolicy),
          realTimeTopicName);
    }
    return isHybrid(hybridStoreConfig);
  }

  /**
   * A store is not hybrid in the following two scenarios:
   * If hybridStoreConfig is null, it means store is not hybrid.
   * If all the hybrid config values are negative, it indicates that the store is being set back to batch-only store.
   */
  public static boolean isHybrid(HybridStoreConfig hybridStoreConfig) {
    return hybridStoreConfig != null && hybridStoreConfig.isHybrid();
  }

  public static int getRmdVersionID(Admin admin, String storeName, String clusterName) {
    final Store store = admin.getStore(clusterName, storeName);
    if (store == null) {
      LOGGER.warn(
          "No store found in the store repository. Will get store-level RMD version ID from cluster config. "
              + "Store name: {}, cluster: {}",
          storeName,
          clusterName);
    } else if (store.getRmdVersion() == ConfigConstants.UNSPECIFIED_REPLICATION_METADATA_VERSION) {
      LOGGER.info("No store-level RMD version ID found for store {} in cluster {}", storeName, clusterName);
    } else {
      LOGGER.info(
          "Found store-level RMD version ID {} for store {} in cluster {}",
          store.getRmdVersion(),
          storeName,
          clusterName);
      return store.getRmdVersion();
    }

    final VeniceControllerClusterConfig controllerClusterConfig =
        admin.getMultiClusterConfigs().getControllerConfig(clusterName);
    if (controllerClusterConfig == null) {
      throw new VeniceException("No controller cluster config found for cluster " + clusterName);
    }
    final int rmdVersionID = controllerClusterConfig.getReplicationMetadataVersion();
    LOGGER.info("Use RMD version ID {} for cluster {}", rmdVersionID, clusterName);
    return rmdVersionID;
  }

  /**
   * Check if a store can support incremental pushes based on other configs. The following rules define when incremental
   * push is allowed:
   * <ol>
   *   <li>If the system is running in single-region mode, the store must by hybrid</li>
   *   <li>If the system is running in multi-region mode,</li>
   *   <ol type="i">
   *     <li>Hybrid + Active-Active</li>
   *     <li>Hybrid + !Active-Active + {@link DataReplicationPolicy} is {@link DataReplicationPolicy#AGGREGATE}</li>
   *     <li>Hybrid + !Active-Active + {@link DataReplicationPolicy} is {@link DataReplicationPolicy#NONE}</li>
   *   </ol>
   * <ol/>
   * @param multiRegion whether the system is running in multi-region mode
   * @param hybridStoreConfig The hybrid store config after applying all updates
   * @return {@code true} if incremental push is allowed, {@code false} otherwise
   */
  public static boolean isIncrementalPushSupported(
      boolean multiRegion,
      boolean activeActiveReplicationEnabled,
      HybridStoreConfig hybridStoreConfig) {
    // Only hybrid stores can support incremental push
    if (!AdminUtils.isHybrid(hybridStoreConfig)) {
      return false;
    }

    // If the system is running in multi-region mode, we need to validate the data replication policies
    if (!multiRegion) {
      return true;
    }

    // A/A can always support incremental push
    if (activeActiveReplicationEnabled) {
      return true;
    }

    DataReplicationPolicy dataReplicationPolicy = hybridStoreConfig.getDataReplicationPolicy();
    return dataReplicationPolicy == DataReplicationPolicy.AGGREGATE
        || dataReplicationPolicy == DataReplicationPolicy.NONE;
  }
}
