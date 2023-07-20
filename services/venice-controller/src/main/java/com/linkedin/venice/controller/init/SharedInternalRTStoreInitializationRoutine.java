package com.linkedin.venice.controller.init;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceControllerMultiClusterConfig;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.commons.lang.StringUtils;


public class SharedInternalRTStoreInitializationRoutine implements ClusterLeaderInitializationRoutine {
  private final String storeCluster;
  private final AvroProtocolDefinition protocolDefinition;
  private final VeniceControllerMultiClusterConfig multiClusterConfigs;
  private final Admin admin;
  private final Schema keySchema;
  private final UpdateStoreQueryParams updateStoreQueryParams;
  private final String storeName;

  public SharedInternalRTStoreInitializationRoutine(
      String storeCluster,
      String systemStoreName,
      AvroProtocolDefinition protocolDefinition,
      VeniceControllerMultiClusterConfig multiClusterConfigs,
      Admin admin,
      Schema keySchema,
      UpdateStoreQueryParams updateStoreQueryParams) {
    this.storeCluster = storeCluster;
    this.storeName = systemStoreName;
    this.protocolDefinition = protocolDefinition;
    this.multiClusterConfigs = multiClusterConfigs;
    this.admin = admin;
    this.keySchema = keySchema;

    if (updateStoreQueryParams == null) {
      this.updateStoreQueryParams = new UpdateStoreQueryParams();
    } else {
      this.updateStoreQueryParams = updateStoreQueryParams;
    }

    if (!this.updateStoreQueryParams.getHybridOffsetLagThreshold().isPresent()) {
      this.updateStoreQueryParams.setHybridOffsetLagThreshold(100L);
    }

    if (!this.updateStoreQueryParams.getHybridRewindSeconds().isPresent()) {
      this.updateStoreQueryParams.setHybridRewindSeconds(TimeUnit.DAYS.toSeconds(7));
    }

    if (!StringUtils.isEmpty(storeCluster) && !this.updateStoreQueryParams.getPartitionCount().isPresent()) {
      this.updateStoreQueryParams
          .setPartitionCount(multiClusterConfigs.getControllerConfig(storeCluster).getMinNumberOfPartitions());
    }
  }

  /**
   * @see ClusterLeaderInitializationRoutine#execute(String)
   */
  @Override
  public void execute(String clusterName) {
    if (storeCluster.equals(clusterName)) {
      SystemStoreInitializationHelper.setupSystemStore(
          clusterName,
          storeName,
          protocolDefinition,
          keySchema,
          store -> !store.isHybrid(),
          updateStoreQueryParams,
          admin,
          multiClusterConfigs);
    }
  }
}
