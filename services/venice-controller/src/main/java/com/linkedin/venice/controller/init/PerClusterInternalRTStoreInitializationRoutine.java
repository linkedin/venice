package com.linkedin.venice.controller.init;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceControllerMultiClusterConfig;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.avro.Schema;


public class PerClusterInternalRTStoreInitializationRoutine implements ClusterLeaderInitializationRoutine {
  private final Function<String, String> clusterToStoreNameSupplier;
  private final VeniceControllerMultiClusterConfig multiClusterConfigs;
  private final Admin admin;
  private final Schema keySchema;
  private final AvroProtocolDefinition protocolDefinition;

  public PerClusterInternalRTStoreInitializationRoutine(
      AvroProtocolDefinition protocolDefinition,
      Function<String, String> clusterToStoreNameSupplier,
      VeniceControllerMultiClusterConfig multiClusterConfigs,
      Admin admin,
      Schema keySchema) {
    this.protocolDefinition = protocolDefinition;
    this.clusterToStoreNameSupplier = clusterToStoreNameSupplier;
    this.multiClusterConfigs = multiClusterConfigs;
    this.admin = admin;
    this.keySchema = keySchema;
  }

  /**
   * @see ClusterLeaderInitializationRoutine#execute(String)
   */
  @Override
  public void execute(String clusterName) {
    String storeName = clusterToStoreNameSupplier.apply(clusterName);
    UpdateStoreQueryParams updateStoreQueryParams = new UpdateStoreQueryParams().setHybridOffsetLagThreshold(100L)
        .setHybridRewindSeconds(TimeUnit.DAYS.toSeconds(7));
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
