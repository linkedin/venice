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
    /*
     * Stores initialized by this routine are created independently by each region's child
     * controller, which writes SOP, EOP, and TopicSwitch directly to the local version topic.
     * The native replication source must therefore point to the local region so that the leader
     * replica does NOT try to consume the version topic from a remote fabric.
     *
     * If the source were remote, the leader would consume the remote VT and produce those
     * records into the local VT — the same VT that the local controller already wrote to.
     * After the leader finishes consuming the remote VT (past its EOP), it switches back to
     * the local VT at an offset beyond the controller's TopicSwitch message. Because the
     * leader skips all remote-VT records after EOP (including the remote TopicSwitch), and
     * its local-VT subscription starts past the controller's TopicSwitch, the leader never
     * sees any TopicSwitch at all. Without a TopicSwitch the leader never transitions to the
     * real-time topic, and the push remains stuck at END_OF_PUSH_RECEIVED indefinitely.
     *
     * Setting the source to {@code multiClusterConfigs.getRegionName()} (which always returns
     * the local region name) ensures the leader consumes the local VT directly.
     */
    UpdateStoreQueryParams updateStoreQueryParams = new UpdateStoreQueryParams().setHybridOffsetLagThreshold(100L)
        .setHybridRewindSeconds(TimeUnit.DAYS.toSeconds(7))
        .setNativeReplicationSourceFabric(multiClusterConfigs.getRegionName());
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
