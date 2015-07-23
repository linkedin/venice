package com.linkedin.venice.controller;

import com.linkedin.venice.config.VeniceStorePartitionInformation;
import com.linkedin.venice.service.AbstractVeniceService;
import java.util.Map;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixConnection;
import org.apache.helix.HelixController;
import org.apache.helix.api.accessor.ClusterAccessor;
import org.apache.helix.api.config.ClusterConfig;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ControllerId;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZkHelixConnection;
import org.apache.helix.model.IdealState;
import org.apache.log4j.Logger;

/**
 * Helix Notes:
 *  # Please do not use Helix Manager. It is flaky/deprecated in Helix 0.7.* . Please refer to the following links for
 *      more details: http://mail-archives.apache.org/mod_mbox/helix-user/201507.mbox/%3cCABaj-QZA5H=coza5gLwahdnGouM_OpNy+WYwfbaGOj=BFTtv1g@mail.gmail.com%3e
 *  # Resources can be add using the ClusterAccessor APIs but requires advance knowledge. Using the HelixAdmin is
 *      easier and is a wrapper around the ClusterAccessor.
 */

/**
 * A service venice controller. Wraps Helix Controller.
 */
public class VeniceControllerService extends AbstractVeniceService {

  private static final Logger logger = Logger.getLogger(VeniceControllerService.class.getName());
  private static final String VENICE_CONTROLLER_SERVICE_NAME = "venice-controller-service";

  private final String zkAddress;
  private final String clusterName;
  private final String controllerName;

  private final HelixConnection connection;
  private final ClusterId clusterId;

  private ClusterAccessor clusterAccessor;
  private HelixController controller;

  private final Map<String, VeniceStorePartitionInformation> storeToPartitionInformationMap;

  public VeniceControllerService (String zkAddress, String clusterName, String controllerName,
      Map<String, VeniceStorePartitionInformation> storeToPartitionInformationMap) {
    super(VENICE_CONTROLLER_SERVICE_NAME);

    this.zkAddress = zkAddress;
    this.clusterName = clusterName;
    this.controllerName = controllerName;
    this.storeToPartitionInformationMap = storeToPartitionInformationMap;

    connection = new ZkHelixConnection(zkAddress);
    clusterId = ClusterId.from(clusterName);
  }

  @Override
  public void startInner() {
    connection.connect();
    clusterAccessor = connection.createClusterAccessor(clusterId);
    controller = connection.createController(clusterId, ControllerId.from(controllerName));
    createHelixClusterIfNeeded();
    controller.start();
    addStoresAsHelixResources();
  }

  @Override
  public void stopInner() {
    if(controller != null && controller.isStarted()){
      controller.stop();
    }
    if(connection != null && connection.isConnected()) {
      connection.disconnect();
    }
  }

  /**
   * Adds Venice stores as a Helix Resource.
   * TODO: Optimize this and do this only for non existing stores.
   */
  private void addStoresAsHelixResources() {
    HelixAdmin admin = new ZKHelixAdmin(zkAddress);

    for(VeniceStorePartitionInformation storePartitionInformation: storeToPartitionInformationMap.values()) {
      String resourceName = storePartitionInformation.getStoreName();
      if(!admin.getResourcesInCluster(clusterName).contains(resourceName)) {
        admin.addResource(clusterName, resourceName, storePartitionInformation.getPartitionsCount(),
            VenicePartitionOnlineOfflineStateModelGenerator.PARTITION_ONLINE_OFFLINE_STATE_MODEL,
            IdealState.RebalanceMode.FULL_AUTO.toString());
        admin.rebalance(clusterName, resourceName, storePartitionInformation.getReplicationFactor());
        logger.info("Added " + storePartitionInformation.getStoreName() + " as a resource to cluster: " + clusterName);
      } else {
        logger.info("Already exists " + storePartitionInformation.getStoreName()
            + "as a resource in cluster: " + clusterName);
      }
    }
  }

  /**
   * Creates a Helix cluster, if required.
   */
  private void createHelixClusterIfNeeded() {
    ClusterConfig clusterConfig = new ClusterConfig.Builder(clusterId).autoJoin(true).build();
    if(!clusterAccessor.createCluster(clusterConfig)) {
      logger.info("Cluster: " + clusterId.stringify() + "already exists.");
    } else {
      logger.info("Adding state model definition named : "
          + VenicePartitionOnlineOfflineStateModelGenerator.PARTITION_ONLINE_OFFLINE_STATE_MODEL
          + " generated using: " + VenicePartitionOnlineOfflineStateModelGenerator.class.toString());

      clusterAccessor.addStateModelDefinition(
          VenicePartitionOnlineOfflineStateModelGenerator.generatePartitionStateModelDefinition());
    }
  }
}
