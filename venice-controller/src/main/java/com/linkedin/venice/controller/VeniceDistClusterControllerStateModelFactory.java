package com.linkedin.venice.controller;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.LeaderStandbySMD;
import org.apache.helix.participant.statemachine.StateModelFactory;


/**
 * Factory to create VeniceDistClusterControllerStateModel and provide some utility methods to get state model by given
 * cluster.
 */
public class VeniceDistClusterControllerStateModelFactory extends StateModelFactory<VeniceDistClusterControllerStateModel> {
  private final ZkClient zkClient;
  private final ConcurrentMap<String, VeniceControllerClusterConfig> clusterToConfigsMap;
  private final ConcurrentMap<String, VeniceDistClusterControllerStateModel> clusterToStateModelsMap =
      new ConcurrentHashMap<>();

  public VeniceDistClusterControllerStateModelFactory(ZkClient zkClient) {
    this.zkClient = zkClient;
    this.clusterToConfigsMap = new ConcurrentHashMap<>();
  }

  @Override
  public VeniceDistClusterControllerStateModel createNewStateModel(String resourceName, String partitionName) {
    String veniceClusterName =
        VeniceDistClusterControllerStateModel.getVeniceClusterNameFromPartitionName(partitionName);

    VeniceDistClusterControllerStateModel model =
        new VeniceDistClusterControllerStateModel(zkClient, clusterToConfigsMap);
    clusterToStateModelsMap.put(veniceClusterName, model);
    return model;
  }

  public void addClusterConfig(String veniceClusterName, VeniceControllerClusterConfig config) {
    clusterToConfigsMap.put(veniceClusterName, config);
  }

  public VeniceControllerClusterConfig getClusterConfig(String veniceClusterName) {
    return clusterToConfigsMap.get(veniceClusterName);
  }

  public void deleteClusterConfig(String veniceClusterName) {
    clusterToConfigsMap.remove(veniceClusterName);
  }

  public VeniceDistClusterControllerStateModel getModel(String veniceClusterName) {
    return clusterToStateModelsMap.get(veniceClusterName);
  }

  /**
   * After start a new venice cluster, wait until state model become STANDBY or LEADER or ERROR.
   * @param veniceClusterName
   * @throws InterruptedException
   */
  public void waitUntilClusterStarted(String veniceClusterName)
      throws InterruptedException {
    //TODO Add time out or use better implementation like lock condition here.
    while (clusterToStateModelsMap.get(veniceClusterName) == null || clusterToStateModelsMap.get(veniceClusterName)
        .getCurrentState().equals(LeaderStandbySMD.States.OFFLINE.toString())) {
      Thread.sleep(300l);
    }
  }
}
