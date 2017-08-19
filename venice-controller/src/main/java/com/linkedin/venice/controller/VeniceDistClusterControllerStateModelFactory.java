package com.linkedin.venice.controller;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.meta.StoreCleaner;
import io.tehuti.metrics.MetricsRepository;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.participant.statemachine.StateModelFactory;


/**
 * Factory to create VeniceDistClusterControllerStateModel and provide some utility methods to get state model by given
 * cluster.
 */
public class VeniceDistClusterControllerStateModelFactory extends StateModelFactory<VeniceDistClusterControllerStateModel> {
  private final ZkClient zkClient;
  private final HelixAdapterSerializer adapterSerializer;
  private final ConcurrentMap<String, VeniceControllerClusterConfig> clusterToConfigsMap;
  private final ConcurrentMap<String, VeniceDistClusterControllerStateModel> clusterToStateModelsMap =
      new ConcurrentHashMap<>();
  private final StoreCleaner storeCleaner;
  private final ConcurrentMap<String, MetricsRepository> metricsRepositories;

  public VeniceDistClusterControllerStateModelFactory(ZkClient zkClient, HelixAdapterSerializer adapterSerializer,
      StoreCleaner storeCleaner) {
    this.zkClient = zkClient;
    this.adapterSerializer = adapterSerializer;
    this.clusterToConfigsMap = new ConcurrentHashMap<>();
    this.storeCleaner = storeCleaner;
    this.metricsRepositories = new ConcurrentHashMap<>();
  }

  @Override
  public VeniceDistClusterControllerStateModel createNewStateModel(String resourceName, String partitionName) {
    String veniceClusterName =
        VeniceDistClusterControllerStateModel.getVeniceClusterNameFromPartitionName(partitionName);

    VeniceDistClusterControllerStateModel model =
        new VeniceDistClusterControllerStateModel(zkClient, adapterSerializer, clusterToConfigsMap, storeCleaner,
            metricsRepositories);
    clusterToStateModelsMap.put(veniceClusterName, model);
    return model;
  }

  public void addClusterConfig(String veniceClusterName, VeniceControllerClusterConfig config, MetricsRepository metricsRepository) {
    clusterToConfigsMap.put(veniceClusterName, config);
    metricsRepositories.put(veniceClusterName, metricsRepository);
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

  public Collection<VeniceDistClusterControllerStateModel> getAllModels(){
    return clusterToStateModelsMap.values();
  }

  /**
   * After start a new venice cluster, judge whether the controller has joined the cluster. After state model becoming
   * STANDBY or LEADER or ERROR, the controller has joined cluster.
   */
  protected boolean hasJoinedCluster(String veniceClusterName) {
    if (clusterToStateModelsMap.get(veniceClusterName) == null || clusterToStateModelsMap.get(veniceClusterName)
        .getCurrentState()
        .equals(HelixState.OFFLINE_STATE)) {
      return false;
    }
    if (clusterToStateModelsMap.get(veniceClusterName).getCurrentState().equals(HelixState.ERROR_STATE)) {
      throw new VeniceException("Controller for " + veniceClusterName
          + " is not started, because we met error when doing Helix state transition.");
    }
    return true;
  }
}
