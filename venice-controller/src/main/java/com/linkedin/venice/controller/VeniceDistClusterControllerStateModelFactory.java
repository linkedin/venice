package com.linkedin.venice.controller;

import com.linkedin.venice.controller.init.ClusterLeaderInitializationRoutine;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.meta.StoreCleaner;
import com.linkedin.venice.replication.TopicReplicator;
import io.tehuti.metrics.MetricsRepository;
import java.util.Collection;
import java.util.Optional;
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
  private final VeniceControllerMultiClusterConfig clusterConfigs;
  private final ConcurrentMap<String, VeniceDistClusterControllerStateModel> clusterToStateModelsMap =
      new ConcurrentHashMap<>();
  private final StoreCleaner storeCleaner;
  private final MetricsRepository metricsRepository;
  private final ClusterLeaderInitializationRoutine controllerInitialization;
  private final Optional<TopicReplicator> onlineOfflineTopicReplicator;
  private final Optional<TopicReplicator> leaderFollowerTopicReplicator;

  public VeniceDistClusterControllerStateModelFactory(ZkClient zkClient, HelixAdapterSerializer adapterSerializer,
      StoreCleaner storeCleaner, VeniceControllerMultiClusterConfig clusterConfigs, MetricsRepository metricsRepository,
      ClusterLeaderInitializationRoutine controllerInitialization, Optional<TopicReplicator> onlineOfflineTopicReplicator,
      Optional<TopicReplicator> leaderFollowerTopicReplicator) {
    this.zkClient = zkClient;
    this.adapterSerializer = adapterSerializer;
    this.clusterConfigs = clusterConfigs;
    this.storeCleaner = storeCleaner;
    this.metricsRepository = metricsRepository;
    this.controllerInitialization = controllerInitialization;
    this.onlineOfflineTopicReplicator = onlineOfflineTopicReplicator;
    this.leaderFollowerTopicReplicator = leaderFollowerTopicReplicator;
  }

  @Override
  public VeniceDistClusterControllerStateModel createNewStateModel(String resourceName, String partitionName) {
    String veniceClusterName =
        VeniceDistClusterControllerStateModel.getVeniceClusterNameFromPartitionName(partitionName);

    VeniceControllerClusterConfig clusterConfig = clusterConfigs.getConfigForCluster(veniceClusterName);

    if (clusterConfig == null) {
      throw new VeniceException("No configuration exists for " + veniceClusterName);
    }

    VeniceDistClusterControllerStateModel model =
        new VeniceDistClusterControllerStateModel(zkClient, adapterSerializer, clusterConfig, storeCleaner,
            metricsRepository, controllerInitialization, onlineOfflineTopicReplicator, leaderFollowerTopicReplicator);
    clusterToStateModelsMap.put(veniceClusterName, model);
    return model;
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
