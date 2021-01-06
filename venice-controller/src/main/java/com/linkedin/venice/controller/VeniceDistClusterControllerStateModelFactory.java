package com.linkedin.venice.controller;

import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controller.init.ClusterLeaderInitializationRoutine;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.replication.TopicReplicator;
import io.tehuti.metrics.MetricsRepository;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.zookeeper.impl.client.ZkClient;


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
  private final VeniceHelixAdmin admin;
  private final MetricsRepository metricsRepository;
  private final ClusterLeaderInitializationRoutine controllerInitialization;
  private final Optional<TopicReplicator> onlineOfflineTopicReplicator;
  private final Optional<TopicReplicator> leaderFollowerTopicReplicator;
  private final Optional<DynamicAccessController> accessController;
  private final MetadataStoreWriter metadataStoreWriter;
  private final HelixAdminClient helixAdminClient;

  public VeniceDistClusterControllerStateModelFactory(ZkClient zkClient, HelixAdapterSerializer adapterSerializer,
      VeniceHelixAdmin admin, VeniceControllerMultiClusterConfig clusterConfigs, MetricsRepository metricsRepository,
      ClusterLeaderInitializationRoutine controllerInitialization, Optional<TopicReplicator> onlineOfflineTopicReplicator,
      Optional<TopicReplicator> leaderFollowerTopicReplicator, Optional<DynamicAccessController> accessController,
      MetadataStoreWriter metadataStoreWriter, HelixAdminClient helixAdminClient) {
    this.zkClient = zkClient;
    this.adapterSerializer = adapterSerializer;
    this.clusterConfigs = clusterConfigs;
    this.admin = admin;
    this.metricsRepository = metricsRepository;
    this.controllerInitialization = controllerInitialization;
    this.onlineOfflineTopicReplicator = onlineOfflineTopicReplicator;
    this.leaderFollowerTopicReplicator = leaderFollowerTopicReplicator;
    this.accessController = accessController;
    this.metadataStoreWriter = metadataStoreWriter;
    this.helixAdminClient = helixAdminClient;
  }

  @Override
  public VeniceDistClusterControllerStateModel createNewStateModel(String resourceName, String partitionName) {
    String veniceClusterName =
        VeniceDistClusterControllerStateModel.getVeniceClusterNameFromPartitionName(partitionName);
    VeniceDistClusterControllerStateModel model =
        new VeniceDistClusterControllerStateModel(veniceClusterName, zkClient, adapterSerializer, clusterConfigs, admin, metricsRepository, controllerInitialization, onlineOfflineTopicReplicator,
            leaderFollowerTopicReplicator, accessController, metadataStoreWriter, helixAdminClient);
    clusterToStateModelsMap.put(veniceClusterName, model);
    return model;
  }

  public VeniceDistClusterControllerStateModel getModel(String veniceClusterName) {
    return clusterToStateModelsMap.get(veniceClusterName);
  }

  public Collection<VeniceDistClusterControllerStateModel> getAllModels(){
    return clusterToStateModelsMap.values();
  }
}
