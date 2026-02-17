package com.linkedin.venice.controller;

import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controller.init.ClusterLeaderInitializationRoutine;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.ingestion.control.RealTimeTopicSwitcher;
import io.tehuti.metrics.MetricsRepository;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.zookeeper.impl.client.ZkClient;


/**
 * Factory to create VeniceDistClusterControllerStateModel and provide some utility methods to get state model by given
 * cluster.
 */
public class VeniceDistClusterControllerStateModelFactory extends StateModelFactory<VeniceControllerStateModel> {
  private final ZkClient zkClient;
  private final HelixAdapterSerializer adapterSerializer;
  private final VeniceControllerMultiClusterConfig clusterConfigs;
  private final ConcurrentMap<String, VeniceControllerStateModel> clusterToStateModelsMap = new ConcurrentHashMap<>();
  private final VeniceHelixAdmin admin;
  private final MetricsRepository metricsRepository;
  private final ClusterLeaderInitializationRoutine controllerInitialization;
  private final RealTimeTopicSwitcher realTimeTopicSwitcher;
  private final Optional<DynamicAccessController> accessController;
  private final HelixAdminClient helixAdminClient;
  private final Optional<List<VeniceVersionLifecycleEventListener>> versionLifecycleEventListeners;

  public VeniceDistClusterControllerStateModelFactory(
      ZkClient zkClient,
      HelixAdapterSerializer adapterSerializer,
      VeniceHelixAdmin admin,
      VeniceControllerMultiClusterConfig clusterConfigs,
      MetricsRepository metricsRepository,
      ClusterLeaderInitializationRoutine controllerInitialization,
      RealTimeTopicSwitcher realTimeTopicSwitcher,
      Optional<DynamicAccessController> accessController,
      HelixAdminClient helixAdminClient,
      Optional<List<VeniceVersionLifecycleEventListener>> versionLifecycleEventListeners) {
    this.zkClient = zkClient;
    this.adapterSerializer = adapterSerializer;
    this.clusterConfigs = clusterConfigs;
    this.admin = admin;
    this.metricsRepository = metricsRepository;
    this.controllerInitialization = controllerInitialization;
    this.realTimeTopicSwitcher = realTimeTopicSwitcher;
    this.accessController = accessController;
    this.helixAdminClient = helixAdminClient;
    this.versionLifecycleEventListeners = versionLifecycleEventListeners;
  }

  /**
   * @see StateModelFactory#createNewStateModel(String, String) createNewStateModel
   */
  @Override
  public VeniceControllerStateModel createNewStateModel(String resourceName, String partitionName) {
    String veniceClusterName = VeniceControllerStateModel.getVeniceClusterNameFromPartitionName(partitionName);
    VeniceControllerStateModel model = new VeniceControllerStateModel(
        veniceClusterName,
        zkClient,
        adapterSerializer,
        clusterConfigs,
        admin,
        metricsRepository,
        controllerInitialization,
        realTimeTopicSwitcher,
        accessController,
        helixAdminClient,
        versionLifecycleEventListeners);
    clusterToStateModelsMap.put(veniceClusterName, model);
    return model;
  }

  /**
   * @return {@code VeniceControllerStateModel} for the input cluster, or
   *         {@code null} if the input cluster's model is not created by the factory.
   */
  public VeniceControllerStateModel getModel(String veniceClusterName) {
    return clusterToStateModelsMap.get(veniceClusterName);
  }

  /**
   * @return all {@code VeniceControllerStateModel} created by the factory.
   */
  public Collection<VeniceControllerStateModel> getAllModels() {
    return clusterToStateModelsMap.values();
  }

  /**
   * Close all {@code VeniceControllerStateModel} created by the factory.
   */
  public void close() {
    for (VeniceControllerStateModel model: clusterToStateModelsMap.values()) {
      model.close();
    }
  }
}
