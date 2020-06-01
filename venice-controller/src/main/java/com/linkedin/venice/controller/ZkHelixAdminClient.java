package com.linkedin.venice.controller;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.ZkClientFactory;
import com.linkedin.venice.stats.ZkClientStatusStats;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.helix.controller.rebalancer.DelayedAutoRebalancer;
import org.apache.helix.controller.rebalancer.strategy.AutoRebalanceStrategy;
import org.apache.helix.controller.rebalancer.strategy.CrushRebalanceStrategy;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.log4j.Logger;
import org.apache.helix.HelixAdmin;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.LeaderStandbySMD;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.zookeeper.impl.client.ZkClient;


/**
 * The purpose of this class is to abstract Helix operations out of the {@link VeniceHelixAdmin} and eventually rename
 * it to VeniceAdmin. This is one implementation of the {@link HelixAdminClient} interface which uses Zk based Helix
 * API. In the future we might move to the Helix REST API.
 */
public class ZkHelixAdminClient implements HelixAdminClient {
  private static final Logger LOGGER = Logger.getLogger(ZkHelixAdminClient.class);
  private static final int CONTROLLER_CLUSTER_PARTITION_COUNT = 1;
  private static final String CONTROLLER_HAAS_ZK_CLIENT_NAME = "controller-zk-client-for-haas-admin";

  private final HelixAdmin helixAdmin;
  private final VeniceControllerMultiClusterConfig multiClusterConfigs;
  private final String haasSuperClusterName;
  private final int controllerClusterReplicaCount;

  public ZkHelixAdminClient(VeniceControllerMultiClusterConfig multiClusterConfigs, MetricsRepository metricsRepository) {
    this.multiClusterConfigs = multiClusterConfigs;
    haasSuperClusterName = multiClusterConfigs.getControllerHAASSuperClusterName();
    controllerClusterReplicaCount = multiClusterConfigs.getControllerClusterReplica();
    ZkClient helixAdminZkClient = ZkClientFactory.newZkClient(multiClusterConfigs.getZkAddress());
    helixAdminZkClient.subscribeStateChanges(new ZkClientStatusStats(metricsRepository, CONTROLLER_HAAS_ZK_CLIENT_NAME));
    helixAdminZkClient.setZkSerializer(new ZNRecordSerializer());
    if (!helixAdminZkClient.waitUntilConnected(ZkClient.DEFAULT_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS)) {
      throw new VeniceException("Failed to connect to ZK within " + ZkClient.DEFAULT_CONNECTION_TIMEOUT + " ms!");
    }
    helixAdmin = new ZKHelixAdmin(helixAdminZkClient);
  }

  @Override
  public boolean isVeniceControllerClusterCreated(String controllerClusterName) {
    return helixAdmin.getClusters().contains(controllerClusterName);
  }

  @Override
  public boolean isVeniceStorageClusterCreated(String clusterName) {
     return helixAdmin.getClusters().contains(clusterName);
  }

  @Override
  public void createVeniceControllerCluster(String controllerClusterName) {
    if (!helixAdmin.addCluster(controllerClusterName, false)) {
      throw new VeniceException("Failed to create Helix cluster: " + controllerClusterName
          + ". HelixAdmin#addCluster returned false");
    }
    Map<String, String> helixClusterProperties = new HashMap<>();
    helixClusterProperties.put(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, String.valueOf(true));
    // Topology and fault zone type fields are used by CRUSH alg. Helix would apply the constrains on CRUSH alg to choose proper instance to hold the replica.
    helixClusterProperties.put(ClusterConfig.ClusterConfigProperty.TOPOLOGY_AWARE_ENABLED.name(), String.valueOf(false));

    updateClusterConfigs(controllerClusterName, helixClusterProperties);
    helixAdmin.addStateModelDef(controllerClusterName, LeaderStandbySMD.name, LeaderStandbySMD.build());

    if (!helixAdmin.getResourcesInCluster(haasSuperClusterName).contains(controllerClusterName)) {
      addClusterToGrandCluster(controllerClusterName);
    }
  }

  @Override
  public void createVeniceStorageCluster(String clusterName, String controllerClusterName,
      Map<String, String> helixClusterProperties) {
    if (!helixAdmin.addCluster(clusterName, false)) {
      throw new VeniceException("Failed to create Helix cluster: " + clusterName
          + ". HelixAdmin#addCluster returned false");
    }
    updateClusterConfigs(clusterName, helixClusterProperties);
    helixAdmin.addStateModelDef(clusterName, VeniceStateModel.PARTITION_ONLINE_OFFLINE_STATE_MODEL,
        VeniceStateModel.getDefinition());
    helixAdmin.addStateModelDef(clusterName, LeaderStandbySMD.name, LeaderStandbySMD.build());
    helixAdmin.addResource(controllerClusterName, clusterName, CONTROLLER_CLUSTER_PARTITION_COUNT, LeaderStandbySMD.name,
        IdealState.RebalanceMode.FULL_AUTO.toString(), AutoRebalanceStrategy.class.getName());
    IdealState idealState = helixAdmin.getResourceIdealState(controllerClusterName, clusterName);
    idealState.setMinActiveReplicas(controllerClusterReplicaCount);
    idealState.setRebalancerClassName(DelayedAutoRebalancer.class.getName());
    idealState.setRebalanceStrategy(CrushRebalanceStrategy.class.getName());
    helixAdmin.setResourceIdealState(controllerClusterName, clusterName, idealState);
    helixAdmin.rebalance(controllerClusterName, clusterName, controllerClusterReplicaCount);
    addClusterToGrandCluster(clusterName);
  }

  @Override
  public boolean isClusterInGrandCluster(String clusterName) {
    return helixAdmin.getResourcesInCluster(haasSuperClusterName).contains(clusterName);
  }

  @Override
  public void addClusterToGrandCluster(String clusterName) {
    helixAdmin.addClusterToGrandCluster(clusterName, haasSuperClusterName);
  }

  @Override
  public void updateClusterConfigs(String clusterName, Map<String, String> helixClusterProperties) {
    HelixConfigScope configScope = new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).
        forCluster(clusterName).build();
    helixAdmin.setConfig(configScope, helixClusterProperties);
  }

  @Override
  public void enablePartition(boolean enabled, String clusterName, String instanceName,
      String resourceName, List<String> partitionNames) {
    helixAdmin.enablePartition(enabled, clusterName, instanceName, resourceName, partitionNames);
  }

  @Override
  public List<String> getInstancesInCluster(String clusterName) {
    return helixAdmin.getInstancesInCluster(clusterName);
  }

  @Override
  public void createVeniceStorageClusterResources(String clusterName, String kafkaTopic , int numberOfPartition ,
      int replicationFactor, boolean isLeaderFollowerStateModel) {
    if (!helixAdmin.getResourcesInCluster(clusterName).contains(kafkaTopic)) {
      helixAdmin.addResource(clusterName, kafkaTopic, numberOfPartition,
          isLeaderFollowerStateModel ? LeaderStandbySMD.name : VeniceStateModel.PARTITION_ONLINE_OFFLINE_STATE_MODEL,
          IdealState.RebalanceMode.FULL_AUTO.toString(),
          AutoRebalanceStrategy.class.getName());
      VeniceControllerClusterConfig config = multiClusterConfigs.getConfigForCluster(clusterName);
      IdealState idealState = helixAdmin.getResourceIdealState(clusterName, kafkaTopic);
      // We don't set the delayed time per resource, we will use the cluster level helix config to decide
      // the delayed rebalance time
      idealState.setRebalancerClassName(DelayedAutoRebalancer.class.getName());
      idealState.setMinActiveReplicas(config.getMinActiveReplica());
      idealState.setRebalanceStrategy(config.getHelixRebalanceAlg());
      helixAdmin.setResourceIdealState(clusterName, kafkaTopic, idealState);
      LOGGER.info("Enabled delayed re-balance for resource:" + kafkaTopic);
      helixAdmin.rebalance(clusterName, kafkaTopic, replicationFactor);
      LOGGER.info("Added " + kafkaTopic + " as a resource to cluster: " + clusterName);
    } else {
      String errorMessage = "Resource:" + kafkaTopic + " already exists, Can not add it to Helix.";
      LOGGER.warn(errorMessage);
      throw new VeniceException(errorMessage);
    }
  }

  @Override
  public void dropResource(String clusterName, String resourceName) {
    helixAdmin.dropResource(clusterName, resourceName);
  }

  @Override
  public void dropStorageInstance(String clusterName, String instanceName) {
    InstanceConfig instanceConfig = helixAdmin.getInstanceConfig(clusterName, instanceName);
    helixAdmin.dropInstance(clusterName, instanceConfig);
  }

  @Override
  public void resetPartition(String clusterName, String instanceName, String resourceName, List<String> partitionNames) {
    helixAdmin.resetPartition(clusterName, instanceName, resourceName, partitionNames);
  }

  @Override
  public void close() {
    helixAdmin.close();
  }
}
