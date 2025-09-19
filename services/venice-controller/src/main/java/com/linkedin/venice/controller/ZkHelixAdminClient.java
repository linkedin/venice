package com.linkedin.venice.controller;

import com.linkedin.venice.controller.helix.HelixCapacityConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceRetriableException;
import com.linkedin.venice.helix.ZkClientFactory;
import com.linkedin.venice.stats.ZkClientStatusStats;
import com.linkedin.venice.utils.RetryUtils;
import io.tehuti.metrics.MetricsRepository;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.controller.rebalancer.DelayedAutoRebalancer;
import org.apache.helix.controller.rebalancer.strategy.AutoRebalanceStrategy;
import org.apache.helix.controller.rebalancer.waged.WagedRebalancer;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LeaderStandbySMD;
import org.apache.helix.model.RESTConfig;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * The purpose of this class is to abstract Helix operations out of the {@link VeniceHelixAdmin} and eventually rename
 * it to VeniceAdmin. This is one implementation of the {@link HelixAdminClient} interface which uses Zk based Helix
 * API. In the future we might move to the Helix REST API.
 */
public class ZkHelixAdminClient implements HelixAdminClient {
  private static final Logger LOGGER = LogManager.getLogger(ZkHelixAdminClient.class);
  private static final int CONTROLLER_CLUSTER_PARTITION_COUNT = 1;
  private static final String CONTROLLER_HAAS_ZK_CLIENT_NAME = "controller-zk-client-for-haas-admin";

  // TODO: Replace with config from Helix lib once we pick up a fresher Helix dependency
  static final String HELIX_PARTICIPANT_DEREGISTRATION_TIMEOUT_CONFIG = "PARTICIPANT_DEREGISTRATION_TIMEOUT";

  private final HelixAdmin helixAdmin;
  private final ConfigAccessor helixConfigAccessor;
  private final VeniceControllerMultiClusterConfig multiClusterConfigs;
  private final String haasSuperClusterName;
  private final String controllerClusterName;

  public ZkHelixAdminClient(
      VeniceControllerMultiClusterConfig multiClusterConfigs,
      MetricsRepository metricsRepository) {
    this.multiClusterConfigs = multiClusterConfigs;
    haasSuperClusterName = multiClusterConfigs.getControllerHAASSuperClusterName();
    controllerClusterName = multiClusterConfigs.getControllerClusterName();
    ZkClient helixAdminZkClient = ZkClientFactory.newZkClient(multiClusterConfigs.getZkAddress());
    helixAdminZkClient
        .subscribeStateChanges(new ZkClientStatusStats(metricsRepository, CONTROLLER_HAAS_ZK_CLIENT_NAME));
    helixAdminZkClient.setZkSerializer(new ZNRecordSerializer());
    if (!helixAdminZkClient.waitUntilConnected(ZkClient.DEFAULT_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS)) {
      throw new VeniceException("Failed to connect to ZK within " + ZkClient.DEFAULT_CONNECTION_TIMEOUT + " ms!");
    }
    helixAdmin = new ZKHelixAdmin(helixAdminZkClient);
    helixConfigAccessor = new ConfigAccessor(helixAdminZkClient);
  }

  /**
   * @see HelixAdminClient#isVeniceControllerClusterCreated()
   */
  @Override
  public boolean isVeniceControllerClusterCreated() {
    return helixAdmin.getClusters().contains(controllerClusterName);
  }

  /**
   * @see HelixAdminClient#isVeniceStorageClusterCreated(String)
   */
  @Override
  public boolean isVeniceStorageClusterCreated(String clusterName) {
    return helixAdmin.getClusters().contains(clusterName);
  }

  /**
   * @see HelixAdminClient#createVeniceControllerCluster()
   */
  @Override
  public void createVeniceControllerCluster() {
    boolean success = RetryUtils.executeWithMaxAttempt(() -> {
      if (!isVeniceControllerClusterCreated()) {
        if (!helixAdmin.addCluster(controllerClusterName, false)) {
          throw new VeniceRetriableException("Failed to create Helix cluster, will retry");
        }
        ClusterConfig clusterConfig = new ClusterConfig(controllerClusterName);
        clusterConfig.getRecord().setBooleanField(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, true);
        // Topology and fault zone type fields are used by CRUSH alg. Helix would apply the constraints on CRUSH alg to
        // choose proper instance to hold the replica.
        clusterConfig.setTopologyAwareEnabled(false);
        clusterConfig.setPersistBestPossibleAssignment(true);

        if (multiClusterConfigs.getHelixGlobalRebalancePreference() != null) {
          // We want to prioritize evenness over less movement when it comes to resource assignment, because the cost
          // of rebalancing for the controller is cheap as it is stateless.
          clusterConfig.setGlobalRebalancePreference(multiClusterConfigs.getHelixGlobalRebalancePreference());
        }

        HelixCapacityConfig helixCapacityConfig = multiClusterConfigs.getHelixCapacityConfig();
        if (multiClusterConfigs.getHelixCapacityConfig() != null) {
          clusterConfig.setInstanceCapacityKeys(helixCapacityConfig.getHelixInstanceCapacityKeys());

          // This is how much capacity a participant can take. The Helix documentation recommends setting this to a high
          // value to avoid rebalance failures. The primary goal of setting this is to enable a constraint that takes
          // the current top-state distribution into account when rebalancing.
          clusterConfig.setDefaultInstanceCapacityMap(helixCapacityConfig.getHelixDefaultInstanceCapacityMap());
          clusterConfig.setDefaultPartitionWeightMap(helixCapacityConfig.getHelixDefaultPartitionWeightMap());
        }

        if (multiClusterConfigs.getControllerHelixParticipantDeregistrationTimeoutMs() >= 0) {
          clusterConfig.getRecord()
              .setLongField(
                  HELIX_PARTICIPANT_DEREGISTRATION_TIMEOUT_CONFIG,
                  multiClusterConfigs.getControllerHelixParticipantDeregistrationTimeoutMs());
        }

        updateClusterConfigs(controllerClusterName, clusterConfig);
        helixAdmin.addStateModelDef(controllerClusterName, LeaderStandbySMD.name, LeaderStandbySMD.build());

        if (multiClusterConfigs.isControllerClusterHelixCloudEnabled()) {
          helixAdmin.addCloudConfig(controllerClusterName, multiClusterConfigs.getHelixCloudConfig());
        }
      }
      return true;
    }, 3, Duration.ofSeconds(5), Collections.singletonList(Exception.class));
    if (!success) {
      throw new VeniceException(
          "Failed to create Helix cluster: " + controllerClusterName
              + " after 3 attempts. HelixAdmin#addCluster returned false");
    }
  }

  /**
   * @see HelixAdminClient#createVeniceStorageCluster(String, ClusterConfig, RESTConfig)
   */
  @Override
  public void createVeniceStorageCluster(String clusterName, ClusterConfig helixClusterConfig, RESTConfig restConfig) {
    boolean success = RetryUtils.executeWithMaxAttempt(() -> {
      if (!isVeniceStorageClusterCreated(clusterName)) {
        if (!helixAdmin.addCluster(clusterName, false)) {
          throw new VeniceRetriableException("Failed to create Helix cluster, will retry");
        }
        updateClusterConfigs(clusterName, helixClusterConfig);
        helixAdmin.addStateModelDef(clusterName, LeaderStandbySMD.name, LeaderStandbySMD.build());

        VeniceControllerClusterConfig clusterConfig = multiClusterConfigs.getControllerConfig(clusterName);
        if (clusterConfig.isStorageClusterHelixCloudEnabled()) {
          helixAdmin.addCloudConfig(clusterName, clusterConfig.getHelixCloudConfig());
        }

        if (restConfig != null) {
          updateRESTConfigs(clusterName, restConfig);
        }
      }
      return true;
    }, 3, Duration.ofSeconds(5), Collections.singletonList(Exception.class));
    if (!success) {
      throw new VeniceException(
          "Failed to create Helix cluster: " + clusterName + " after 3 attempts. HelixAdmin#addCluster returned false");
    }
  }

  /**
   * @see HelixAdminClient#isVeniceStorageClusterInControllerCluster(String)
   */
  @Override
  public boolean isVeniceStorageClusterInControllerCluster(String clusterName) {
    return helixAdmin.getResourcesInCluster(controllerClusterName).contains(clusterName);
  }

  /**
   * @see HelixAdminClient#addVeniceStorageClusterToControllerCluster(String)
   */
  @Override
  public void addVeniceStorageClusterToControllerCluster(String clusterName) {
    try {
      helixAdmin.addResource(
          controllerClusterName,
          clusterName,
          CONTROLLER_CLUSTER_PARTITION_COUNT,
          LeaderStandbySMD.name,
          IdealState.RebalanceMode.FULL_AUTO.toString(),
          AutoRebalanceStrategy.class.getName());
    } catch (Exception e) {
      // Check if the cluster resource is already added to the controller cluster by another Venice controller
      // concurrently.
      if (!isVeniceStorageClusterInControllerCluster(clusterName)) {
        throw e;
      } else {
        LOGGER.info(
            "Controller cluster resource for storage cluster: {} already exists in controller cluster: {}",
            clusterName,
            controllerClusterName);
      }
      return;
    }

    VeniceControllerClusterConfig config = multiClusterConfigs.getControllerConfig(clusterName);
    IdealState idealState = helixAdmin.getResourceIdealState(controllerClusterName, clusterName);
    int controllerClusterReplicaCount = config.getControllerClusterReplica();
    idealState.setReplicas(String.valueOf(controllerClusterReplicaCount));
    idealState.setMinActiveReplicas(Math.max(controllerClusterReplicaCount - 1, 1));
    idealState.setRebalancerClassName(WagedRebalancer.class.getName());

    String instanceGroupTag = config.getControllerResourceInstanceGroupTag();
    if (!instanceGroupTag.isEmpty()) {
      idealState.setInstanceGroupTag(instanceGroupTag);
    }

    helixAdmin.setResourceIdealState(controllerClusterName, clusterName, idealState);
    helixAdmin.rebalance(controllerClusterName, clusterName, controllerClusterReplicaCount);
  }

  /**
   * @see HelixAdminClient#isClusterInGrandCluster(String)
   */
  @Override
  public boolean isClusterInGrandCluster(String clusterName) {
    return helixAdmin.getResourcesInCluster(haasSuperClusterName).contains(clusterName);
  }

  /**
   * @see HelixAdminClient#addClusterToGrandCluster(String)
   */
  @Override
  public void addClusterToGrandCluster(String clusterName) {
    try {
      helixAdmin.addClusterToGrandCluster(clusterName, haasSuperClusterName);
    } catch (Exception e) {
      // Check if the cluster is already added to the grand cluster by another Venice controller concurrently.
      if (!isClusterInGrandCluster(clusterName)) {
        throw e;
      }
    }
  }

  /**
   * @see HelixAdminClient#updateClusterConfigs(String, ClusterConfig)
   */
  @Override
  public void updateClusterConfigs(String clusterName, ClusterConfig clusterConfig) {
    helixConfigAccessor.setClusterConfig(clusterName, clusterConfig);
  }

  /**
   * @see HelixAdminClient#updateRESTConfigs(String, RESTConfig)
   */
  @Override
  public void updateRESTConfigs(String clusterName, RESTConfig restConfig) {
    helixConfigAccessor.setRESTConfig(clusterName, restConfig);
  }

  /**
   * @see HelixAdminClient#enablePartition(boolean, String, String, String, List)
   */
  @Override
  public void enablePartition(
      boolean enabled,
      String clusterName,
      String instanceName,
      String resourceName,
      List<String> partitionNames) {
    helixAdmin.enablePartition(enabled, clusterName, instanceName, resourceName, partitionNames);
  }

  /**
   * @see HelixAdminClient#getInstancesInCluster(String)
   */
  @Override
  public List<String> getInstancesInCluster(String clusterName) {
    return helixAdmin.getInstancesInCluster(clusterName);
  }

  /**
   * @see HelixAdminClient#createVeniceStorageClusterResources(String, String, int, int)
   */
  @Override
  public void createVeniceStorageClusterResources(
      String clusterName,
      String kafkaTopic,
      int numberOfPartition,
      int replicationFactor) {
    if (!helixAdmin.getResourcesInCluster(clusterName).contains(kafkaTopic)) {
      helixAdmin.addResource(
          clusterName,
          kafkaTopic,
          numberOfPartition,
          LeaderStandbySMD.name,
          IdealState.RebalanceMode.FULL_AUTO.toString(),
          AutoRebalanceStrategy.class.getName());
      VeniceControllerClusterConfig config = multiClusterConfigs.getControllerConfig(clusterName);
      IdealState idealState = helixAdmin.getResourceIdealState(clusterName, kafkaTopic);
      // We don't set the delayed time per resource, we will use the cluster level helix config to decide
      // the delayed rebalance time
      idealState.setRebalancerClassName(DelayedAutoRebalancer.class.getName());
      idealState.setMinActiveReplicas(Math.max(replicationFactor - 1, 1));
      idealState.setRebalanceStrategy(config.getHelixRebalanceAlg());
      helixAdmin.setResourceIdealState(clusterName, kafkaTopic, idealState);
      LOGGER.info("Enabled delayed re-balance for resource: {}", kafkaTopic);
      helixAdmin.rebalance(clusterName, kafkaTopic, replicationFactor);
      LOGGER.info("Added {} as a resource to cluster: {}", kafkaTopic, clusterName);
    } else {
      String errorMessage = "Resource:" + kafkaTopic + " already exists, Can not add it to Helix.";
      LOGGER.warn(errorMessage);
      throw new VeniceException(errorMessage);
    }
  }

  @Override
  public boolean containsResource(String clusterName, String resourceName) {
    return helixAdmin.getResourceIdealState(clusterName, resourceName) != null;
  }

  /**
   * @see HelixAdminClient#dropResource(String, String)
   */
  @Override
  public void dropResource(String clusterName, String resourceName) {
    helixAdmin.dropResource(clusterName, resourceName);
  }

  /**
   * @see HelixAdminClient#dropStorageInstance(String, String)
   */
  @Override
  public void dropStorageInstance(String clusterName, String instanceName) {
    InstanceConfig instanceConfig = helixAdmin.getInstanceConfig(clusterName, instanceName);
    helixAdmin.dropInstance(clusterName, instanceConfig);
  }

  public Map<String, List<String>> getDisabledPartitionsMap(String clusterName, String instanceName) {
    InstanceConfig instanceConfig = helixAdmin.getInstanceConfig(clusterName, instanceName);
    return instanceConfig.getDisabledPartitionsMap();
  }

  /**
   * @see HelixAdminClient#resetPartition(String, String, String, List)
   */
  @Override
  public void resetPartition(
      String clusterName,
      String instanceName,
      String resourceName,
      List<String> partitionNames) {
    helixAdmin.resetPartition(clusterName, instanceName, resourceName, partitionNames);
  }

  /**
   * @see HelixAdminClient#close()
   */
  @Override
  public void close() {
    helixAdmin.close();
  }

  /**
   * @see HelixAdminClient#manuallyEnableMaintenanceMode(String, boolean, String, Map<String, String>)
   */
  public void manuallyEnableMaintenanceMode(
      String clusterName,
      boolean enabled,
      String reason,
      Map<String, String> customFields) {
    helixAdmin.manuallyEnableMaintenanceMode(clusterName, enabled, reason, customFields);
  }

  /**
   * @see HelixAdminClient#setInstanceOperation(String, String, InstanceConstants.InstanceOperation, String)
   */
  public void setInstanceOperation(
      String clusterName,
      String instanceName,
      InstanceConstants.InstanceOperation instanceOperation,
      String reason) {
    helixAdmin.setInstanceOperation(clusterName, instanceName, instanceOperation, reason);
  }

  public IdealState getResourceIdealState(String clusterName, String resourceName) {
    return helixAdmin.getResourceIdealState(clusterName, resourceName);
  }

  public void updateIdealState(String clusterName, String resourceName, IdealState idealState) {
    helixAdmin.updateIdealState(clusterName, resourceName, idealState);
  }
}
