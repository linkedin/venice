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
import org.apache.helix.controller.rebalancer.strategy.CrushRebalanceStrategy;
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
  private static final String CONTROLLER_CLUSTER_ZK_CLIENT_NAME = "controller-zk-client-for-controller-cluster";

  // TODO: Replace with config from Helix lib once we pick up a fresher Helix dependency
  static final String HELIX_PARTICIPANT_DEREGISTRATION_TIMEOUT_CONFIG = "PARTICIPANT_DEREGISTRATION_TIMEOUT";

  /**
   * HelixAdmin/ConfigAccessor connected to {@code zookeeper.address} (storage cluster ZK). Used for operations on
   * Venice storage clusters (e.g. createVeniceStorageCluster, dropResource, enablePartition for storage clusters).
   */
  private final HelixAdmin helixAdmin;
  private final ConfigAccessor helixConfigAccessor;
  /**
   * HelixAdmin/ConfigAccessor connected to {@code controller.cluster.zk.address}. Used for operations on the controller
   * cluster ({@link #controllerClusterName}) and the HAAS grand cluster ({@link #haasSuperClusterName}). When the two
   * ZK addresses are equal (the common case) this aliases {@link #helixAdmin}/{@link #helixConfigAccessor} to avoid a
   * second ZK connection.
   */
  private final HelixAdmin controllerClusterHelixAdmin;
  private final ConfigAccessor controllerClusterHelixConfigAccessor;
  private final VeniceControllerMultiClusterConfig multiClusterConfigs;
  private final String haasSuperClusterName;
  private final String controllerClusterName;

  public ZkHelixAdminClient(
      VeniceControllerMultiClusterConfig multiClusterConfigs,
      MetricsRepository metricsRepository) {
    this.multiClusterConfigs = multiClusterConfigs;
    haasSuperClusterName = multiClusterConfigs.getControllerHAASSuperClusterName();
    controllerClusterName = multiClusterConfigs.getControllerClusterName();

    String storageZkAddress = multiClusterConfigs.getZkAddress();
    ZkClient helixAdminZkClient = ZkClientFactory.newZkClient(storageZkAddress);
    helixAdminZkClient
        .subscribeStateChanges(new ZkClientStatusStats(metricsRepository, CONTROLLER_HAAS_ZK_CLIENT_NAME));
    helixAdminZkClient.setZkSerializer(new ZNRecordSerializer());
    if (!helixAdminZkClient.waitUntilConnected(ZkClient.DEFAULT_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS)) {
      throw new VeniceException("Failed to connect to ZK within " + ZkClient.DEFAULT_CONNECTION_TIMEOUT + " ms!");
    }
    helixAdmin = new ZKHelixAdmin(helixAdminZkClient);
    helixConfigAccessor = new ConfigAccessor(helixAdminZkClient);

    String controllerClusterZkAddress = multiClusterConfigs.getControllerClusterZkAddress();
    if (storageZkAddress.equals(controllerClusterZkAddress)) {
      // Common case: both addresses are equal. Reuse the storage-cluster admin and accessor to avoid a second ZK
      // connection. controller.cluster.zk.address defaults to zookeeper.address when not explicitly overridden, so
      // existing deployments hit this path unchanged.
      controllerClusterHelixAdmin = helixAdmin;
      controllerClusterHelixConfigAccessor = helixConfigAccessor;
    } else {
      // Split-ZK deployment: zookeeper.address and controller.cluster.zk.address point to different ensembles. Keep
      // operations on the controller cluster and HAAS grand cluster on controller.cluster.zk.address; storage cluster
      // operations stay on zookeeper.address. Without this, the controller cluster would be created on one ZK while
      // participants register on the other, leading to "Missing znode .../LIVEINSTANCES/<host>_<port>" failures.
      LOGGER.info(
          "controller.cluster.zk.address ({}) differs from zookeeper.address ({}); creating a separate HelixAdmin "
              + "for operations on the controller cluster and HAAS grand cluster.",
          controllerClusterZkAddress,
          storageZkAddress);
      ZkClient controllerClusterZkClient = ZkClientFactory.newZkClient(controllerClusterZkAddress);
      controllerClusterZkClient
          .subscribeStateChanges(new ZkClientStatusStats(metricsRepository, CONTROLLER_CLUSTER_ZK_CLIENT_NAME));
      controllerClusterZkClient.setZkSerializer(new ZNRecordSerializer());
      if (!controllerClusterZkClient.waitUntilConnected(ZkClient.DEFAULT_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS)) {
        throw new VeniceException(
            "Failed to connect to controller cluster ZK within " + ZkClient.DEFAULT_CONNECTION_TIMEOUT + " ms!");
      }
      controllerClusterHelixAdmin = new ZKHelixAdmin(controllerClusterZkClient);
      controllerClusterHelixConfigAccessor = new ConfigAccessor(controllerClusterZkClient);
    }
  }

  /**
   * Returns the {@link HelixAdmin} that should be used for operations on {@code clusterName}. Operations on the
   * controller cluster and HAAS grand cluster route to the controller-cluster ZK admin; everything else routes to
   * the storage ZK admin.
   */
  private HelixAdmin helixAdminFor(String clusterName) {
    if (clusterName.equals(controllerClusterName) || clusterName.equals(haasSuperClusterName)) {
      return controllerClusterHelixAdmin;
    }
    return helixAdmin;
  }

  /**
   * Returns the {@link ConfigAccessor} that should be used for operations on {@code clusterName}. Same routing rule
   * as {@link #helixAdminFor(String)}.
   */
  private ConfigAccessor helixConfigAccessorFor(String clusterName) {
    if (clusterName.equals(controllerClusterName) || clusterName.equals(haasSuperClusterName)) {
      return controllerClusterHelixConfigAccessor;
    }
    return helixConfigAccessor;
  }

  /**
   * @see HelixAdminClient#isVeniceControllerClusterCreated()
   */
  @Override
  public boolean isVeniceControllerClusterCreated() {
    return controllerClusterHelixAdmin.getClusters().contains(controllerClusterName);
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
        if (!controllerClusterHelixAdmin.addCluster(controllerClusterName, false)) {
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
        controllerClusterHelixAdmin
            .addStateModelDef(controllerClusterName, LeaderStandbySMD.name, LeaderStandbySMD.build());

        if (multiClusterConfigs.isControllerClusterHelixCloudEnabled()) {
          controllerClusterHelixAdmin.addCloudConfig(controllerClusterName, multiClusterConfigs.getHelixCloudConfig());
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
    // The resource being checked lives on the controller cluster, so route via the controller-cluster admin.
    return controllerClusterHelixAdmin.getResourcesInCluster(controllerClusterName).contains(clusterName);
  }

  /**
   * @see HelixAdminClient#addVeniceStorageClusterToControllerCluster(String)
   */
  @Override
  public void addVeniceStorageClusterToControllerCluster(String clusterName) {
    try {
      controllerClusterHelixAdmin.addResource(
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
    IdealState idealState = controllerClusterHelixAdmin.getResourceIdealState(controllerClusterName, clusterName);
    int controllerClusterReplicaCount = config.getControllerClusterReplica();
    idealState.setReplicas(String.valueOf(controllerClusterReplicaCount));
    idealState.setMinActiveReplicas(Math.max(controllerClusterReplicaCount - 1, 1));
    idealState.setRebalancerClassName(WagedRebalancer.class.getName());

    String instanceGroupTag = config.getControllerResourceInstanceGroupTag();
    if (!instanceGroupTag.isEmpty()) {
      idealState.setInstanceGroupTag(instanceGroupTag);
    }

    controllerClusterHelixAdmin.setResourceIdealState(controllerClusterName, clusterName, idealState);
    controllerClusterHelixAdmin.rebalance(controllerClusterName, clusterName, controllerClusterReplicaCount);
  }

  /**
   * @see HelixAdminClient#addStorageClusterResourceToControllerCluster(String, int)
   */
  @Override
  public void addStorageClusterResourceToControllerCluster(String clusterName, int replicaCount) {
    if (isVeniceStorageClusterInControllerCluster(clusterName)) {
      return;
    }
    // Use controllerClusterHelixAdmin (routes to controller.cluster.zk.address) so this works in
    // both shared-ZK and split-ZK deployments. Keep the legacy non-HAAS rebalancer config so
    // that the assignment algorithm is unchanged for this path.
    controllerClusterHelixAdmin.addResource(
        controllerClusterName,
        clusterName,
        CONTROLLER_CLUSTER_PARTITION_COUNT,
        LeaderStandbySMD.name,
        IdealState.RebalanceMode.FULL_AUTO.toString(),
        AutoRebalanceStrategy.class.getName());
    IdealState idealState = controllerClusterHelixAdmin.getResourceIdealState(controllerClusterName, clusterName);
    idealState.setReplicas(String.valueOf(replicaCount));
    idealState.setMinActiveReplicas(replicaCount);
    idealState.setRebalancerClassName(DelayedAutoRebalancer.class.getName());
    idealState.setRebalanceStrategy(CrushRebalanceStrategy.class.getName());
    controllerClusterHelixAdmin.setResourceIdealState(controllerClusterName, clusterName, idealState);
    controllerClusterHelixAdmin.rebalance(controllerClusterName, clusterName, replicaCount);
  }

  @Override
  public boolean isClusterInGrandCluster(String clusterName) {
    return controllerClusterHelixAdmin.getResourcesInCluster(haasSuperClusterName).contains(clusterName);
  }

  /**
   * @see HelixAdminClient#addClusterToGrandCluster(String)
   */
  @Override
  public void addClusterToGrandCluster(String clusterName) {
    try {
      controllerClusterHelixAdmin.addClusterToGrandCluster(clusterName, haasSuperClusterName);
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
    helixConfigAccessorFor(clusterName).setClusterConfig(clusterName, clusterConfig);
  }

  /**
   * @see HelixAdminClient#updateRESTConfigs(String, RESTConfig)
   */
  @Override
  public void updateRESTConfigs(String clusterName, RESTConfig restConfig) {
    helixConfigAccessorFor(clusterName).setRESTConfig(clusterName, restConfig);
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
    helixAdminFor(clusterName).enablePartition(enabled, clusterName, instanceName, resourceName, partitionNames);
  }

  /**
   * @see HelixAdminClient#getInstancesInCluster(String)
   */
  @Override
  public List<String> getInstancesInCluster(String clusterName) {
    return helixAdminFor(clusterName).getInstancesInCluster(clusterName);
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
    return helixAdminFor(clusterName).getResourceIdealState(clusterName, resourceName) != null;
  }

  /**
   * @see HelixAdminClient#dropResource(String, String)
   */
  @Override
  public void dropResource(String clusterName, String resourceName) {
    helixAdminFor(clusterName).dropResource(clusterName, resourceName);
  }

  /**
   * @see HelixAdminClient#dropStorageInstance(String, String)
   */
  @Override
  public void dropStorageInstance(String clusterName, String instanceName) {
    HelixAdmin admin = helixAdminFor(clusterName);
    InstanceConfig instanceConfig = admin.getInstanceConfig(clusterName, instanceName);
    admin.dropInstance(clusterName, instanceConfig);
  }

  public Map<String, List<String>> getDisabledPartitionsMap(String clusterName, String instanceName) {
    InstanceConfig instanceConfig = helixAdminFor(clusterName).getInstanceConfig(clusterName, instanceName);
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
    helixAdminFor(clusterName).resetPartition(clusterName, instanceName, resourceName, partitionNames);
  }

  /**
   * @see HelixAdminClient#close()
   */
  @Override
  public void close() {
    helixAdmin.close();
    if (controllerClusterHelixAdmin != helixAdmin) {
      controllerClusterHelixAdmin.close();
    }
  }

  /**
   * @see HelixAdminClient#manuallyEnableMaintenanceMode(String, boolean, String, Map<String, String>)
   */
  public void manuallyEnableMaintenanceMode(
      String clusterName,
      boolean enabled,
      String reason,
      Map<String, String> customFields) {
    helixAdminFor(clusterName).manuallyEnableMaintenanceMode(clusterName, enabled, reason, customFields);
  }

  /**
   * @see HelixAdminClient#setInstanceOperation(String, String, InstanceConstants.InstanceOperation, String)
   */
  public void setInstanceOperation(
      String clusterName,
      String instanceName,
      InstanceConstants.InstanceOperation instanceOperation,
      String reason) {
    helixAdminFor(clusterName).setInstanceOperation(clusterName, instanceName, instanceOperation, reason);
  }

  public IdealState getResourceIdealState(String clusterName, String resourceName) {
    return helixAdminFor(clusterName).getResourceIdealState(clusterName, resourceName);
  }

  public void updateIdealState(String clusterName, String resourceName, IdealState idealState) {
    helixAdminFor(clusterName).updateIdealState(clusterName, resourceName, idealState);
  }
}
