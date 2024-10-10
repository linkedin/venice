package com.linkedin.venice.controller;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceRetriableException;
import com.linkedin.venice.helix.ZkClientFactory;
import com.linkedin.venice.stats.ZkClientStatusStats;
import com.linkedin.venice.utils.RetryUtils;
import io.tehuti.metrics.MetricsRepository;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.helix.HelixAdmin;
import org.apache.helix.controller.rebalancer.DelayedAutoRebalancer;
import org.apache.helix.controller.rebalancer.strategy.AutoRebalanceStrategy;
import org.apache.helix.controller.rebalancer.waged.WagedRebalancer;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.model.CloudConfig;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LeaderStandbySMD;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
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

  private final HelixAdmin helixAdmin;
  private final VeniceControllerClusterConfig commonConfig;
  private final VeniceControllerMultiClusterConfig multiClusterConfigs;
  private final String haasSuperClusterName;
  private final String controllerClusterName;
  private final int controllerClusterReplicaCount;

  public ZkHelixAdminClient(
      VeniceControllerMultiClusterConfig multiClusterConfigs,
      MetricsRepository metricsRepository) {
    this.multiClusterConfigs = multiClusterConfigs;
    this.commonConfig = multiClusterConfigs.getCommonConfig();
    haasSuperClusterName = multiClusterConfigs.getControllerHAASSuperClusterName();
    controllerClusterName = multiClusterConfigs.getControllerClusterName();
    controllerClusterReplicaCount = multiClusterConfigs.getControllerClusterReplica();
    ZkClient helixAdminZkClient = ZkClientFactory.newZkClient(multiClusterConfigs.getZkAddress());
    helixAdminZkClient
        .subscribeStateChanges(new ZkClientStatusStats(metricsRepository, CONTROLLER_HAAS_ZK_CLIENT_NAME));
    helixAdminZkClient.setZkSerializer(new ZNRecordSerializer());
    if (!helixAdminZkClient.waitUntilConnected(ZkClient.DEFAULT_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS)) {
      throw new VeniceException("Failed to connect to ZK within " + ZkClient.DEFAULT_CONNECTION_TIMEOUT + " ms!");
    }
    helixAdmin = new ZKHelixAdmin(helixAdminZkClient);
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
        Map<String, String> helixClusterProperties = new HashMap<>();
        helixClusterProperties.put(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, String.valueOf(true));
        // Topology and fault zone type fields are used by CRUSH alg. Helix would apply the constrains on CRUSH alg to
        // choose proper instance to hold the replica.
        helixClusterProperties
            .put(ClusterConfig.ClusterConfigProperty.TOPOLOGY_AWARE_ENABLED.name(), String.valueOf(false));

        updateClusterConfigs(controllerClusterName, helixClusterProperties);
        helixAdmin.addStateModelDef(controllerClusterName, LeaderStandbySMD.name, LeaderStandbySMD.build());

        if (commonConfig.isControllerClusterHelixCloudEnabled()) {
          setCloudConfig(controllerClusterName, commonConfig);
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
   * @see HelixAdminClient#createVeniceStorageCluster(String, Map)
   */
  @Override
  public void createVeniceStorageCluster(String clusterName, Map<String, String> helixClusterProperties) {
    boolean success = RetryUtils.executeWithMaxAttempt(() -> {
      if (!isVeniceStorageClusterCreated(clusterName)) {
        if (!helixAdmin.addCluster(clusterName, false)) {
          throw new VeniceRetriableException("Failed to create Helix cluster, will retry");
        }
        updateClusterConfigs(clusterName, helixClusterProperties);
        helixAdmin.addStateModelDef(clusterName, LeaderStandbySMD.name, LeaderStandbySMD.build());

        VeniceControllerClusterConfig config = multiClusterConfigs.getControllerConfig(clusterName);
        if (config.isStorageClusterHelixCloudEnabled()) {
          setCloudConfig(clusterName, config);
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
      VeniceControllerClusterConfig config = multiClusterConfigs.getControllerConfig(clusterName);
      IdealState idealState = helixAdmin.getResourceIdealState(controllerClusterName, clusterName);
      idealState.setMinActiveReplicas(controllerClusterReplicaCount);
      idealState.setRebalancerClassName(WagedRebalancer.class.getName());

      String instanceGroupTag = config.getControllerResourceInstanceGroupTag();
      if (!instanceGroupTag.isEmpty()) {
        idealState.setInstanceGroupTag(instanceGroupTag);
      }

      helixAdmin.setResourceIdealState(controllerClusterName, clusterName, idealState);
      helixAdmin.rebalance(controllerClusterName, clusterName, controllerClusterReplicaCount);
    } catch (Exception e) {
      // Check if the cluster resource is already added to the controller cluster by another Venice controller
      // concurrently.
      if (!isVeniceStorageClusterInControllerCluster(clusterName)) {
        throw e;
      }
    }
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
   * @see HelixAdminClient#updateClusterConfigs(String, Map)
   */
  @Override
  public void updateClusterConfigs(String clusterName, Map<String, String> helixClusterProperties) {
    HelixConfigScope configScope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(clusterName).build();
    helixAdmin.setConfig(configScope, helixClusterProperties);
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
      idealState.setMinActiveReplicas(replicationFactor - 1);
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
   * @see HelixAdminClient#addInstanceTag(String, String, String)()
   */
  @Override
  public void addInstanceTag(String clusterName, String instanceName, String tag) {
    helixAdmin.addInstanceTag(clusterName, instanceName, tag);
  }

  public void setCloudConfig(String clusterName, VeniceControllerClusterConfig config) {
    String cloudId = config.getHelixCloudId();
    List<String> cloudInfoSources = config.getHelixCloudInfoSources();
    String cloudInfoProcessorName = config.getHelixCloudInfoProcessorName();
    CloudConfig.Builder cloudConfigBuilder =
        new CloudConfig.Builder().setCloudEnabled(true).setCloudProvider(config.getHelixCloudProvider());

    if (!cloudId.isEmpty()) {
      cloudConfigBuilder.setCloudID(cloudId);
    }

    if (!cloudInfoSources.isEmpty()) {
      cloudConfigBuilder.setCloudInfoSources(cloudInfoSources);
    }

    if (!cloudInfoProcessorName.isEmpty()) {
      cloudConfigBuilder.setCloudInfoProcessorName(cloudInfoProcessorName);
    }
    helixAdmin.addCloudConfig(clusterName, cloudConfigBuilder.build());
  }
}
