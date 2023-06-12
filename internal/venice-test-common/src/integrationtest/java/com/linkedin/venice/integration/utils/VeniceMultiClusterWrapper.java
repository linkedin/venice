package com.linkedin.venice.integration.utils;

import static com.linkedin.venice.ConfigKeys.CONTROLLER_ENABLE_BATCH_PUSH_FROM_ADMIN_IN_CHILD;
import static com.linkedin.venice.ConfigKeys.LOCAL_REGION_NAME;
import static com.linkedin.venice.ConfigKeys.SYSTEM_SCHEMA_CLUSTER_NAME;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.KafkaSSLUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;


public class VeniceMultiClusterWrapper extends ProcessWrapper {
  public static final String SERVICE_NAME = "VeniceMultiCluster";
  private final Map<String, VeniceClusterWrapper> clusters;
  private final Map<Integer, VeniceControllerWrapper> controllers;
  private final ZkServerWrapper zkServerWrapper;
  private final PubSubBrokerWrapper pubSubBrokerWrapper;
  private final Map<String, String> clusterToD2;
  private final D2Client clientConfigD2Client;
  private final String regionName;

  VeniceMultiClusterWrapper(
      File dataDirectory,
      ZkServerWrapper zkServerWrapper,
      PubSubBrokerWrapper pubSubBrokerWrapper,
      Map<String, VeniceClusterWrapper> clusters,
      Map<Integer, VeniceControllerWrapper> controllers,
      Map<String, String> clusterToD2,
      D2Client clientConfigD2Client,
      String regionName) {
    super(SERVICE_NAME, dataDirectory);
    this.zkServerWrapper = zkServerWrapper;
    this.pubSubBrokerWrapper = pubSubBrokerWrapper;
    this.controllers = controllers;
    this.clusters = clusters;
    this.clusterToD2 = clusterToD2;
    this.clientConfigD2Client = clientConfigD2Client;
    this.regionName = regionName;
  }

  static ServiceProvider<VeniceMultiClusterWrapper> generateService(VeniceMultiClusterCreateOptions options) {
    Map<String, VeniceClusterWrapper> clusterWrapperMap = new HashMap<>();
    Map<Integer, VeniceControllerWrapper> controllerMap = new HashMap<>();
    ZkServerWrapper zkServerWrapper = options.getZkServerWrapper();
    PubSubBrokerWrapper pubSubBrokerWrapper = options.getKafkaBrokerWrapper();

    try {
      if (zkServerWrapper == null) {
        zkServerWrapper = ServiceFactory.getZkServer();
      }
      if (pubSubBrokerWrapper == null) {
        pubSubBrokerWrapper =
            ServiceFactory.getPubSubBroker(new PubSubBrokerConfigs.Builder().setZkWrapper(zkServerWrapper).build());
      }
      String[] clusterNames = new String[options.getNumberOfClusters()];
      Map<String, String> clusterToD2 = new HashMap<>();
      Map<String, String> clusterToServerD2 = new HashMap<>();
      for (int i = 0; i < options.getNumberOfClusters(); i++) {
        String clusterName =
            options.isRandomizeClusterName() ? Utils.getUniqueString("venice-cluster" + i) : "venice-cluster" + i;
        clusterNames[i] = clusterName;
        String d2ServiceName = "venice-" + i;
        clusterToD2.put(clusterName, d2ServiceName);
        String serverD2ServiceName = Utils.getUniqueString(clusterName + "_d2");
        clusterToServerD2.put(clusterName, serverD2ServiceName);
      }

      // Create controllers for multi-cluster
      Properties controllerProperties = options.getChildControllerProperties();
      if (options.isMultiRegionSetup()
          && !controllerProperties.containsKey(CONTROLLER_ENABLE_BATCH_PUSH_FROM_ADMIN_IN_CHILD)) {
        // In multi-region setup, we don't allow batch push to each individual child region, but just parent region
        controllerProperties.put(CONTROLLER_ENABLE_BATCH_PUSH_FROM_ADMIN_IN_CHILD, "false");
      }
      if (options.getRegionName() != null) {
        controllerProperties.setProperty(LOCAL_REGION_NAME, options.getRegionName());
      }

      // Setup D2 for controller
      String zkAddress = zkServerWrapper.getAddress();
      D2TestUtils.setupD2Config(
          zkAddress,
          false,
          VeniceControllerWrapper.D2_CLUSTER_NAME,
          VeniceControllerWrapper.D2_SERVICE_NAME);
      D2Client clientConfigD2Client = D2TestUtils.getAndStartD2Client(zkAddress);
      controllerProperties.put(
          VeniceServerWrapper.CLIENT_CONFIG_FOR_CONSUMER,
          ClientConfig.defaultGenericClientConfig("")
              .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
              .setD2Client(clientConfigD2Client));
      VeniceControllerCreateOptions controllerCreateOptions =
          new VeniceControllerCreateOptions.Builder(clusterNames, zkServerWrapper, pubSubBrokerWrapper)
              .regionName(options.getRegionName())
              .replicationFactor(options.getReplicationFactor())
              .partitionSize(options.getPartitionSize())
              .rebalanceDelayMs(options.getRebalanceDelayMs())
              .minActiveReplica(options.getMinActiveReplica())
              .clusterToD2(clusterToD2)
              .clusterToServerD2(clusterToServerD2)
              .sslToKafka(false)
              .d2Enabled(true)
              .extraProperties(controllerProperties)
              .build();
      for (int i = 0; i < options.getNumberOfControllers(); i++) {
        VeniceControllerWrapper controllerWrapper = ServiceFactory.getVeniceController(controllerCreateOptions);
        controllerMap.put(controllerWrapper.getPort(), controllerWrapper);
      }
      // Specify the system store cluster name
      Properties extraProperties = options.getVeniceProperties().toProperties();
      extraProperties.put(SYSTEM_SCHEMA_CLUSTER_NAME, clusterNames[0]);
      extraProperties.putAll(KafkaSSLUtils.getLocalCommonKafkaSSLConfig());
      VeniceClusterCreateOptions.Builder vccBuilder =
          new VeniceClusterCreateOptions.Builder().regionName(options.getRegionName())
              .standalone(false)
              .zkServerWrapper(zkServerWrapper)
              .kafkaBrokerWrapper(pubSubBrokerWrapper)
              .clusterToD2(clusterToD2)
              .clusterToServerD2(clusterToServerD2)
              .numberOfControllers(0)
              .numberOfServers(options.getNumberOfServers())
              .numberOfRouters(options.getNumberOfRouters())
              .replicationFactor(options.getReplicationFactor())
              .partitionSize(options.getPartitionSize())
              .enableAllowlist(options.isEnableAllowlist())
              .enableAutoJoinAllowlist(options.isEnableAutoJoinAllowlist())
              .rebalanceDelayMs(options.getRebalanceDelayMs())
              .minActiveReplica(options.getMinActiveReplica())
              .sslToStorageNodes(options.isSslToStorageNodes())
              .extraProperties(extraProperties)
              .forkServer(options.isForkServer())
              .kafkaClusterMap(options.getKafkaClusterMap());

      for (int i = 0; i < options.getNumberOfClusters(); i++) {
        // Create a wrapper for cluster without controller.
        vccBuilder.clusterName(clusterNames[i]);
        VeniceClusterWrapper clusterWrapper = ServiceFactory.getVeniceCluster(vccBuilder.build());
        controllerMap.values().forEach(clusterWrapper::addVeniceControllerWrapper);
        clusterWrapperMap.put(clusterWrapper.getClusterName(), clusterWrapper);
        clusterWrapper.setExternalControllerDiscoveryURL(
            controllerMap.values()
                .stream()
                .map(VeniceControllerWrapper::getControllerUrl)
                .collect(Collectors.joining(",")));
      }
      final ZkServerWrapper finalZkServerWrapper = zkServerWrapper;
      final PubSubBrokerWrapper finalPubSubBrokerWrapper = pubSubBrokerWrapper;
      return (serviceName) -> new VeniceMultiClusterWrapper(
          null,
          finalZkServerWrapper,
          finalPubSubBrokerWrapper,
          clusterWrapperMap,
          controllerMap,
          clusterToD2,
          clientConfigD2Client,
          options.getRegionName());
    } catch (Exception e) {
      controllerMap.values().forEach(Utils::closeQuietlyWithErrorLogged);
      clusterWrapperMap.values().forEach(Utils::closeQuietlyWithErrorLogged);
      Utils.closeQuietlyWithErrorLogged(pubSubBrokerWrapper);
      Utils.closeQuietlyWithErrorLogged(zkServerWrapper);
      throw e;
    }
  }

  @Override
  public String getHost() {
    throw new VeniceException("Not applicable since this is a whole cluster of many different services.");
  }

  @Override
  public int getPort() {
    throw new VeniceException("Not applicable since this is a whole cluster of many different services.");
  }

  @Override
  public String getComponentTagForLogging() {
    return new StringBuilder(getComponentTagPrefix(regionName)).append(getServiceName()).toString();
  }

  @Override
  protected void internalStart() throws Exception {
    // Everything should already be started. So this is a no-op.
  }

  @Override
  protected void internalStop() throws Exception {
    controllers.values().forEach(IOUtils::closeQuietly);
    clusters.values().forEach(IOUtils::closeQuietly);
    if (clientConfigD2Client != null) {
      D2ClientUtils.shutdownClient(clientConfigD2Client);
    }
    IOUtils.closeQuietly(pubSubBrokerWrapper);
    IOUtils.closeQuietly(zkServerWrapper);
  }

  @Override
  protected void newProcess() throws Exception {
    throw new UnsupportedOperationException("Cluster does not support to create new process.");
  }

  public Map<String, VeniceClusterWrapper> getClusters() {
    return clusters;
  }

  public Map<Integer, VeniceControllerWrapper> getControllers() {
    return controllers;
  }

  public ZkServerWrapper getZkServerWrapper() {
    return zkServerWrapper;
  }

  public PubSubBrokerWrapper getKafkaBrokerWrapper() {
    return pubSubBrokerWrapper;
  }

  public VeniceControllerWrapper getRandomController() {
    return this.controllers.values().stream().filter(controller -> controller.isRunning()).findAny().get();
  }

  public VeniceControllerWrapper getLeaderController(String clusterName) {
    return getLeaderController(clusterName, 60 * Time.MS_PER_SECOND);
  }

  public VeniceControllerWrapper getLeaderController(String clusterName, long timeoutMs) {
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    while (System.nanoTime() < deadline) {
      for (VeniceControllerWrapper controller: controllers.values()) {
        if (controller.isRunning() && controller.isLeaderController(clusterName)) {
          return controller;
        }
      }
      Utils.sleep(Time.MS_PER_SECOND);
    }
    throw new VeniceException("Leader controller does not exist, cluster=" + clusterName);
  }

  public String getControllerConnectString() {
    StringBuilder connectStr = new StringBuilder("");
    for (VeniceControllerWrapper controllerWrapper: controllers.values()) {
      connectStr.append(controllerWrapper.getControllerUrl());
      connectStr.append(',');
    }
    if (connectStr.length() != 0) {
      connectStr.deleteCharAt(connectStr.length() - 1);
    }
    return connectStr.toString();
  }

  public String[] getClusterNames() {
    return clusters.keySet().toArray(new String[clusters.keySet().size()]);
  }

  public Map<String, String> getClusterToD2() {
    return clusterToD2;
  }

  public void restartControllers() {
    controllers.values().forEach(veniceControllerWrapper -> {
      try {
        veniceControllerWrapper.stop();
        veniceControllerWrapper.restart();
      } catch (Exception e) {
        throw new VeniceException("Can not restart controller " + veniceControllerWrapper.getControllerUrl(), e);
      }
    });
  }

  public void removeOneController() {
    if (controllers.size() > 1) {
      VeniceControllerWrapper controllerWrapper = controllers.values().stream().findFirst().get();
      controllerWrapper.close();
      controllers.remove(controllerWrapper.getPort());
    }
  }

  public String getRegionName() {
    return regionName;
  }
}
