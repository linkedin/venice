package com.linkedin.venice.integration.utils;

import static com.linkedin.venice.ConfigKeys.CONTROLLER_ENABLE_REAL_TIME_TOPIC_VERSIONING;
import static com.linkedin.venice.ConfigKeys.LOCAL_REGION_NAME;
import static com.linkedin.venice.ConfigKeys.PARTICIPANT_MESSAGE_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.SYSTEM_SCHEMA_CLUSTER_NAME;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.LogContext;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class VeniceMultiClusterWrapper extends ProcessWrapper {
  private static final Logger LOGGER = LogManager.getLogger(VeniceMultiClusterWrapper.class);
  public static final String SERVICE_NAME = "VeniceMultiCluster";
  private final Map<String, VeniceClusterWrapper> clusters;
  private final Map<Integer, VeniceControllerWrapper> controllers;
  private final ZkServerWrapper zkServerWrapper;
  private final PubSubBrokerWrapper pubSubBrokerWrapper;
  private final Map<String, String> pubBrokerDetails;
  private final Map<String, String> clusterToD2;
  private final D2Client clientConfigD2Client;
  private final String regionName;

  VeniceMultiClusterWrapper(
      File dataDirectory,
      ZkServerWrapper zkServerWrapper,
      PubSubBrokerWrapper pubSubBrokerWrapper,
      Map<String, String> pubBrokerDetails,
      Map<String, VeniceClusterWrapper> clusters,
      Map<Integer, VeniceControllerWrapper> controllers,
      Map<String, String> clusterToD2,
      D2Client clientConfigD2Client,
      String regionName) {
    super(SERVICE_NAME, dataDirectory);
    this.zkServerWrapper = zkServerWrapper;
    this.pubSubBrokerWrapper = pubSubBrokerWrapper;
    this.pubBrokerDetails = pubBrokerDetails;
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
    Map<String, D2Client> d2Clients = options.getD2Clients();

    try {
      if (zkServerWrapper == null) {
        zkServerWrapper = ServiceFactory.getZkServer();
      }

      // Set local d2Client for the cluster.
      String regionName = options.getRegionName();
      if (d2Clients == null) {
        if (regionName == null || regionName.isEmpty()) {
          regionName = VeniceClusterWrapperConstants.STANDALONE_REGION_NAME;
        }
        d2Clients = new HashMap<>();
      }
      d2Clients.put(regionName, D2TestUtils.getAndStartD2Client(zkServerWrapper.getAddress()));

      IntegrationTestUtils.ensureZkPathExists(zkServerWrapper.getAddress(), options.getVeniceZkBasePath());
      if (pubSubBrokerWrapper == null) {
        pubSubBrokerWrapper = ServiceFactory.getPubSubBroker(
            new PubSubBrokerConfigs.Builder().setZkWrapper(zkServerWrapper)
                .setRegionName(options.getRegionName())
                .build());
      } else if (!pubSubBrokerWrapper.getRegionName().equals(options.getRegionName())) {
        throw new RuntimeException(
            "PubSubBrokerWrapper region name " + pubSubBrokerWrapper.getRegionName()
                + " does not match with the region name " + options.getRegionName() + " in the options");
      }
      Map<String, String> pubBrokerDetails =
          PubSubBrokerWrapper.getBrokerDetailsForClients(Collections.singletonList(pubSubBrokerWrapper));
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
      pubBrokerDetails.forEach((key, value) -> controllerProperties.putIfAbsent(key, value));
      VeniceControllerCreateOptions controllerCreateOptions =
          new VeniceControllerCreateOptions.Builder(clusterNames, zkServerWrapper, pubSubBrokerWrapper, d2Clients)
              .multiRegion(options.isMultiRegion())
              .regionName(options.getRegionName())
              .veniceZkBasePath(options.getVeniceZkBasePath())
              .replicationFactor(options.getReplicationFactor())
              .partitionSize(options.getPartitionSize())
              .rebalanceDelayMs(options.getRebalanceDelayMs())
              .clusterToD2(clusterToD2)
              .clusterToServerD2(clusterToServerD2)
              .sslToKafka(false)
              .d2Enabled(true)
              .dynamicAccessController(options.getAccessController())
              .extraProperties(controllerProperties)
              .build();
      for (int i = 0; i < options.getNumberOfControllers(); i++) {
        VeniceControllerWrapper controllerWrapper = ServiceFactory.getVeniceController(controllerCreateOptions);
        controllerMap.put(controllerWrapper.getPort(), controllerWrapper);
      }
      // Specify the system store cluster name
      Properties extraProperties = options.getExtraProperties();
      extraProperties.setProperty(
          CONTROLLER_ENABLE_REAL_TIME_TOPIC_VERSIONING,
          VeniceClusterWrapper.CONTROLLER_ENABLE_REAL_TIME_TOPIC_VERSIONING_IN_TESTS);
      extraProperties.put(SYSTEM_SCHEMA_CLUSTER_NAME, clusterNames[0]);
      extraProperties.putAll(KafkaTestUtils.getLocalCommonKafkaSSLConfig(SslUtils.getTlsConfiguration()));
      pubBrokerDetails.forEach((key, value) -> extraProperties.putIfAbsent(key, value));
      if (controllerProperties.containsKey(PARTICIPANT_MESSAGE_STORE_ENABLED)) {
        extraProperties
            .put(PARTICIPANT_MESSAGE_STORE_ENABLED, controllerProperties.get(PARTICIPANT_MESSAGE_STORE_ENABLED));
      }

      VeniceClusterCreateOptions.Builder vccBuilder =
          new VeniceClusterCreateOptions.Builder().regionName(options.getRegionName())
              .multiRegion(options.isMultiRegion())
              .standalone(false)
              .zkServerWrapper(zkServerWrapper)
              .veniceZkBasePath(options.getVeniceZkBasePath())
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
              .sslToKafka(options.isSslToKafka())
              .sslToStorageNodes(options.isSslToStorageNodes())
              .extraProperties(extraProperties)
              .forkServer(options.isForkServer())
              .kafkaClusterMap(options.getKafkaClusterMap())
              .d2Clients(d2Clients);

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
          pubBrokerDetails,
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
  public LogContext getComponentTagForLogging() {
    return LogContext.newBuilder().setComponentName(getServiceName()).setRegionName(regionName).build();
  }

  @Override
  protected void internalStart() throws Exception {
    // Everything should already be started. So this is a no-op.
  }

  @Override
  protected void internalStop() throws Exception {
    LOGGER.info("Starting parallel shutdown of VeniceMultiClusterWrapper");
    long overallStartTime = System.currentTimeMillis();

    // Use a dedicated thread pool to avoid ForkJoinPool starvation from nested parallel shutdowns
    ExecutorService shutdownExecutor = Executors.newCachedThreadPool();
    try {
      // Step 1: Stop controllers and clusters concurrently
      // Controllers and clusters don't depend on each other during shutdown
      long controllersAndClustersTime = TimingUtils.timeOperationAndReturnDuration(
          LOGGER,
          "Step 1: Shutting down " + controllers.size() + " controllers and " + clusters.size()
              + " clusters in parallel",
          () -> {
            List<CompletableFuture<Void>> shutdownTasks = new ArrayList<>();
            // Controller shutdown tasks
            int controllerIndex = 0;
            for (VeniceControllerWrapper controller: controllers.values()) {
              final int currentIndex = controllerIndex++;
              shutdownTasks.add(CompletableFuture.runAsync(() -> {
                long startTime = System.currentTimeMillis();
                LOGGER.debug("Shutting down controller {}", currentIndex);
                IOUtils.closeQuietly(controller);
                LOGGER.debug(
                    "Completed shutdown of controller {} in {} ms",
                    currentIndex,
                    System.currentTimeMillis() - startTime);
              }, shutdownExecutor));
            }
            // Cluster shutdown tasks
            int clusterIndex = 0;
            for (Map.Entry<String, VeniceClusterWrapper> clusterEntry: clusters.entrySet()) {
              final int currentIndex = clusterIndex++;
              final String clusterName = clusterEntry.getKey();
              final VeniceClusterWrapper cluster = clusterEntry.getValue();
              shutdownTasks.add(CompletableFuture.runAsync(() -> {
                long startTime = System.currentTimeMillis();
                LOGGER.debug("Shutting down cluster {} ({})", currentIndex, clusterName);
                IOUtils.closeQuietly(cluster);
                LOGGER.debug(
                    "Completed shutdown of cluster {} ({}) in {} ms",
                    currentIndex,
                    clusterName,
                    System.currentTimeMillis() - startTime);
              }, shutdownExecutor));
            }
            CompletableFuture.allOf(shutdownTasks.toArray(new CompletableFuture[0])).join();
          });

      // Step 2: Stop D2 client and PubSub broker concurrently
      // PubSub broker depends on ZK during shutdown (Kafka calls ZooKeeperClient.waitUntilConnected),
      // so ZK must remain alive until PubSub is fully stopped.
      long d2AndPubSubTime = TimingUtils.timeOperationAndReturnDuration(
          LOGGER,
          "Step 2: Shutting down D2 client and PubSub broker in parallel",
          () -> {
            List<CompletableFuture<Void>> shutdownTasks = new ArrayList<>();
            shutdownTasks.add(CompletableFuture.runAsync(() -> {
              long startTime = System.currentTimeMillis();
              if (clientConfigD2Client != null) {
                D2ClientUtils.shutdownClient(clientConfigD2Client);
              }
              LOGGER.debug("Completed shutdown of D2 client in {} ms", System.currentTimeMillis() - startTime);
            }, shutdownExecutor));
            shutdownTasks.add(CompletableFuture.runAsync(() -> {
              long startTime = System.currentTimeMillis();
              IOUtils.closeQuietly(pubSubBrokerWrapper);
              LOGGER.debug("Completed shutdown of PubSub broker in {} ms", System.currentTimeMillis() - startTime);
            }, shutdownExecutor));
            CompletableFuture.allOf(shutdownTasks.toArray(new CompletableFuture[0])).join();
          });

      // Step 3: Stop ZooKeeper last (PubSub broker requires ZK during its shutdown)
      long zkTime = TimingUtils.timeOperationAndReturnDuration(
          LOGGER,
          "Step 3: Shutting down ZooKeeper server",
          () -> IOUtils.closeQuietly(zkServerWrapper));

      long totalShutdownTime = System.currentTimeMillis() - overallStartTime;

      // Log comprehensive timing summary
      LOGGER.info(
          "Parallel shutdown timing summary - Total: {} ms, "
              + "Controllers + Clusters: {} ms, D2 + PubSub: {} ms, ZooKeeper: {} ms",
          totalShutdownTime,
          controllersAndClustersTime,
          d2AndPubSubTime,
          zkTime);
    } finally {
      shutdownExecutor.shutdownNow();
    }
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

  /**
   * @deprecated Use {@link #getPubSubBrokerWrapper()} instead.
   */
  @Deprecated
  public PubSubBrokerWrapper getKafkaBrokerWrapper() {
    return pubSubBrokerWrapper;
  }

  public PubSubBrokerWrapper getPubSubBrokerWrapper() {
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

  public Map<String, String> getPubSubClientProperties() {
    return pubBrokerDetails;
  }
}
