package com.linkedin.venice.integration.utils;

import static com.linkedin.venice.ConfigKeys.ACTIVE_ACTIVE_REAL_TIME_SOURCE_FABRIC_LIST;
import static com.linkedin.venice.ConfigKeys.ADMIN_TOPIC_SOURCE_REGION;
import static com.linkedin.venice.ConfigKeys.AGGREGATE_REAL_TIME_SOURCE_REGION;
import static com.linkedin.venice.ConfigKeys.CHILD_DATA_CENTER_KAFKA_URL_PREFIX;
import static com.linkedin.venice.ConfigKeys.KAFKA_CLUSTER_MAP_KEY_NAME;
import static com.linkedin.venice.ConfigKeys.KAFKA_CLUSTER_MAP_KEY_OTHER_URLS;
import static com.linkedin.venice.ConfigKeys.KAFKA_CLUSTER_MAP_KEY_URL;
import static com.linkedin.venice.ConfigKeys.KAFKA_CLUSTER_MAP_SECURITY_PROTOCOL;
import static com.linkedin.venice.ConfigKeys.KAFKA_SECURITY_PROTOCOL;
import static com.linkedin.venice.ConfigKeys.NATIVE_REPLICATION_FABRIC_ALLOWLIST;
import static com.linkedin.venice.ConfigKeys.NATIVE_REPLICATION_SOURCE_FABRIC_AS_DEFAULT_FOR_BATCH_ONLY_STORES;
import static com.linkedin.venice.ConfigKeys.NATIVE_REPLICATION_SOURCE_FABRIC_AS_DEFAULT_FOR_HYBRID_STORES;
import static com.linkedin.venice.ConfigKeys.PARENT_KAFKA_CLUSTER_FABRIC_LIST;
import static com.linkedin.venice.ConfigKeys.PARTICIPANT_MESSAGE_STORE_ENABLED;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.CHILD_REGION_NAME_PREFIX;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_PARENT_DATA_CENTER_REGION_NAME;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.PubSubSecurityProtocol;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class VeniceTwoLayerMultiRegionMultiClusterWrapper extends ProcessWrapper {
  private static final Logger LOGGER = LogManager.getLogger(VeniceTwoLayerMultiRegionMultiClusterWrapper.class);
  public static final String SERVICE_NAME = "VeniceTwoLayerMultiCluster";
  private final String parentRegionName;
  private final List<String> childRegionNames;
  private final List<VeniceMultiClusterWrapper> childRegions;
  private final List<VeniceControllerWrapper> parentControllers;
  private final String[] clusterNames;
  private final ZkServerWrapper zkServerWrapper;
  private final PubSubBrokerWrapper parentPubSubBrokerWrapper;

  VeniceTwoLayerMultiRegionMultiClusterWrapper(
      File dataDirectory,
      ZkServerWrapper zkServerWrapper,
      PubSubBrokerWrapper parentPubSubBrokerWrapper,
      List<VeniceMultiClusterWrapper> childRegions,
      List<VeniceControllerWrapper> parentControllers,
      String parentRegionName,
      List<String> childRegionNames) {
    super(SERVICE_NAME, dataDirectory);
    this.zkServerWrapper = zkServerWrapper;
    this.parentPubSubBrokerWrapper = parentPubSubBrokerWrapper;
    this.parentControllers = parentControllers;
    this.childRegions = childRegions;
    this.parentRegionName = parentRegionName;
    this.childRegionNames = childRegionNames;
    this.clusterNames = childRegions.get(0).getClusterNames();
  }

  static ServiceProvider<VeniceTwoLayerMultiRegionMultiClusterWrapper> generateService(
      VeniceMultiRegionClusterCreateOptions options) {
    String parentRegionName = DEFAULT_PARENT_DATA_CENTER_REGION_NAME;
    final List<VeniceControllerWrapper> parentControllers = new ArrayList<>(options.getNumberOfParentControllers());
    final List<VeniceMultiClusterWrapper> multiClusters = new ArrayList<>(options.getNumberOfRegions());
    /**
     * Enable participant system store by default in a two-layer multi-region set-up
     */
    Properties defaultParentControllerProps = new Properties();
    defaultParentControllerProps.setProperty(PARTICIPANT_MESSAGE_STORE_ENABLED, "true");

    ZkServerWrapper zkServer = null;
    PubSubBrokerWrapper parentPubSubBrokerWrapper = null;
    List<PubSubBrokerWrapper> allPubSubBrokerWrappers = new ArrayList<>();

    try {
      zkServer = ServiceFactory.getZkServer();
      IntegrationTestUtils.ensureZkPathExists(zkServer.getAddress(), options.getParentVeniceZkBasePath());
      parentPubSubBrokerWrapper = ServiceFactory.getPubSubBroker(
          new PubSubBrokerConfigs.Builder().setZkWrapper(zkServer)
              .setRegionName(DEFAULT_PARENT_DATA_CENTER_REGION_NAME)
              .build());
      allPubSubBrokerWrappers.add(parentPubSubBrokerWrapper);

      Map<String, String> clusterToD2 = new HashMap<>();
      Map<String, String> clusterToServerD2 = new HashMap<>();
      String[] clusterNames = new String[options.getNumberOfClusters()];
      for (int i = 0; i < options.getNumberOfClusters(); i++) {
        String clusterName = "venice-cluster" + i;
        clusterNames[i] = clusterName;
        String routerD2ServiceName = "venice-" + i;
        clusterToD2.put(clusterName, routerD2ServiceName);
        String serverD2ServiceName = Utils.getUniqueString(clusterName + "_d2");
        clusterToServerD2.put(clusterName, serverD2ServiceName);
      }
      List<String> childRegionName = new ArrayList<>(options.getNumberOfRegions());

      for (int i = 0; i < options.getNumberOfRegions(); i++) {
        childRegionName.add(CHILD_REGION_NAME_PREFIX + i);
      }

      String childRegionList = String.join(",", childRegionName);

      /**
       * Need to build Zk servers and Kafka brokers first since they are building blocks of a Venice cluster. In other
       * words, building the remaining part of a Venice cluster sometimes requires knowledge of all Kafka brokers/clusters
       * and or Zookeeper servers.
       */
      Map<String, ZkServerWrapper> zkServerByRegionName = new HashMap<>(childRegionName.size());
      Map<String, PubSubBrokerWrapper> pubSubBrokerByRegionName = new HashMap<>(childRegionName.size());

      defaultParentControllerProps
          .put(NATIVE_REPLICATION_SOURCE_FABRIC_AS_DEFAULT_FOR_BATCH_ONLY_STORES, childRegionName.get(0));
      defaultParentControllerProps
          .put(NATIVE_REPLICATION_SOURCE_FABRIC_AS_DEFAULT_FOR_HYBRID_STORES, childRegionName.get(0));
      defaultParentControllerProps.put(AGGREGATE_REAL_TIME_SOURCE_REGION, parentRegionName);
      defaultParentControllerProps.put(NATIVE_REPLICATION_FABRIC_ALLOWLIST, childRegionList + "," + parentRegionName);

      final Properties finalParentControllerProperties = new Properties();
      finalParentControllerProperties.putAll(defaultParentControllerProps);
      Properties parentControllerPropsOverride = options.getParentControllerProperties();
      if (parentControllerPropsOverride != null) {
        finalParentControllerProperties.putAll(parentControllerPropsOverride);
      }

      Properties nativeReplicationRequiredChildControllerProps = new Properties();
      nativeReplicationRequiredChildControllerProps.put(ADMIN_TOPIC_SOURCE_REGION, parentRegionName);
      nativeReplicationRequiredChildControllerProps.put(PARENT_KAFKA_CLUSTER_FABRIC_LIST, parentRegionName);
      nativeReplicationRequiredChildControllerProps
          .put(CHILD_DATA_CENTER_KAFKA_URL_PREFIX + "." + parentRegionName, parentPubSubBrokerWrapper.getAddress());
      for (String regionName: childRegionName) {
        ZkServerWrapper zkServerWrapper = ServiceFactory.getZkServer();
        IntegrationTestUtils.ensureZkPathExists(zkServer.getAddress(), options.getChildVeniceZkBasePath());
        PubSubBrokerWrapper regionalPubSubBrokerWrapper = ServiceFactory.getPubSubBroker(
            new PubSubBrokerConfigs.Builder().setZkWrapper(zkServerWrapper).setRegionName(regionName).build());
        allPubSubBrokerWrappers.add(regionalPubSubBrokerWrapper);
        zkServerByRegionName.put(regionName, zkServerWrapper);
        pubSubBrokerByRegionName.put(regionName, regionalPubSubBrokerWrapper);
        nativeReplicationRequiredChildControllerProps
            .put(CHILD_DATA_CENTER_KAFKA_URL_PREFIX + "." + regionName, regionalPubSubBrokerWrapper.getAddress());
      }
      Properties activeActiveRequiredChildControllerProps = new Properties();
      activeActiveRequiredChildControllerProps.put(ACTIVE_ACTIVE_REAL_TIME_SOURCE_FABRIC_LIST, childRegionList);

      Properties defaultChildControllerProps = new Properties();
      defaultChildControllerProps.putAll(finalParentControllerProperties);
      defaultChildControllerProps.putAll(nativeReplicationRequiredChildControllerProps);
      defaultChildControllerProps.putAll(activeActiveRequiredChildControllerProps);

      final Properties finalChildControllerProperties = new Properties();
      finalChildControllerProperties.putAll(defaultChildControllerProps);
      Properties childControllerPropsOverride = options.getChildControllerProperties();
      if (childControllerPropsOverride != null) {
        finalChildControllerProperties.putAll(childControllerPropsOverride);
      }

      Map<String, Map<String, String>> kafkaClusterMap = addKafkaClusterIDMappingToServerConfigs(
          Optional.ofNullable(options.getServerProperties()),
          childRegionName,
          allPubSubBrokerWrappers);

      Map<String, String> pubSubBrokerProps = PubSubBrokerWrapper.getBrokerDetailsForClients(allPubSubBrokerWrappers);
      LOGGER.info("### PubSub broker configs: {}", pubSubBrokerProps);
      finalParentControllerProperties.putAll(pubSubBrokerProps); // parent controllers
      finalChildControllerProperties.putAll(pubSubBrokerProps); // child controllers

      Properties additionalServerProps = new Properties();
      Properties serverPropsOverride = options.getServerProperties();
      if (serverPropsOverride != null) {
        additionalServerProps.putAll(serverPropsOverride);
      }
      additionalServerProps.putAll(pubSubBrokerProps);

      VeniceMultiClusterCreateOptions.Builder builder = new VeniceMultiClusterCreateOptions.Builder().multiRegion(true)
          .veniceZkBasePath(options.getChildVeniceZkBasePath())
          .numberOfClusters(options.getNumberOfClusters())
          .numberOfControllers(options.getNumberOfChildControllers())
          .numberOfServers(options.getNumberOfServers())
          .numberOfRouters(options.getNumberOfRouters())
          .replicationFactor(options.getReplicationFactor())
          .randomizeClusterName(false)
          .childControllerProperties(finalChildControllerProperties)
          .extraProperties(additionalServerProps)
          .sslToStorageNodes(options.isSslToStorageNodes())
          .sslToKafka(options.isSslToKafka())
          .forkServer(options.isForkServer())
          .kafkaClusterMap(kafkaClusterMap);
      // Create multi-clusters
      for (int i = 0; i < options.getNumberOfRegions(); i++) {
        String regionName = childRegionName.get(i);
        builder.regionName(regionName)
            .kafkaBrokerWrapper(pubSubBrokerByRegionName.get(regionName))
            .zkServerWrapper(zkServerByRegionName.get(regionName));
        VeniceMultiClusterWrapper multiClusterWrapper = ServiceFactory.getVeniceMultiClusterWrapper(builder.build());
        multiClusters.add(multiClusterWrapper);
      }

      // random controller from each multi-cluster, in reality this should include all controllers, not just one
      VeniceControllerWrapper[] childControllers = multiClusters.stream()
          .map(VeniceMultiClusterWrapper::getRandomController)
          .toArray(VeniceControllerWrapper[]::new);

      // Setup D2 for parent controller
      D2TestUtils.setupD2Config(
          zkServer.getAddress(),
          false,
          VeniceControllerWrapper.PARENT_D2_CLUSTER_NAME,
          VeniceControllerWrapper.PARENT_D2_SERVICE_NAME);
      VeniceControllerCreateOptions parentControllerCreateOptions =
          new VeniceControllerCreateOptions.Builder(clusterNames, zkServer, parentPubSubBrokerWrapper).multiRegion(true)
              .veniceZkBasePath(options.getParentVeniceZkBasePath())
              .replicationFactor(options.getReplicationFactor())
              .childControllers(childControllers)
              .extraProperties(finalParentControllerProperties)
              .clusterToD2(clusterToD2)
              .clusterToServerD2(clusterToServerD2)
              .regionName(parentRegionName)
              .authorizerService(options.getParentAuthorizerService())
              .build();
      // Create parentControllers for multi-cluster
      for (int i = 0; i < options.getNumberOfParentControllers(); i++) {
        VeniceControllerWrapper parentController = ServiceFactory.getVeniceController(parentControllerCreateOptions);
        parentControllers.add(parentController);
      }

      final ZkServerWrapper finalZkServer = zkServer;
      final PubSubBrokerWrapper finalParentKafka = parentPubSubBrokerWrapper;

      return (serviceName) -> new VeniceTwoLayerMultiRegionMultiClusterWrapper(
          null,
          finalZkServer,
          finalParentKafka,
          multiClusters,
          parentControllers,
          parentRegionName,
          childRegionName);
    } catch (Exception e) {
      parentControllers.forEach(IOUtils::closeQuietly);
      multiClusters.forEach(IOUtils::closeQuietly);
      IOUtils.closeQuietly(parentPubSubBrokerWrapper);
      IOUtils.closeQuietly(zkServer);
      throw e;
    }
  }

  public static Map<String, Map<String, String>> addKafkaClusterIDMappingToServerConfigs(
      Optional<Properties> serverProperties,
      List<String> regionNames,
      List<PubSubBrokerWrapper> kafkaBrokers) {
    if (serverProperties.isPresent()) {
      PubSubSecurityProtocol baseSecurityProtocol = PubSubSecurityProtocol.valueOf(
          serverProperties.get().getProperty(KAFKA_SECURITY_PROTOCOL, PubSubSecurityProtocol.PLAINTEXT.name()));
      Map<String, Map<String, String>> kafkaClusterMap = new HashMap<>();
      Map<String, String> mapping;
      for (int i = 1; i <= regionNames.size(); i++) {
        int clusterId = i - 1;
        String regionName = regionNames.get(clusterId);
        PubSubSecurityProtocol securityProtocol = baseSecurityProtocol;
        if (clusterId > 0) {
          // Testing mixed security on any 2-layer setup with 2 or more DCs.
          securityProtocol = PubSubSecurityProtocol.SSL;
        }
        PubSubBrokerWrapper pubSubBrokerWrapper = kafkaBrokers.get(i);
        mapping = prepareKafkaClusterMappingInfo(regionName, pubSubBrokerWrapper, securityProtocol, "");
        kafkaClusterMap.put(String.valueOf(clusterId), mapping);
      }

      for (int i = 1 + regionNames.size(); i <= 2 * regionNames.size(); i++) {
        int clusterId = i - 1;
        String regionName = regionNames.get(clusterId - regionNames.size());
        PubSubBrokerWrapper pubSubBrokerWrapper = kafkaBrokers.get(i - regionNames.size());
        mapping = prepareKafkaClusterMappingInfo(
            regionName,
            pubSubBrokerWrapper,
            baseSecurityProtocol,
            Utils.SEPARATE_TOPIC_SUFFIX);
        kafkaClusterMap.put(String.valueOf(clusterId), mapping);
      }

      LOGGER.info(
          "addKafkaClusterIDMappingToServerConfigs \n\treceived broker list: \n\t\t{} \n\tand generated cluster map: \n\t\t{}",
          kafkaBrokers.stream().map(PubSubBrokerWrapper::toString).collect(Collectors.joining("\n\t\t")),
          kafkaClusterMap.entrySet().stream().map(Objects::toString).collect(Collectors.joining("\n\t\t")));
      return kafkaClusterMap;
    } else {
      return Collections.emptyMap();
    }
  }

  static Map<String, String> prepareKafkaClusterMappingInfo(
      String regionName,
      PubSubBrokerWrapper pubSubBrokerWrapper,
      PubSubSecurityProtocol securityProtocol,
      String suffix) {
    Map<String, String> mapping = new HashMap<>();
    mapping.put(KAFKA_CLUSTER_MAP_KEY_NAME, regionName + suffix);
    mapping.put(KAFKA_CLUSTER_MAP_SECURITY_PROTOCOL, securityProtocol.name());

    String kafkaAddress = securityProtocol == PubSubSecurityProtocol.SSL
        ? pubSubBrokerWrapper.getSSLAddress()
        : pubSubBrokerWrapper.getAddress();
    mapping.put(KAFKA_CLUSTER_MAP_KEY_URL, kafkaAddress + suffix);
    String otherKafkaAddress = securityProtocol == PubSubSecurityProtocol.PLAINTEXT
        ? pubSubBrokerWrapper.getSSLAddress()
        : pubSubBrokerWrapper.getAddress();
    mapping.put(KAFKA_CLUSTER_MAP_KEY_OTHER_URLS, otherKafkaAddress + suffix);
    return mapping;
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
    return getServiceName();
  }

  @Override
  protected void internalStart() throws Exception {
    // Everything should already be started. So this is a no-op.
  }

  @Override
  protected void internalStop() throws Exception {
    parentControllers.forEach(IOUtils::closeQuietly);
    childRegions.forEach(IOUtils::closeQuietly);
    IOUtils.closeQuietly(parentPubSubBrokerWrapper);
    IOUtils.closeQuietly(zkServerWrapper);
  }

  @Override
  protected void newProcess() throws Exception {
    throw new UnsupportedOperationException("Cluster does not support to create new process.");
  }

  public List<VeniceMultiClusterWrapper> getChildRegions() {
    return childRegions;
  }

  public List<VeniceControllerWrapper> getParentControllers() {
    return parentControllers;
  }

  public ZkServerWrapper getZkServerWrapper() {
    return zkServerWrapper;
  }

  public PubSubBrokerWrapper getParentKafkaBrokerWrapper() {
    return parentPubSubBrokerWrapper;
  }

  public VeniceControllerWrapper getLeaderParentControllerWithRetries(String clusterName) {
    return getLeaderParentControllerWithRetries(clusterName, 60 * Time.MS_PER_SECOND);
  }

  public VeniceControllerWrapper getLeaderParentControllerWithRetries(String clusterName, long timeoutMs) {
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    while (System.nanoTime() < deadline) {
      for (VeniceControllerWrapper controller: parentControllers) {
        if (controller.isRunning() && controller.isLeaderController(clusterName)) {
          return controller;
        }
      }
      Utils.sleep(Time.MS_PER_SECOND);
    }
    throw new VeniceException("Leader controller does not exist, cluster=" + clusterName);
  }

  public String getParentRegionName() {
    return parentRegionName;
  }

  public List<String> getChildRegionNames() {
    return childRegionNames;
  }

  public String[] getClusterNames() {
    return clusterNames;
  }

  public String getControllerConnectString() {
    return getParentControllers().stream()
        .map(controller -> controller.getControllerUrl())
        .collect(Collectors.joining(","));
  }

  public void logMultiCluster() {
    LOGGER.info(
        "--> Multiregion cluster created : parentRegion: {}, childRegions: {},"
            + " clusters: {}, zk: {}, broker: {}, controllers: {}",
        getParentRegionName(),
        getChildRegionNames(),
        Arrays.toString(getClusterNames()),
        getZkServerWrapper(),
        getParentKafkaBrokerWrapper(),
        getParentControllers().stream()
            .map(VeniceControllerWrapper::getControllerUrl)
            .collect(Collectors.joining(",")));

    for (VeniceMultiClusterWrapper childDatacenter: getChildRegions()) {
      LOGGER.info(
          "--> ChildDataCenter : name: {}, controllers: {} clusters: {}, zk: {}, broker: {}",
          childDatacenter.getRegionName(),
          childDatacenter.getControllers()
              .entrySet()
              .stream()
              .map(e -> e.getKey() + ":" + e.getValue().getControllerUrl())
              .collect(Collectors.joining(",")),
          Arrays.toString(childDatacenter.getClusterNames()),
          childDatacenter.getZkServerWrapper(),
          childDatacenter.getKafkaBrokerWrapper());
      Map<String, VeniceClusterWrapper> clusters = childDatacenter.getClusters();
      for (Map.Entry<String, VeniceClusterWrapper> clusterEntry: clusters.entrySet()) {
        VeniceClusterWrapper clusterWrapper = clusterEntry.getValue();
        LOGGER.info(
            "--> Cluster -> cluster: {}, region: {} , controller: {}, zk: {}, broker: {} ",
            clusterEntry.getKey(),
            clusterWrapper.getRegionName(),
            clusterWrapper.getAllControllersURLs(),
            clusterWrapper.getZk(),
            clusterWrapper.getPubSubBrokerWrapper());
        LOGGER.info("--> broker: {}", clusterWrapper.getPubSubBrokerWrapper());
        for (VeniceControllerWrapper controller: clusterWrapper.getVeniceControllers()) {
          LOGGER.info("--> Controller: {}", controller.getControllerUrl());
        }
        for (int i = 0; i < clusterWrapper.getVeniceServers().size(); i++) {
          VeniceServerWrapper server = clusterWrapper.getVeniceServers().get(i);
          LOGGER.info("--> Server: {}", server.getAddressForLogging());
        }
        for (VeniceRouterWrapper router: clusterWrapper.getVeniceRouters()) {
          LOGGER.info("--> Router: {}", router.getAddressForLogging());
        }
      }
    }
  }
}
