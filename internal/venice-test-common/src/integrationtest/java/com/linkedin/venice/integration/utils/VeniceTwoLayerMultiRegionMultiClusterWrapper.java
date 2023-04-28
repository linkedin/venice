package com.linkedin.venice.integration.utils;

import static com.linkedin.venice.ConfigKeys.ACTIVE_ACTIVE_REAL_TIME_SOURCE_FABRIC_LIST;
import static com.linkedin.venice.ConfigKeys.ADMIN_TOPIC_REMOTE_CONSUMPTION_ENABLED;
import static com.linkedin.venice.ConfigKeys.ADMIN_TOPIC_SOURCE_REGION;
import static com.linkedin.venice.ConfigKeys.AGGREGATE_REAL_TIME_SOURCE_REGION;
import static com.linkedin.venice.ConfigKeys.CHILD_DATA_CENTER_KAFKA_URL_PREFIX;
import static com.linkedin.venice.ConfigKeys.ENABLE_NATIVE_REPLICATION_AS_DEFAULT_FOR_BATCH_ONLY;
import static com.linkedin.venice.ConfigKeys.ENABLE_NATIVE_REPLICATION_AS_DEFAULT_FOR_HYBRID;
import static com.linkedin.venice.ConfigKeys.ENABLE_NATIVE_REPLICATION_AS_DEFAULT_FOR_INCREMENTAL_PUSH;
import static com.linkedin.venice.ConfigKeys.ENABLE_NATIVE_REPLICATION_FOR_BATCH_ONLY;
import static com.linkedin.venice.ConfigKeys.ENABLE_NATIVE_REPLICATION_FOR_HYBRID;
import static com.linkedin.venice.ConfigKeys.ENABLE_NATIVE_REPLICATION_FOR_INCREMENTAL_PUSH;
import static com.linkedin.venice.ConfigKeys.KAFKA_CLUSTER_MAP_KEY_NAME;
import static com.linkedin.venice.ConfigKeys.KAFKA_CLUSTER_MAP_KEY_OTHER_URLS;
import static com.linkedin.venice.ConfigKeys.KAFKA_CLUSTER_MAP_KEY_URL;
import static com.linkedin.venice.ConfigKeys.KAFKA_CLUSTER_MAP_SECURITY_PROTOCOL;
import static com.linkedin.venice.ConfigKeys.KAFKA_SECURITY_PROTOCOL;
import static com.linkedin.venice.ConfigKeys.NATIVE_REPLICATION_FABRIC_ALLOWLIST;
import static com.linkedin.venice.ConfigKeys.NATIVE_REPLICATION_SOURCE_FABRIC_AS_DEFAULT_FOR_BATCH_ONLY_STORES;
import static com.linkedin.venice.ConfigKeys.NATIVE_REPLICATION_SOURCE_FABRIC_AS_DEFAULT_FOR_HYBRID_STORES;
import static com.linkedin.venice.ConfigKeys.NATIVE_REPLICATION_SOURCE_FABRIC_AS_DEFAULT_FOR_INCREMENTAL_PUSH_STORES;
import static com.linkedin.venice.ConfigKeys.PARENT_KAFKA_CLUSTER_FABRIC_LIST;
import static com.linkedin.venice.ConfigKeys.PARTICIPANT_MESSAGE_STORE_ENABLED;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
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
import org.apache.kafka.common.protocol.SecurityProtocol;
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
      int numberOfRegions,
      int numberOfClustersInEachRegion,
      int numberOfParentControllers,
      int numberOfControllers,
      int numberOfServers,
      int numberOfRouters,
      int replicationFactor,
      Optional<VeniceProperties> parentControllerProperties,
      Optional<VeniceProperties> serverProperties) {
    return generateService(
        numberOfRegions,
        numberOfClustersInEachRegion,
        numberOfParentControllers,
        numberOfControllers,
        numberOfServers,
        numberOfRouters,
        replicationFactor,
        parentControllerProperties,
        Optional.empty(),
        serverProperties,
        false);
  }

  static ServiceProvider<VeniceTwoLayerMultiRegionMultiClusterWrapper> generateService(
      int numberOfRegions,
      int numberOfClustersInEachRegion,
      int numberOfParentControllers,
      int numberOfControllers,
      int numberOfServers,
      int numberOfRouters,
      int replicationFactor,
      Optional<VeniceProperties> parentControllerPropertiesOverride,
      Optional<Properties> childControllerPropertiesOverride,
      Optional<VeniceProperties> serverProperties,
      boolean forkServer) {
    String parentRegionName = VeniceControllerWrapper.DEFAULT_PARENT_DATA_CENTER_REGION_NAME;
    final List<VeniceControllerWrapper> parentControllers = new ArrayList<>(numberOfParentControllers);
    final List<VeniceMultiClusterWrapper> multiClusters = new ArrayList<>(numberOfRegions);

    /**
     * Enable participant system store by default in a two-layer multi-region set-up
     */
    Properties defaultParentControllerProps = new Properties();
    defaultParentControllerProps.setProperty(PARTICIPANT_MESSAGE_STORE_ENABLED, "true");
    defaultParentControllerProps.setProperty(ADMIN_TOPIC_REMOTE_CONSUMPTION_ENABLED, "false");

    ZkServerWrapper zkServer = null;
    PubSubBrokerWrapper parentKafka = null;
    List<PubSubBrokerWrapper> allKafkaBrokers = new ArrayList<>();

    try {
      zkServer = ServiceFactory.getZkServer();
      parentKafka = ServiceFactory.getPubSubBroker(new PubSubBrokerConfigs.Builder().setZkWrapper(zkServer).build());
      allKafkaBrokers.add(parentKafka);

      Properties parentControllerProps = parentControllerPropertiesOverride.isPresent()
          ? parentControllerPropertiesOverride.get().getPropertiesCopy()
          : new Properties();
      /** Enable participant system store by default in a two-layer multi-region set-up */
      parentControllerProps.setProperty(PARTICIPANT_MESSAGE_STORE_ENABLED, "true");
      parentControllerPropertiesOverride = Optional.of(new VeniceProperties(parentControllerProps));

      Map<String, String> clusterToD2 = new HashMap<>();
      Map<String, String> clusterToServerD2 = new HashMap<>();
      String[] clusterNames = new String[numberOfClustersInEachRegion];
      for (int i = 0; i < numberOfClustersInEachRegion; i++) {
        String clusterName = "venice-cluster" + i;
        clusterNames[i] = clusterName;
        String routerD2ServiceName = "venice-" + i;
        clusterToD2.put(clusterName, routerD2ServiceName);
        String serverD2ServiceName = Utils.getUniqueString(clusterName + "_d2");
        clusterToServerD2.put(clusterName, serverD2ServiceName);
      }
      List<String> childRegionName = new ArrayList<>(numberOfRegions);

      for (int i = 0; i < numberOfRegions; i++) {
        childRegionName.add("dc-" + i);
      }

      String childRegionList = String.join(",", childRegionName);

      /**
       * Need to build Zk servers and Kafka brokers first since they are building blocks of a Venice cluster. In other
       * words, building the remaining part of a Venice cluster sometimes requires knowledge of all Kafka brokers/clusters
       * and or Zookeeper servers.
       */
      Map<String, ZkServerWrapper> zkServerByRegionName = new HashMap<>(childRegionName.size());
      Map<String, PubSubBrokerWrapper> kafkaBrokerByRegionName = new HashMap<>(childRegionName.size());

      defaultParentControllerProps.put(ENABLE_NATIVE_REPLICATION_FOR_BATCH_ONLY, true);
      defaultParentControllerProps.put(ENABLE_NATIVE_REPLICATION_AS_DEFAULT_FOR_BATCH_ONLY, true);
      defaultParentControllerProps.put(ENABLE_NATIVE_REPLICATION_FOR_INCREMENTAL_PUSH, true);
      defaultParentControllerProps.put(ENABLE_NATIVE_REPLICATION_AS_DEFAULT_FOR_INCREMENTAL_PUSH, true);
      defaultParentControllerProps.put(ENABLE_NATIVE_REPLICATION_FOR_HYBRID, true);
      defaultParentControllerProps.put(ENABLE_NATIVE_REPLICATION_AS_DEFAULT_FOR_HYBRID, true);
      defaultParentControllerProps
          .put(NATIVE_REPLICATION_SOURCE_FABRIC_AS_DEFAULT_FOR_BATCH_ONLY_STORES, childRegionName.get(0));
      defaultParentControllerProps
          .put(NATIVE_REPLICATION_SOURCE_FABRIC_AS_DEFAULT_FOR_HYBRID_STORES, childRegionName.get(0));
      defaultParentControllerProps
          .put(NATIVE_REPLICATION_SOURCE_FABRIC_AS_DEFAULT_FOR_INCREMENTAL_PUSH_STORES, childRegionName.get(0));
      defaultParentControllerProps.put(AGGREGATE_REAL_TIME_SOURCE_REGION, parentRegionName);
      defaultParentControllerProps.put(NATIVE_REPLICATION_FABRIC_ALLOWLIST, childRegionList + "," + parentRegionName);

      final Properties finalParentControllerProperties = new Properties();
      finalParentControllerProperties.putAll(defaultParentControllerProps);
      parentControllerPropertiesOverride.ifPresent(p -> finalParentControllerProperties.putAll(p.getPropertiesCopy()));

      Properties nativeReplicationRequiredChildControllerProps = new Properties();
      nativeReplicationRequiredChildControllerProps.put(ADMIN_TOPIC_SOURCE_REGION, parentRegionName);
      nativeReplicationRequiredChildControllerProps.put(PARENT_KAFKA_CLUSTER_FABRIC_LIST, parentRegionName);
      nativeReplicationRequiredChildControllerProps
          .put(CHILD_DATA_CENTER_KAFKA_URL_PREFIX + "." + parentRegionName, parentKafka.getAddress());
      for (String regionName: childRegionName) {
        ZkServerWrapper zkServerWrapper = ServiceFactory.getZkServer();
        PubSubBrokerWrapper pubSubBrokerWrapper =
            ServiceFactory.getPubSubBroker(new PubSubBrokerConfigs.Builder().setZkWrapper(zkServerWrapper).build());
        allKafkaBrokers.add(pubSubBrokerWrapper);
        zkServerByRegionName.put(regionName, zkServerWrapper);
        kafkaBrokerByRegionName.put(regionName, pubSubBrokerWrapper);
        nativeReplicationRequiredChildControllerProps
            .put(CHILD_DATA_CENTER_KAFKA_URL_PREFIX + "." + regionName, pubSubBrokerWrapper.getAddress());
      }
      Properties activeActiveRequiredChildControllerProps = new Properties();
      activeActiveRequiredChildControllerProps.put(ACTIVE_ACTIVE_REAL_TIME_SOURCE_FABRIC_LIST, childRegionList);

      Properties defaultChildControllerProps = new Properties();
      defaultChildControllerProps.putAll(finalParentControllerProperties);
      defaultChildControllerProps.putAll(nativeReplicationRequiredChildControllerProps);
      defaultChildControllerProps.putAll(activeActiveRequiredChildControllerProps);
      defaultChildControllerProps.setProperty(PARTICIPANT_MESSAGE_STORE_ENABLED, "true");
      defaultChildControllerProps.setProperty(ADMIN_TOPIC_REMOTE_CONSUMPTION_ENABLED, "true");

      final Properties finalChildControllerProperties = new Properties();
      finalChildControllerProperties.putAll(defaultChildControllerProps);
      childControllerPropertiesOverride.ifPresent(finalChildControllerProperties::putAll);

      Map<String, Map<String, String>> kafkaClusterMap =
          addKafkaClusterIDMappingToServerConfigs(serverProperties, childRegionName, allKafkaBrokers);

      VeniceMultiClusterCreateOptions.Builder builder =
          new VeniceMultiClusterCreateOptions.Builder(numberOfClustersInEachRegion)
              .numberOfControllers(numberOfControllers)
              .numberOfServers(numberOfServers)
              .numberOfRouters(numberOfRouters)
              .replicationFactor(replicationFactor)
              .randomizeClusterName(false)
              .multiRegionSetup(true)
              .childControllerProperties(finalChildControllerProperties)
              .veniceProperties(serverProperties.orElse(null))
              .forkServer(forkServer)
              .kafkaClusterMap(kafkaClusterMap);
      // Create multi-clusters
      for (int i = 0; i < numberOfRegions; i++) {
        String regionName = childRegionName.get(i);
        builder.regionName(regionName)
            .kafkaBrokerWrapper(kafkaBrokerByRegionName.get(regionName))
            .zkServerWrapper(zkServerByRegionName.get(regionName));
        VeniceMultiClusterWrapper multiClusterWrapper = ServiceFactory.getVeniceMultiClusterWrapper(builder.build());
        multiClusters.add(multiClusterWrapper);
      }

      VeniceControllerWrapper[] childControllers = multiClusters.stream()
          .map(VeniceMultiClusterWrapper::getRandomController)
          .toArray(VeniceControllerWrapper[]::new);

      // Setup D2 for parent controller
      D2TestUtils.setupD2Config(
          zkServer.getAddress(),
          false,
          VeniceControllerWrapper.PARENT_D2_CLUSTER_NAME,
          VeniceControllerWrapper.PARENT_D2_SERVICE_NAME);
      VeniceControllerCreateOptions options =
          new VeniceControllerCreateOptions.Builder(clusterNames, zkServer, parentKafka)
              .replicationFactor(replicationFactor)
              .childControllers(childControllers)
              .extraProperties(finalParentControllerProperties)
              .clusterToD2(clusterToD2)
              .clusterToServerD2(clusterToServerD2)
              .build();
      // Create parentControllers for multi-cluster
      for (int i = 0; i < numberOfParentControllers; i++) {
        // random controller from each multi-cluster, in reality this should include all controllers, not just one
        VeniceControllerWrapper parentController = ServiceFactory.getVeniceController(options);
        parentControllers.add(parentController);
      }

      final ZkServerWrapper finalZkServer = zkServer;
      final PubSubBrokerWrapper finalParentKafka = parentKafka;

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
      IOUtils.closeQuietly(parentKafka);
      IOUtils.closeQuietly(zkServer);
      throw e;
    }
  }

  private static Map<String, Map<String, String>> addKafkaClusterIDMappingToServerConfigs(
      Optional<VeniceProperties> serverProperties,
      List<String> regionNames,
      List<PubSubBrokerWrapper> kafkaBrokers) {
    if (serverProperties.isPresent()) {
      SecurityProtocol baseSecurityProtocol = SecurityProtocol
          .valueOf(serverProperties.get().getString(KAFKA_SECURITY_PROTOCOL, SecurityProtocol.PLAINTEXT.name));
      Map<String, Map<String, String>> kafkaClusterMap = new HashMap<>();

      Map<String, String> mapping;
      for (int i = 1; i <= regionNames.size(); i++) {
        mapping = new HashMap<>();
        int clusterId = i - 1;
        mapping.put(KAFKA_CLUSTER_MAP_KEY_NAME, regionNames.get(clusterId));
        SecurityProtocol securityProtocol = baseSecurityProtocol;
        if (clusterId > 0) {
          // Testing mixed security on any 2-layer setup with 2 or more DCs.
          securityProtocol = SecurityProtocol.SSL;
        }
        mapping.put(KAFKA_CLUSTER_MAP_SECURITY_PROTOCOL, securityProtocol.name);

        // N.B. the first Kafka broker in the list is the parent, which we're excluding from the mapping, so this
        // is why the index here is offset by 1 compared to the cluster ID.
        PubSubBrokerWrapper pubSubBrokerWrapper = kafkaBrokers.get(i);
        String kafkaAddress = securityProtocol == SecurityProtocol.SSL
            ? pubSubBrokerWrapper.getSSLAddress()
            : pubSubBrokerWrapper.getAddress();
        mapping.put(KAFKA_CLUSTER_MAP_KEY_URL, kafkaAddress);
        String otherKafkaAddress = securityProtocol == SecurityProtocol.PLAINTEXT
            ? pubSubBrokerWrapper.getSSLAddress()
            : pubSubBrokerWrapper.getAddress();
        mapping.put(KAFKA_CLUSTER_MAP_KEY_OTHER_URLS, otherKafkaAddress);
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
            clusterWrapper.getKafka());
        LOGGER.info("--> broker: {}", clusterWrapper.getKafka());
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
