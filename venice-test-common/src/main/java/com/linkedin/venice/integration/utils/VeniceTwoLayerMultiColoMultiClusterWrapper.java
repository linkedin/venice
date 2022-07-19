package com.linkedin.venice.integration.utils;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.samza.VeniceSystemFactory;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;

import static com.linkedin.venice.ConfigKeys.*;


public class VeniceTwoLayerMultiColoMultiClusterWrapper extends ProcessWrapper {
  public static final String SERVICE_NAME = "VeniceTwoLayerMultiCluster";
  private final List<VeniceMultiClusterWrapper> clusters;
  private final List<VeniceControllerWrapper> parentControllers;
  private final ZkServerWrapper zkServerWrapper;
  private final KafkaBrokerWrapper parentKafkaBrokerWrapper;

  VeniceTwoLayerMultiColoMultiClusterWrapper(File dataDirectory, ZkServerWrapper zkServerWrapper,
      KafkaBrokerWrapper parentKafkaBrokerWrapper, List<VeniceMultiClusterWrapper> clusters,
      List<VeniceControllerWrapper> parentControllers) {
    super(SERVICE_NAME, dataDirectory);
    this.zkServerWrapper = zkServerWrapper;
    this.parentKafkaBrokerWrapper = parentKafkaBrokerWrapper;
    this.parentControllers = parentControllers;
    this.clusters = clusters;
  }

  static ServiceProvider<VeniceTwoLayerMultiColoMultiClusterWrapper> generateService(int numberOfColos,
      int numberOfClustersInEachColo, int numberOfParentControllers, int numberOfControllers, int numberOfServers,
      int numberOfRouters, int replicationFactor, Optional<VeniceProperties> parentControllerProperties,
      Optional<VeniceProperties> serverProperties) {
    return generateService(numberOfColos, numberOfClustersInEachColo, numberOfParentControllers, numberOfControllers,
        numberOfServers, numberOfRouters, replicationFactor, parentControllerProperties, Optional.empty(),
        serverProperties, false, false);
  }

  static ServiceProvider<VeniceTwoLayerMultiColoMultiClusterWrapper> generateService(int numberOfColos,
      int numberOfClustersInEachColo, int numberOfParentControllers, int numberOfControllers, int numberOfServers,
      int numberOfRouters, int replicationFactor, Optional<VeniceProperties> parentControllerPropertiesOverride,
      Optional<Properties> childControllerPropertiesOverride, Optional<VeniceProperties> serverProperties, boolean multiD2,
      boolean forkServer) {
    String parentColoName = VeniceControllerWrapper.DEFAULT_PARENT_DATA_CENTER_REGION_NAME;
    final List<VeniceControllerWrapper> parentControllers = new ArrayList<>(numberOfParentControllers);
    final List<VeniceMultiClusterWrapper> multiClusters = new ArrayList<>(numberOfColos);

    /**
     * Enable participant system store by default in a two-layer multi-colo set-up
     */
    Properties defaultParentControllerProps = new Properties();
    defaultParentControllerProps.setProperty(PARTICIPANT_MESSAGE_STORE_ENABLED, "true");
    defaultParentControllerProps.setProperty(ADMIN_TOPIC_REMOTE_CONSUMPTION_ENABLED, "false");

    ZkServerWrapper zkServer = null;
    KafkaBrokerWrapper parentKafka = null;
    List<KafkaBrokerWrapper> allKafkaBrokers = new ArrayList<>();

    try {
      zkServer = ServiceFactory.getZkServer();
      parentKafka = ServiceFactory.getKafkaBroker(zkServer);
      allKafkaBrokers.add(parentKafka);

      Properties parentControllerProps = parentControllerPropertiesOverride.isPresent()
          ? parentControllerPropertiesOverride.get().getPropertiesCopy() : new Properties();
      /** Enable participant system store by default in a two-layer multi-colo set-up */
      parentControllerProps.setProperty(PARTICIPANT_MESSAGE_STORE_ENABLED, "true");
      parentControllerPropertiesOverride = Optional.of(new VeniceProperties(parentControllerProps));

      String clusterToD2 = "";
      String[] clusterNames = new String[numberOfClustersInEachColo];
      for (int i = 0; i < numberOfClustersInEachColo; i++) {
        String clusterName = "venice-cluster" + i;
        clusterNames[i] = clusterName;
        if (multiD2) {
          clusterToD2 += clusterName + ":venice-" + i + ",";
        } else {
          clusterToD2 += TestUtils.getClusterToDefaultD2String(clusterName) + ",";
        }
      }
      clusterToD2 = clusterToD2.substring(0, clusterToD2.length() - 1);
      List<String> childColoNames = new ArrayList<>(numberOfColos);

      for (int i = 0; i < numberOfColos; i++) {
        childColoNames.add("dc-" + i);
      }

      String childColoList = String.join(",", childColoNames);

      /**
       * Need to build Zk servers and Kafka brokers first since they are building blocks of a Venice cluster. In other
       * words, building the remaining part of a Venice cluster sometimes requires knowledge of all Kafka brokers/clusters
       * and or Zookeeper servers.
       */
      Map<String, ZkServerWrapper> zkServerByColoName = new HashMap<>(childColoNames.size());
      Map<String, KafkaBrokerWrapper> kafkaBrokerByColoName = new HashMap<>(childColoNames.size());

      defaultParentControllerProps.put(ENABLE_NATIVE_REPLICATION_FOR_BATCH_ONLY, true);
      defaultParentControllerProps.put(ENABLE_NATIVE_REPLICATION_AS_DEFAULT_FOR_BATCH_ONLY, true);
      defaultParentControllerProps.put(ENABLE_NATIVE_REPLICATION_FOR_INCREMENTAL_PUSH, true);
      defaultParentControllerProps.put(ENABLE_NATIVE_REPLICATION_AS_DEFAULT_FOR_INCREMENTAL_PUSH, true);
      defaultParentControllerProps.put(ENABLE_NATIVE_REPLICATION_FOR_HYBRID, true);
      defaultParentControllerProps.put(ENABLE_NATIVE_REPLICATION_AS_DEFAULT_FOR_HYBRID, true);
      defaultParentControllerProps.put(NATIVE_REPLICATION_SOURCE_FABRIC_AS_DEFAULT_FOR_BATCH_ONLY_STORES, childColoNames.get(0));
      defaultParentControllerProps.put(NATIVE_REPLICATION_SOURCE_FABRIC_AS_DEFAULT_FOR_HYBRID_STORES, childColoNames.get(0));
      defaultParentControllerProps.put(NATIVE_REPLICATION_SOURCE_FABRIC_AS_DEFAULT_FOR_INCREMENTAL_PUSH_STORES, childColoNames.get(0));
      defaultParentControllerProps.put(AGGREGATE_REAL_TIME_SOURCE_REGION, parentColoName);
      defaultParentControllerProps.put(NATIVE_REPLICATION_FABRIC_ALLOWLIST, childColoList + "," + parentColoName);

      final Properties finalParentControllerProperties = new Properties();
      finalParentControllerProperties.putAll(defaultParentControllerProps);
      parentControllerPropertiesOverride.ifPresent(p -> finalParentControllerProperties.putAll(p.getPropertiesCopy()));

      Properties nativeReplicationRequiredChildControllerProps = new Properties();
      nativeReplicationRequiredChildControllerProps.put(ADMIN_TOPIC_SOURCE_REGION, parentColoName);
      nativeReplicationRequiredChildControllerProps.put(PARENT_KAFKA_CLUSTER_FABRIC_LIST, parentColoName);
      nativeReplicationRequiredChildControllerProps.put(CHILD_DATA_CENTER_KAFKA_URL_PREFIX + "." + parentColoName, parentKafka.getAddress());
      nativeReplicationRequiredChildControllerProps.put(CHILD_DATA_CENTER_KAFKA_ZK_PREFIX + "." + parentColoName, parentKafka.getZkAddress());
      for (String coloName : childColoNames) {
        ZkServerWrapper zkServerWrapper = ServiceFactory.getZkServer();
        KafkaBrokerWrapper kafkaBrokerWrapper = ServiceFactory.getKafkaBroker(zkServerWrapper);
        allKafkaBrokers.add(kafkaBrokerWrapper);
        zkServerByColoName.put(coloName, zkServerWrapper);
        kafkaBrokerByColoName.put(coloName, kafkaBrokerWrapper);
        nativeReplicationRequiredChildControllerProps.put(CHILD_DATA_CENTER_KAFKA_URL_PREFIX + "." + coloName, kafkaBrokerWrapper.getAddress());
        nativeReplicationRequiredChildControllerProps.put(CHILD_DATA_CENTER_KAFKA_ZK_PREFIX + "." + coloName, kafkaBrokerWrapper.getZkAddress());
      }
      Properties activeActiveRequiredChildControllerProps = new Properties();
      activeActiveRequiredChildControllerProps.put(ACTIVE_ACTIVE_REAL_TIME_SOURCE_FABRIC_LIST, childColoList);

      Properties defaultChildControllerProps = new Properties();
      defaultChildControllerProps.putAll(finalParentControllerProperties);
      defaultChildControllerProps.putAll(nativeReplicationRequiredChildControllerProps);
      defaultChildControllerProps.putAll(activeActiveRequiredChildControllerProps);
      defaultChildControllerProps.setProperty(PARTICIPANT_MESSAGE_STORE_ENABLED, "true");
      defaultChildControllerProps.setProperty(ADMIN_TOPIC_REMOTE_CONSUMPTION_ENABLED, "true");
      defaultChildControllerProps.setProperty(LF_MODEL_DEPENDENCY_CHECK_DISABLED, "false");

      final Properties finalChildControllerProperties = new Properties();
      finalChildControllerProperties.putAll(defaultChildControllerProps);
      childControllerPropertiesOverride.ifPresent(finalChildControllerProperties::putAll);

      Optional<Map<String, Map<String, String>>> kafkaClusterMap = addKafkaClusterIDMappingToServerConfigs(serverProperties, parentColoName, childColoNames, allKafkaBrokers);

      // Create multi-clusters
      for (int i = 0; i < numberOfColos; i++) {
        String coloName = childColoNames.get(i);
        VeniceMultiClusterWrapper multiClusterWrapper =
            ServiceFactory.getVeniceMultiClusterWrapper(
                    coloName,
                    kafkaBrokerByColoName.get(coloName),
                    zkServerByColoName.get(coloName),
                    numberOfClustersInEachColo,
                    numberOfControllers,
                    numberOfServers,
                    numberOfRouters,
                    replicationFactor,
                    false,
                    true,
                    multiD2,
                    Optional.of(finalChildControllerProperties),
                    serverProperties,
                    forkServer,
                    kafkaClusterMap
            );
        multiClusters.add(multiClusterWrapper);
      }

      VeniceControllerWrapper[] childControllers =
          multiClusters.stream().map(cluster -> cluster.getRandomController()).toArray(VeniceControllerWrapper[]::new);

      // Setup D2 for parent controller
      D2TestUtils.setupD2Config(parentKafka.getZkAddress(), false, D2TestUtils.CONTROLLER_CLUSTER_NAME, VeniceSystemFactory.VENICE_PARENT_D2_SERVICE, false);
      // Create parentControllers for multi-cluster
      for (int i = 0; i < numberOfParentControllers; i++) {
        // random controller from each multi-cluster, in reality this should include all controllers, not just one
        VeniceControllerWrapper parentController = ServiceFactory.getVeniceParentController(
            clusterNames, parentKafka.getZkAddress(), parentKafka, childControllers, clusterToD2, false,
            replicationFactor, new VeniceProperties(finalParentControllerProperties), Optional.empty());
        parentControllers.add(parentController);
      }

      final ZkServerWrapper finalZkServer = zkServer;
      final KafkaBrokerWrapper finalParentKafka = parentKafka;

      return (serviceName) -> new VeniceTwoLayerMultiColoMultiClusterWrapper(
          null, finalZkServer, finalParentKafka, multiClusters, parentControllers);
    } catch (Exception e) {
      parentControllers.forEach(IOUtils::closeQuietly);
      multiClusters.forEach(IOUtils::closeQuietly);
      IOUtils.closeQuietly(parentKafka);
      IOUtils.closeQuietly(zkServer);
      throw e;
    }
  }

  private static Optional<Map<String, Map<String, String>>> addKafkaClusterIDMappingToServerConfigs(
          Optional<VeniceProperties> serverProperties,
          String parentColoName,
          List<String> coloNames,
          List<KafkaBrokerWrapper> kafkaBrokers
  ) {
    if (serverProperties.isPresent()) {
      Map<String, Map<String, String>> kafkaClusterMap = new HashMap<>();

      Map<String, String> mapping = new HashMap<>();
      mapping.put(KAFKA_CLUSTER_MAP_KEY_NAME, parentColoName);
      mapping.put(KAFKA_CLUSTER_MAP_KEY_URL, kafkaBrokers.get(0).getAddress());
      kafkaClusterMap.put(String.valueOf(0), mapping);
      for (int i = 1; i <= coloNames.size(); i++) {
        mapping = new HashMap<>();
        mapping.put(KAFKA_CLUSTER_MAP_KEY_NAME, coloNames.get(i-1));
        mapping.put(KAFKA_CLUSTER_MAP_KEY_URL, kafkaBrokers.get(i).getAddress());
        kafkaClusterMap.put(String.valueOf(i), mapping);
      }
      return Optional.of(kafkaClusterMap);
    } else {
      return Optional.empty(); // Do not populate if it is Optional.empty()
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
  protected void internalStart() throws Exception {
    // Everything should already be started. So this is a no-op.
  }

  @Override
  protected void internalStop() throws Exception {
    parentControllers.forEach(IOUtils::closeQuietly);
    clusters.forEach(IOUtils::closeQuietly);
    IOUtils.closeQuietly(parentKafkaBrokerWrapper);
    IOUtils.closeQuietly(zkServerWrapper);
  }

  @Override
  protected void newProcess() throws Exception {
    throw new UnsupportedOperationException("Cluster does not support to create new process.");
  }

  public List<VeniceMultiClusterWrapper> getClusters() {
    return clusters;
  }

  public List<VeniceControllerWrapper> getParentControllers() {
    return parentControllers;
  }

  public ZkServerWrapper getZkServerWrapper() {
    return zkServerWrapper;
  }

  public KafkaBrokerWrapper getParentKafkaBrokerWrapper() {
    return parentKafkaBrokerWrapper;
  }

  public VeniceControllerWrapper getLeaderParentControllerWithRetries(String clusterName) {
    return getLeaderParentControllerWithRetries(clusterName, 60 * Time.MS_PER_SECOND);
  }

  public VeniceControllerWrapper getLeaderParentControllerWithRetries(String clusterName, long timeoutMs) {
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    while (System.nanoTime() < deadline) {
      for (VeniceControllerWrapper controller : parentControllers) {
        if (controller.isRunning() && controller.isLeaderController(clusterName)) {
          return controller;
        }
      }
      Utils.sleep(Time.MS_PER_SECOND);
    }
    throw new VeniceException("Leader controller does not exist, cluster=" + clusterName);
  }
}
