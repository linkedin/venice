package com.linkedin.venice.integration.utils;

import com.linkedin.davinci.client.AvroGenericDaVinciClient;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.authorization.AuthorizerService;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.server.AdminSparkServer;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.replication.TopicReplicator;
import com.linkedin.venice.utils.MockTime;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.ReflectUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

/**
 * A factory for generating Venice services and external service instances
 * used in integration tests.
 */
public class ServiceFactory {
  // CLASS-LEVEL STATE AND APIs

  private static final Logger LOGGER = Logger.getLogger(ZkServerWrapper.class);
  private static final VeniceProperties EMPTY_VENICE_PROPS = new VeniceProperties(new Properties());

  // Test config
  private static final int MAX_ATTEMPT = 5;
  private static final int DEFAULT_REPLICATION_FACTOR =1;
  private static final int DEFAULT_PARTITION_SIZE_BYTES = 100;
  private static final long DEFAULT_DELAYED_TO_REBALANCE_MS = 0; // By default, disable the delayed rebalance for testing.
  private static final boolean DEFAULT_SSL_TO_STORAGE_NODES = false;
  private static final boolean DEFAULT_SSL_TO_KAFKA = false;

  // Wait time to make sure all the cluster services have been started.
  // If this value is not large enough, i.e. some services have not been
  // started before clients start to interact, please increase it.
  private static final int DEFAULT_WAIT_TIME_FOR_CLUSTER_START_S = 90;

  /**
   * @return an instance of {@link ZkServerWrapper}
   */
  public static ZkServerWrapper getZkServer()  {
    return getStatefulService(ZkServerWrapper.SERVICE_NAME, ZkServerWrapper.generateService());
  }

  public static ZkServerWrapper getZkServer(int port)  {
    return getStatefulService(ZkServerWrapper.SERVICE_NAME, ZkServerWrapper.generateService(), port);
  }

  /**
   * @return an instance of {@link KafkaBrokerWrapper}
   */
  public static KafkaBrokerWrapper getKafkaBroker() {
    /**
     * Get the ZK dependency outside of the lambda, to avoid time complexity of
     * O({@value MAX_ATTEMPT} ^2) on the amount of retries. {@link #getZkServer()}
     * has its own retries, so we can assume it's reliable enough.
     */

    return getKafkaBroker(ServiceFactory.getZkServer());
  }

  public static KafkaBrokerWrapper getKafkaBroker(MockTime mockTime) {
    return getKafkaBroker(ServiceFactory.getZkServer(), Optional.of(mockTime));
  }

  static KafkaBrokerWrapper getKafkaBroker(ZkServerWrapper zkServerWrapper) {
    return getKafkaBroker(zkServerWrapper, Optional.empty());
  }

  static KafkaBrokerWrapper getKafkaBroker(ZkServerWrapper zkServerWrapper, Optional<MockTime> mockTime) {
    return getStatefulService(KafkaBrokerWrapper.SERVICE_NAME, KafkaBrokerWrapper.generateService(zkServerWrapper, mockTime));
  }

  public static MirrorMakerWrapper getKafkaMirrorMaker(KafkaBrokerWrapper sourceKafka, KafkaBrokerWrapper targetKafka) {
    return getService(MirrorMakerWrapper.SERVICE_NAME, MirrorMakerWrapper.generateService(sourceKafka, targetKafka));
  }

  public static MirrorMakerWrapper getKafkaMirrorMaker(KafkaBrokerWrapper sourceKafka, KafkaBrokerWrapper targetKafka, String whitelistForKMM) {
    return getService(MirrorMakerWrapper.SERVICE_NAME, MirrorMakerWrapper.generateService(sourceKafka, targetKafka, whitelistForKMM));
  }

  public static MirrorMakerWrapper getKafkaMirrorMaker(
      String sourceKafkaAddress,
      String targetKafkaAddress,
      String targetZkAddress,
      String topicWhitelist,
      Properties consumerProperties,
      Properties producerProperties) {
    return getService(
        MirrorMakerWrapper.SERVICE_NAME,
        MirrorMakerWrapper.generateService(
            sourceKafkaAddress,
            targetKafkaAddress,
            targetZkAddress,
            topicWhitelist,
            consumerProperties,
            producerProperties));
  }

  public static BrooklinWrapper getBrooklinWrapper(KafkaBrokerWrapper kafka){
    return getService(BrooklinWrapper.SERVICE_NAME, BrooklinWrapper.generateService(kafka));
  }

  /**
   * @return an instance of {@link com.linkedin.venice.controller.VeniceControllerService} with all default settings
   */
  public static VeniceControllerWrapper getVeniceController(String clusterName, KafkaBrokerWrapper kafkaBrokerWrapper) {
    return getVeniceController(clusterName, kafkaBrokerWrapper, false);
  }

  /**
   * @return an instance of {@link com.linkedin.venice.controller.VeniceControllerService}
   */
  public static VeniceControllerWrapper getVeniceController(String clusterName, KafkaBrokerWrapper kafkaBrokerWrapper, boolean sslToKafka) {
    return getVeniceController(clusterName, kafkaBrokerWrapper, DEFAULT_REPLICATION_FACTOR, DEFAULT_PARTITION_SIZE_BYTES,
        DEFAULT_DELAYED_TO_REBALANCE_MS, DEFAULT_REPLICATION_FACTOR, null, sslToKafka);
  }

  /**
   * @return an instance of {@link com.linkedin.venice.controller.VeniceControllerService}
   */
  public static VeniceControllerWrapper getVeniceController(String clusterName, KafkaBrokerWrapper kafkaBrokerWrapper,
      int replicaFactor, int partitionSize, long delayToRebalanceMS, int minActiveReplica,
      BrooklinWrapper brooklinWrapper, boolean sslToKafka) {
    return getVeniceController(new String[]{clusterName}, kafkaBrokerWrapper, replicaFactor, partitionSize,
        delayToRebalanceMS, minActiveReplica, brooklinWrapper, null, sslToKafka, false, new Properties());
  }

  public static VeniceControllerWrapper getVeniceController(String[] clusterNames,
      KafkaBrokerWrapper kafkaBrokerWrapper, int replicaFactor, int partitionSize, long delayToRebalanceMS,
      int minActiveReplica, BrooklinWrapper brooklinWrapper, String clusterToD2, boolean sslToKafka, boolean d2Enabled,
      Properties properties) {
    if (null != brooklinWrapper) {
      properties.put(ConfigKeys.ENABLE_TOPIC_REPLICATOR, "true");
      properties.put(TopicReplicator.TOPIC_REPLICATOR_CLASS_NAME, "com.linkedin.venice.replication.BrooklinTopicReplicator");
      properties.put(TopicReplicator.TOPIC_REPLICATOR_SOURCE_KAFKA_CLUSTER, kafkaBrokerWrapper.getAddress());
      properties.put(TopicReplicator.TOPIC_REPLICATOR_CONFIG_PREFIX + "brooklin.connection.string", brooklinWrapper.getBrooklinDmsUri());
      properties.put(TopicReplicator.TOPIC_REPLICATOR_CONFIG_PREFIX + "brooklin.application.id", TestUtils.getUniqueString("venice"));

    }
    VeniceProperties extraProperties = new VeniceProperties(properties);
    return getStatefulService(VeniceControllerWrapper.SERVICE_NAME,
        VeniceControllerWrapper.generateService(clusterNames, kafkaBrokerWrapper.getZkAddress(), kafkaBrokerWrapper, false, replicaFactor, partitionSize,
            delayToRebalanceMS, minActiveReplica, null, extraProperties, clusterToD2, sslToKafka, d2Enabled));
  }

  /**
   * @return an instance of {@link com.linkedin.venice.controller.VeniceControllerService}, which will be working in parent mode.
   */
  public static VeniceControllerWrapper getVeniceParentController(
      String clusterName,
      String zkAddress,
      KafkaBrokerWrapper kafkaBrokerWrapper,
      VeniceControllerWrapper[] childControllers,
      boolean sslToKafka) {
    return getStatefulService(
        VeniceControllerWrapper.SERVICE_NAME,
        VeniceControllerWrapper.generateService(clusterName, zkAddress, kafkaBrokerWrapper, true, DEFAULT_REPLICATION_FACTOR,
            DEFAULT_PARTITION_SIZE_BYTES, DEFAULT_DELAYED_TO_REBALANCE_MS, DEFAULT_REPLICATION_FACTOR, childControllers, EMPTY_VENICE_PROPS, sslToKafka));
  }

  public static VeniceControllerWrapper getVeniceParentController(
      String clusterName,
      String zkAddress,
      KafkaBrokerWrapper kafkaBrokerWrapper,
      VeniceControllerWrapper[] childControllers,
      VeniceProperties properties,
      boolean sslToKafka) {
    return getStatefulService(
        VeniceControllerWrapper.SERVICE_NAME,
        VeniceControllerWrapper.generateService(clusterName, zkAddress, kafkaBrokerWrapper, true, DEFAULT_REPLICATION_FACTOR,
            DEFAULT_PARTITION_SIZE_BYTES, DEFAULT_DELAYED_TO_REBALANCE_MS, DEFAULT_REPLICATION_FACTOR, childControllers, properties, sslToKafka));
  }

  public static VeniceControllerWrapper getVeniceParentController(
      String[] clusterNames,
      String zkAddress,
      KafkaBrokerWrapper kafkaBrokerWrapper,
      VeniceControllerWrapper[] childControllers,
      String clusterToD2,
      boolean sslToKafka,
      int replicationFactor) {
    return getStatefulService(
        VeniceControllerWrapper.SERVICE_NAME,
        VeniceControllerWrapper.generateService(clusterNames, zkAddress, kafkaBrokerWrapper, true, replicationFactor,
            DEFAULT_PARTITION_SIZE_BYTES, DEFAULT_DELAYED_TO_REBALANCE_MS, replicationFactor - 1, childControllers, EMPTY_VENICE_PROPS, clusterToD2, sslToKafka, true));
  }

  public static VeniceControllerWrapper getVeniceParentController(
      String[] clusterNames,
      String zkAddress,
      KafkaBrokerWrapper kafkaBrokerWrapper,
      VeniceControllerWrapper[] childControllers,
      String clusterToD2,
      boolean sslToKafka,
      int replicationFactor,
      VeniceProperties properties) {
    return getStatefulService(
        VeniceControllerWrapper.SERVICE_NAME,
        VeniceControllerWrapper.generateService(clusterNames, zkAddress, kafkaBrokerWrapper, true, replicationFactor,
            DEFAULT_PARTITION_SIZE_BYTES, DEFAULT_DELAYED_TO_REBALANCE_MS, replicationFactor - 1, childControllers, properties, clusterToD2, sslToKafka, true));
  }

  /**
   * @return an instance of {@link com.linkedin.venice.controller.VeniceControllerService} which takes an authorizerService
   * and which will be working in parent mode.
   */
  public static VeniceControllerWrapper getVeniceParentController(
      String clusterName,
      String zkAddress,
      KafkaBrokerWrapper kafkaBrokerWrapper,
      VeniceControllerWrapper[] childControllers,
      boolean sslToKafka,
      Optional<AuthorizerService> authorizerService) {
    return getStatefulService(
        VeniceControllerWrapper.SERVICE_NAME,
        VeniceControllerWrapper.generateService(clusterName, zkAddress, kafkaBrokerWrapper, true, DEFAULT_REPLICATION_FACTOR,
            DEFAULT_PARTITION_SIZE_BYTES, DEFAULT_DELAYED_TO_REBALANCE_MS, DEFAULT_REPLICATION_FACTOR, childControllers, EMPTY_VENICE_PROPS, sslToKafka, authorizerService));
  }

  /**
   * Get a running admin spark server with a passed-in {@link Admin}, good for tests that want to provide a mock admin
   * @param admin
   * @return
   */
  public static AdminSparkServer getMockAdminSparkServer(Admin admin, String cluster) {
    return getService("MockAdminSparkServer", (serviceName, port) -> {
      Set<String> clusters = new HashSet<String>();
      clusters.add(cluster);
      AdminSparkServer server = new AdminSparkServer(port, admin, new MetricsRepository(), clusters, false, Optional.empty(), false, Optional.empty());
      server.start();
      return server;
    });
  }

  public static VeniceServerWrapper getVeniceServer(String clusterName, KafkaBrokerWrapper kafkaBrokerWrapper, Properties featureProperties,
      Properties configProperties) {
    return getStatefulService(VeniceServerWrapper.SERVICE_NAME,
        VeniceServerWrapper.generateService(clusterName, kafkaBrokerWrapper, featureProperties, configProperties));
  }

  static VeniceRouterWrapper getVeniceRouter(String clusterName, KafkaBrokerWrapper kafkaBrokerWrapper,
      boolean sslToStorageNodes, Properties properties){
    return getService(VeniceRouterWrapper.SERVICE_NAME,
        VeniceRouterWrapper.generateService(clusterName, kafkaBrokerWrapper, sslToStorageNodes, null, properties));
  }

  static VeniceRouterWrapper getVeniceRouter(String clusterName, KafkaBrokerWrapper kafkaBrokerWrapper,
      boolean sslToStorageNodes, String  clusterToD2){
    return getService(VeniceRouterWrapper.SERVICE_NAME,
        VeniceRouterWrapper.generateService(clusterName, kafkaBrokerWrapper, sslToStorageNodes, clusterToD2, new Properties()));
  }

  public static MockVeniceRouterWrapper getMockVeniceRouter(String zkAddress, boolean sslToStorageNodes, Properties extraConfigs){
    return getService(
        MockVeniceRouterWrapper.SERVICE_NAME,
        MockVeniceRouterWrapper.generateService(zkAddress, sslToStorageNodes, extraConfigs));
  }

  public static MockD2ServerWrapper getMockD2Server(String serviceName) {
    return getMockD2Server(serviceName, D2TestUtils.DEFAULT_TEST_CLUSTER_NAME, D2TestUtils.DEFAULT_TEST_SERVICE_NAME);
  }

  public static MockD2ServerWrapper getMockD2Server(String serviceName, String d2ClusterName, String d2ServiceName) {
    return getService(serviceName, MockD2ServerWrapper.generateService(d2ClusterName, d2ServiceName));
  }

  /**
   * Initialize MockHttpServerWrapper, this function will setup a simple http server
   */
  public static MockHttpServerWrapper getMockHttpServer(String serviceName) {
    return getService(serviceName, MockHttpServerWrapper.generateService());
  }

  public static VeniceClusterWrapper getVeniceCluster() {
    return getVeniceCluster(DEFAULT_SSL_TO_STORAGE_NODES);
  }

  public static VeniceClusterWrapper getVeniceCluster(String clusterName) {
    return getVeniceCluster(clusterName, 1, 1, 1, DEFAULT_REPLICATION_FACTOR,
        DEFAULT_PARTITION_SIZE_BYTES,false, false, DEFAULT_DELAYED_TO_REBALANCE_MS,
        DEFAULT_REPLICATION_FACTOR - 1, DEFAULT_SSL_TO_STORAGE_NODES, DEFAULT_SSL_TO_KAFKA);
  }

  //TODO There are too many parameters and options that we used to create a venice cluster wrapper.
  //TODO need a builder pattern or option class to simply this.
  public static VeniceClusterWrapper getVeniceClusterWithKafkaSSL(boolean isKafkaOpenSSLEnabled) {
    return getVeniceCluster(TestUtils.getUniqueString("venice-cluster"), 1, 1, 1, DEFAULT_REPLICATION_FACTOR, DEFAULT_PARTITION_SIZE_BYTES,
        false, false, DEFAULT_DELAYED_TO_REBALANCE_MS, DEFAULT_REPLICATION_FACTOR-1, DEFAULT_SSL_TO_STORAGE_NODES, true, isKafkaOpenSSLEnabled);
  }

  public static VeniceClusterWrapper getVeniceCluster(boolean sslToStorageNodes) {
    return getVeniceCluster(1, 1, 1, DEFAULT_REPLICATION_FACTOR, DEFAULT_PARTITION_SIZE_BYTES, sslToStorageNodes, DEFAULT_SSL_TO_KAFKA);
  }

  /**
   * Start up a testing Venice cluster in another process.
   *
   * The reason to call this method instead of other {@link #getVeniceCluster()} methods is
   * when one wants to maximize its testing environment isolation.
   * Example usage: {@link com.linkedin.venice.benchmark.IngestionBenchmarkWithTwoProcesses}
   *
   * @param clusterInfoFilePath works as IPC to pass back the needed information to the caller process
   */
  public static void startVeniceClusterInAnotherProcess(String clusterInfoFilePath) {
    startVeniceClusterInAnotherProcess(clusterInfoFilePath, DEFAULT_WAIT_TIME_FOR_CLUSTER_START_S);
  }

  /**
   * Start up a testing Venice cluster in another process.
   *
   * The reason to call this method instead of other {@link #getVeniceCluster()} methods is
   * when one wants to maximize its testing environment isolation.
   * Example usage: {@link com.linkedin.venice.benchmark.IngestionBenchmarkWithTwoProcesses}
   *
   * @param clusterInfoFilePath works as IPC to pass back the needed information to the caller process
   * @param waitTimeInSeconds gives some wait time to make sure all the services have been started in the other process.
   *                          The default wait time is an empirical value based on observations. If we have more
   *                          components to start in the future, this value needs to be increased.
   */
  public static void startVeniceClusterInAnotherProcess(String clusterInfoFilePath, int waitTimeInSeconds) {
    try {
      VeniceClusterWrapper.generateServiceInAnotherProcess(clusterInfoFilePath, waitTimeInSeconds);
    } catch (IOException | InterruptedException e) {
      throw new VeniceException("Start Venice cluster in another process has failed", e);
    }
  }

  public static void stopVeniceClusterInAnotherProcess() {
    VeniceClusterWrapper.stopServiceInAnotherProcess();
  }

  public static VeniceClusterWrapper getVeniceCluster(int numberOfControllers, int numberOfServers, int numberOfRouter) {
    return getVeniceCluster(numberOfControllers, numberOfServers, numberOfRouter, DEFAULT_REPLICATION_FACTOR,
        DEFAULT_PARTITION_SIZE_BYTES, DEFAULT_SSL_TO_STORAGE_NODES, DEFAULT_SSL_TO_KAFKA);
  }

  public static VeniceClusterWrapper getVeniceCluster(int numberOfControllers, int numberOfServers, int numberOfRouter, int replicationFactor) {
    return getVeniceCluster(numberOfControllers, numberOfServers, numberOfRouter, replicationFactor,
        DEFAULT_PARTITION_SIZE_BYTES, DEFAULT_SSL_TO_STORAGE_NODES, DEFAULT_SSL_TO_KAFKA);
  }

  public static VeniceClusterWrapper getVeniceCluster(int numberOfControllers, int numberOfServers, int numberOfRouter,
      int replicationFactor, int partitionSize, boolean sslToStorageNodes, boolean sslToKafka, Properties extraProperties) {
    return getService(VeniceClusterWrapper.SERVICE_NAME,
        VeniceClusterWrapper.generateService(TestUtils.getUniqueString("venice-cluster"), numberOfControllers,
            numberOfServers, numberOfRouter, replicationFactor, partitionSize, false, false,
            DEFAULT_DELAYED_TO_REBALANCE_MS, replicationFactor - 1, sslToStorageNodes,
            sslToKafka, false, extraProperties));
  }

  public static VeniceClusterWrapper getVeniceCluster(int numberOfControllers, int numberOfServers, int numberOfRouter,
      int replicationFactor, int partitionSize, boolean sslToStorageNodes, boolean sslToKafka) {
    // As we introduce bootstrap state in to venice and transition from bootstrap to online will be blocked until get
    // "end of push" message. We need more venice server for testing, because there is a limitation in helix about how
    // many uncompleted transitions one server could handle. So if we still use one server and that limitation is
    // reached, venice can not create new resource which will cause failed tests.
    // Enable to start multiple controllers and routers too, so that we could fail some of them to do the failover integration test.
    return getVeniceCluster(numberOfControllers, numberOfServers, numberOfRouter, replicationFactor, partitionSize,
        false, false, DEFAULT_DELAYED_TO_REBALANCE_MS, replicationFactor - 1, sslToStorageNodes, sslToKafka);
    }

  public static VeniceClusterWrapper getVeniceCluster(int numberOfControllers, int numberOfServers, int numberOfRouter,
      int replicationFactor, int partitionSize, boolean enableWhitelist, boolean enableAutoJoinWhitelist,
      long delayToRebalanceMS, int minActiveReplica, boolean sslToStorageNodes, boolean sslToKafka) {
    return getVeniceCluster(TestUtils.getUniqueString("venice-cluster"), numberOfControllers, numberOfServers, numberOfRouter, replicationFactor, partitionSize,
        enableWhitelist, enableAutoJoinWhitelist, delayToRebalanceMS, minActiveReplica, sslToStorageNodes, sslToKafka);
  }

  public static VeniceClusterWrapper getVeniceCluster(String clusterName, int numberOfControllers, int numberOfServers, int numberOfRouter,
      int replicaFactor, int partitionSize, boolean enableWhitelist, boolean enableAutoJoinWhitelist,
      long delayToRebalanceMS, int minActiveReplica, boolean sslToStorageNodes, boolean sslToKafka) {
    return getVeniceCluster(clusterName, numberOfControllers, numberOfServers, numberOfRouter, replicaFactor, partitionSize,
        enableWhitelist, enableAutoJoinWhitelist, delayToRebalanceMS, minActiveReplica, sslToStorageNodes, sslToKafka, false);
  }

  // TODO instead of passing more and more parameters here, we could create a class ClusterOptions to include all of options to start a cluster. Then we only need one parameter here.
  // Or a builder pattern
  public static VeniceClusterWrapper getVeniceCluster(String clusterName, int numberOfControllers, int numberOfServers, int numberOfRouter,
      int replicaFactor, int partitionSize, boolean enableWhitelist, boolean enableAutoJoinWhitelist,
      long delayToRebalanceMS, int minActiveReplica, boolean sslToStorageNodes, boolean sslToKafka, boolean isKafkaOpenSSLEnabled) {
    return getService(VeniceClusterWrapper.SERVICE_NAME,
        VeniceClusterWrapper.generateService(clusterName, numberOfControllers, numberOfServers, numberOfRouter, replicaFactor,
            partitionSize, enableWhitelist, enableAutoJoinWhitelist, delayToRebalanceMS, minActiveReplica, sslToStorageNodes,
            sslToKafka, isKafkaOpenSSLEnabled, new Properties()));
  }

  protected static VeniceClusterWrapper getVeniceClusterWrapperForMultiCluster(
      ZkServerWrapper zkServerWrapper,
      KafkaBrokerWrapper kafkaBrokerWrapper,
      BrooklinWrapper brooklinWrapper,
      String clusterName,
      String clusterToD2,
      int numberOfControllers,
      int numberOfServers,
      int numberOfRouter,
      int replicaFactor,
      int partitionSize,
      boolean enableWhitelist,
      boolean enableAutoJoinWhitelist,
      long delayToRebalanceMS,
      int minActiveReplica,
      boolean sslToStorageNodes,
      boolean sslToKafka,
      Optional<VeniceProperties> veniceProperties) {
    return getService(VeniceClusterWrapper.SERVICE_NAME,
        VeniceClusterWrapper.generateService(zkServerWrapper, kafkaBrokerWrapper, brooklinWrapper, clusterName, clusterToD2,
            numberOfControllers, numberOfServers, numberOfRouter, replicaFactor, partitionSize, enableWhitelist,
            enableAutoJoinWhitelist, delayToRebalanceMS, minActiveReplica, sslToStorageNodes, sslToKafka, false,
            veniceProperties.isPresent() ? veniceProperties.get().toProperties() : new Properties()));
  }

  public static VeniceMultiClusterWrapper getVeniceMultiClusterWrapper(int numberOfClusters, int numberOfControllers,
      int numberOfServers, int numberOfRouter) {
    return getService(VeniceMultiClusterWrapper.SERVICE_NAME,
        VeniceMultiClusterWrapper.generateService(numberOfClusters, numberOfControllers,
            numberOfServers, numberOfRouter, DEFAULT_REPLICATION_FACTOR, DEFAULT_PARTITION_SIZE_BYTES, false, false,
            DEFAULT_DELAYED_TO_REBALANCE_MS, DEFAULT_REPLICATION_FACTOR - 1, DEFAULT_SSL_TO_STORAGE_NODES,
            Optional.empty(), true, false, Optional.empty(), false));
  }

  /**
   * Predictable cluster name
   */
  public static VeniceMultiClusterWrapper getVeniceMultiClusterWrapper(int numberOfClusters, int numberOfControllers,
      int numberOfServers, int numberOfRouter, boolean randomizeClusterName, boolean multiColoSetup, boolean multiD2) {
    return getService(VeniceMultiClusterWrapper.SERVICE_NAME,
        VeniceMultiClusterWrapper.generateService(numberOfClusters, numberOfControllers,
            numberOfServers, numberOfRouter, DEFAULT_REPLICATION_FACTOR, DEFAULT_PARTITION_SIZE_BYTES, false, false,
            DEFAULT_DELAYED_TO_REBALANCE_MS, DEFAULT_REPLICATION_FACTOR - 1, DEFAULT_SSL_TO_STORAGE_NODES, Optional.empty(), randomizeClusterName,
            multiColoSetup, Optional.empty(), multiD2));
  }

  /**
   * Predictable cluster name
   */
  public static VeniceMultiClusterWrapper getVeniceMultiClusterWrapper(int numberOfClusters, int numberOfControllers, int numberOfServers,
      int numberOfRouter, int replicationFactor, boolean randomizeClusterName, boolean multiColoSetup, boolean multiD2, Optional<VeniceProperties> veniceProperties) {
    return getService(VeniceMultiClusterWrapper.SERVICE_NAME,
        VeniceMultiClusterWrapper.generateService(numberOfClusters, numberOfControllers,
            numberOfServers, numberOfRouter, replicationFactor, DEFAULT_PARTITION_SIZE_BYTES, false, false,
            DEFAULT_DELAYED_TO_REBALANCE_MS, replicationFactor - 1, DEFAULT_SSL_TO_STORAGE_NODES, Optional.empty(), randomizeClusterName,
            multiColoSetup, veniceProperties, multiD2));
  }

  /**
   * Allows specific Zookeeper port for debugging
   */
  public static VeniceMultiClusterWrapper getVeniceMultiClusterWrapper(int numberOfClusters, int numberOfControllers,
      int numberOfServers, int numberOfRouter, int zkPort) {
    return getService(VeniceMultiClusterWrapper.SERVICE_NAME,
        VeniceMultiClusterWrapper.generateService(numberOfClusters, numberOfControllers,
            numberOfServers, numberOfRouter, DEFAULT_REPLICATION_FACTOR, DEFAULT_PARTITION_SIZE_BYTES, false, false,
            DEFAULT_DELAYED_TO_REBALANCE_MS, DEFAULT_REPLICATION_FACTOR - 1, DEFAULT_SSL_TO_STORAGE_NODES,
            Optional.of(zkPort), true, false, Optional.empty(), false));
  }

  public static VeniceTwoLayerMultiColoMultiClusterWrapper getVeniceTwoLayerMultiColoMultiClusterWrapper(int numberOfColos,
      int numberOfClustersInEachColo, int numberOfParentControllers, int numberOfControllers, int numberOfServers, int numberOfRouters) {
    return getService(VeniceTwoLayerMultiColoMultiClusterWrapper.SERVICE_NAME,
        VeniceTwoLayerMultiColoMultiClusterWrapper.generateService(numberOfColos, numberOfClustersInEachColo,
            numberOfParentControllers, numberOfControllers, numberOfServers, numberOfRouters, DEFAULT_REPLICATION_FACTOR, Optional.empty(), Optional.empty(), Optional.empty()));
  }

  public static VeniceTwoLayerMultiColoMultiClusterWrapper getVeniceTwoLayerMultiColoMultiClusterWrapper(int numberOfColos,
      int numberOfClustersInEachColo, int numberOfParentControllers, int numberOfControllers, int numberOfServers, int numberOfRouters, int zkPort) {
    return getService(VeniceTwoLayerMultiColoMultiClusterWrapper.SERVICE_NAME,
        VeniceTwoLayerMultiColoMultiClusterWrapper.generateService(numberOfColos, numberOfClustersInEachColo, numberOfParentControllers,
            numberOfControllers, numberOfServers, numberOfRouters, DEFAULT_REPLICATION_FACTOR, Optional.of(zkPort), Optional.empty(), Optional.empty()));
  }

  public static VeniceTwoLayerMultiColoMultiClusterWrapper getVeniceTwoLayerMultiColoMultiClusterWrapper(int numberOfColos,
      int numberOfClustersInEachColo, int numberOfParentControllers, int numberOfControllers, int numberOfServers, int numberOfRouters,
      int replicationFactor, Optional<Integer> zkPort, Optional<VeniceProperties> parentControllerProps, Optional<VeniceProperties> serverProps, boolean multiD2, String whitelistForKMM) {
    return getService(VeniceTwoLayerMultiColoMultiClusterWrapper.SERVICE_NAME,
        VeniceTwoLayerMultiColoMultiClusterWrapper.generateService(numberOfColos, numberOfClustersInEachColo, numberOfParentControllers,
            numberOfControllers, numberOfServers, numberOfRouters, replicationFactor, zkPort, parentControllerProps, serverProps, multiD2, whitelistForKMM));
  }

  public static VeniceTwoLayerMultiColoMultiClusterWrapper getVeniceTwoLayerMultiColoMultiClusterWrapper(int numberOfColos,
      int numberOfClustersInEachColo, int numberOfParentControllers, int numberOfControllers, int numberOfServers, int numberOfRouters,
      VeniceProperties parentControllerProperties, boolean multiD2) {
    return getService(VeniceTwoLayerMultiColoMultiClusterWrapper.SERVICE_NAME,
        VeniceTwoLayerMultiColoMultiClusterWrapper.generateService(numberOfColos, numberOfClustersInEachColo, numberOfParentControllers,
            numberOfControllers, numberOfServers, numberOfRouters, DEFAULT_REPLICATION_FACTOR, Optional.empty(), Optional.of(parentControllerProperties),
            Optional.empty(), multiD2, MirrorMakerWrapper.DEFAULT_TOPIC_WHITELIST));
  }

  public static HelixAsAServiceWrapper getHelixController(String zkAddress) {
    return getService(
        HelixAsAServiceWrapper.SERVICE_NAME, HelixAsAServiceWrapper.generateService(zkAddress));
  }




  private static <S extends ProcessWrapper> S getStatefulService(String serviceName, StatefulServiceProvider<S> serviceProvider, int port) {
    return getService(serviceName, serviceProvider, port);
  }

  private static <S extends ProcessWrapper> S getStatefulService(String serviceName, StatefulServiceProvider<S> serviceProvider) {
    return getService(serviceName, serviceProvider);
  }

  private static <S extends Closeable> S getService(String serviceName, ArbitraryServiceProvider<S> serviceProvider) {
    // Just some initial state. If the fabric of space-time holds up, you should never see these strings.
    Exception lastException = new VeniceException("There is no spoon.");
    String errorMessage = "If you see this message, something went horribly wrong.";

    for (int attempt = 1; attempt <= MAX_ATTEMPT; attempt++) {
      int freePort = IntegrationTestUtils.getFreePort();
      try {
        return getService(serviceName, serviceProvider, freePort);
      } catch (Exception e) {
        lastException = e;
        errorMessage = "Got " + e.getClass().getSimpleName() + " while trying to start " + serviceName +
            " with random port number " + freePort + ". Attempt #" + attempt + "/" + MAX_ATTEMPT + ".";
        LOGGER.warn(errorMessage, e);
      }
    }

    throw new VeniceException(errorMessage + " Aborting.", lastException);
  }

  private static <S extends Closeable> S getService(String serviceName, ArbitraryServiceProvider<S> serviceProvider, int port) {
    // Just some initial state. If the fabric of space-time holds up, you should never see these strings.
    Exception lastException = new VeniceException("There is no spoon.");
    String errorMessage = "If you see this message, something went horribly wrong.";

    for (int attempt = 1; attempt <= MAX_ATTEMPT; attempt++) {
      S wrapper = null;
      try {
        wrapper = serviceProvider.get(serviceName, port);

        if (wrapper instanceof ProcessWrapper) {
          LOGGER.info("Starting ProcessWrapper: " + serviceName);

          // N.B.: The contract for start() is that it should block until the wrapped service is fully started.
          ProcessWrapper processWrapper = (ProcessWrapper) wrapper;
          processWrapper.start();

          LOGGER.info("Started ProcessWrapper: " + serviceName);
        }
        return wrapper;
      } catch (NoSuchMethodError e) {
        LOGGER.error("Got a " + e.getClass().getSimpleName() + " while trying to start " + serviceName + ". Will print the jar containing the bad class and then bubble up.");
        ReflectUtils.printJarContainingBadClass(e);
        IOUtils.closeQuietly(wrapper);
        throw e;
      } catch (LinkageError e) {
        LOGGER.error("Got a " + e.getClass().getSimpleName() + " while trying to start " + serviceName + ". Will print the classpath and then bubble up.");
        ReflectUtils.printClasspath();
        IOUtils.closeQuietly(wrapper);
        throw e;
      } catch (Exception e) {
        lastException = e;
        errorMessage = "Got " + e.getClass().getSimpleName() + " while trying to start " + serviceName +
            " with given port number " + port + ". Attempt #" + attempt + "/" + MAX_ATTEMPT + ".";
        LOGGER.warn(errorMessage, e);
        IOUtils.closeQuietly(wrapper);
        // We don't throw for other exception types, since we want to retry.
      }
    }

    throw new VeniceException(errorMessage + " Aborting.", lastException);
  }

  public static <K, V> DaVinciClient<K, V> getGenericAvroDaVinciClient(String storeName, VeniceClusterWrapper cluster) {
    return getGenericAvroDaVinciClient(storeName, cluster, TestUtils.getTempDataDirectory().getAbsolutePath());
  }

  public static <K, V> DaVinciClient<K, V> getGenericAvroDaVinciClient(String storeName, VeniceClusterWrapper cluster, String dataBasePath) {
    VeniceProperties backendConfig = new PropertyBuilder()
                                         .put(ConfigKeys.DATA_BASE_PATH, dataBasePath)
                                         .put(ConfigKeys.PERSISTENCE_TYPE, PersistenceType.ROCKS_DB)
                                         .build();
    return getGenericAvroDaVinciClient(storeName, cluster, new DaVinciConfig(), backendConfig);
  }

  public static <K, V> DaVinciClient<K, V> getGenericAvroDaVinciClient(String storeName, VeniceClusterWrapper cluster, DaVinciConfig daVinciConfig, VeniceProperties backendConfig) {
    ClientConfig clientConfig = ClientConfig
        .defaultGenericClientConfig(storeName)
        .setD2ServiceName(ClientConfig.DEFAULT_D2_SERVICE_NAME)
        .setVeniceURL(cluster.getZk().getAddress());
    DaVinciClient<K, V> client = new AvroGenericDaVinciClient<>(daVinciConfig, clientConfig, backendConfig);
    client.start();
    return client;
  }

  public static <K, V> DaVinciClient<K, V> getGenericAvroDaVinciClient(String storeName, String zkAddress, String dataBasePath) {
    VeniceProperties backendConfig = new PropertyBuilder()
        .put(ConfigKeys.DATA_BASE_PATH, dataBasePath)
        .put(ConfigKeys.PERSISTENCE_TYPE, PersistenceType.ROCKS_DB)
        .build();

    ClientConfig clientConfig = ClientConfig
        .defaultGenericClientConfig(storeName)
        .setD2ServiceName(ClientConfig.DEFAULT_D2_SERVICE_NAME)
        .setVeniceURL(zkAddress);

    DaVinciClient<K, V> client = new AvroGenericDaVinciClient<>(new DaVinciConfig(), clientConfig, backendConfig);
    client.start();
    return client;
  }
}
