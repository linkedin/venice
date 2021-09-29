package com.linkedin.venice.integration.utils;

import com.linkedin.venice.authorization.AuthorizerService;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceController;
import com.linkedin.venice.controller.kafka.consumer.AdminConsumerService;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.kafka.admin.KafkaAdminClient;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.utils.KafkaSSLUtils;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.server.factory.D2Server;

import io.tehuti.metrics.MetricsRepository;
import java.util.Properties;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.integration.utils.D2TestUtils.*;


/**
 * A wrapper for the {@link VeniceControllerWrapper}.
 * <p>
 * Calling close() will clean up the controller's data directory.
 */
public class VeniceControllerWrapper extends ProcessWrapper {
  public static final Logger logger = Logger.getLogger(VeniceControllerWrapper.class);

  public static final String SERVICE_NAME = "VeniceController";
  public static final double DEFAULT_STORAGE_ENGINE_OVERHEAD_RATIO = 0.85d;
  public static final String DEFAULT_PARENT_DATA_CENTER_REGION_NAME = "dc-parent-0";

  private VeniceController service;
  private final List<VeniceProperties> configs;
  private final int port;
  private final int securePort;
  private final String zkAddress;
  private final List<D2Server> d2ServerList;
  private final MetricsRepository metricsRepository;

  private VeniceControllerWrapper(
      String serviceName,
      File dataDirectory,
      VeniceController service,
      int port,
      int securePort,
      List<VeniceProperties> configs,
      List<D2Server> d2ServerList,
      String zkAddress,
      MetricsRepository metricsRepository) {
    super(serviceName, dataDirectory);
    this.service = service;
    this.configs = configs;
    this.port = port;
    this.securePort = securePort;
    this.zkAddress = zkAddress;
    this.d2ServerList = d2ServerList;
    this.metricsRepository = metricsRepository;
  }

  static StatefulServiceProvider<VeniceControllerWrapper> generateService(
      String[] clusterNames,
      String zkAddress,
      KafkaBrokerWrapper kafkaBroker,
      boolean isParent,
      int replicationFactor,
      int partitionSize,
      long rebalanceDelayMs,
      int minActiveReplica,
      VeniceControllerWrapper[] childControllers,
      Properties extraProperties,
      String clusterToD2,
      boolean sslToKafka,
      boolean d2Enabled,
      Optional<AuthorizerService> authorizerService) {

    return (serviceName, dataDirectory) -> {
      int adminPort = Utils.getFreePort();
      int adminSecurePort = Utils.getFreePort();
      List<VeniceProperties> propertiesList = new ArrayList<>();

      VeniceProperties extraProps = new VeniceProperties(extraProperties);
      for(String clusterName : clusterNames) {
        VeniceProperties clusterProps =
            IntegrationTestUtils.getClusterProps(clusterName, dataDirectory, zkAddress, kafkaBroker, sslToKafka);

        // TODO: Validate that these configs are all still used.
        // TODO: Centralize default config values in a single place
        PropertyBuilder builder = new PropertyBuilder().put(clusterProps.toProperties())
            .put(KAFKA_REPLICATION_FACTOR, 1)
            .put(ADMIN_TOPIC_REPLICATION_FACTOR, 1)
            .put(KAFKA_ZK_ADDRESS, kafkaBroker.getZkAddress())
            .put(CONTROLLER_NAME, "venice-controller") // Why is this configurable?
            .put(DEFAULT_REPLICA_FACTOR, replicationFactor)
            .put(DEFAULT_NUMBER_OF_PARTITION, 1)
            .put(ADMIN_PORT, adminPort)
            .put(ADMIN_SECURE_PORT, adminSecurePort)
            /**
             * Running with just one partition may not fully exercise the distributed nature of the system,
             * but we do want to minimize the number as each partition results in files, connections, threads, etc.
             * in the whole system. 3 seems like a reasonable tradeoff between these concerns.
             */
            .put(DEFAULT_MAX_NUMBER_OF_PARTITIONS, 3)
            .put(DEFAULT_PARTITION_SIZE, partitionSize)
            .put(CONTROLLER_PARENT_MODE, isParent)
            .put(DELAY_TO_REBALANCE_MS, rebalanceDelayMs)
            .put(MIN_ACTIVE_REPLICA, minActiveReplica)
            .put(TOPIC_CREATION_THROTTLING_TIME_WINDOW_MS, 100)
            .put(STORAGE_ENGINE_OVERHEAD_RATIO, DEFAULT_STORAGE_ENGINE_OVERHEAD_RATIO)
            .put(CLUSTER_TO_D2,
                Utils.isNullOrEmpty(clusterToD2) ? TestUtils.getClusterToDefaultD2String(clusterName) : clusterToD2)
            .put(SSL_TO_KAFKA, sslToKafka)
            .put(SSL_KAFKA_BOOTSTRAP_SERVERS, kafkaBroker.getSSLAddress())
            .put(ENABLE_OFFLINE_PUSH_SSL_WHITELIST, false)
            .put(ENABLE_HYBRID_PUSH_SSL_WHITELIST, false)
            .put(KAFKA_BOOTSTRAP_SERVERS, kafkaBroker.getAddress())
            .put(KAFKA_REQUEST_TIMEOUT_MS, 5000)
            .put(KAFKA_DELIVERY_TIMEOUT_MS, 5000)
            .put(OFFLINE_JOB_START_TIMEOUT_MS, 60_000)
            // To speed up topic cleanup
            .put(TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS, 100)
            .put(KAFKA_ADMIN_CLASS, KafkaAdminClient.class.getName())
            .put(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB)
            // Moving from topic monitor to admin protocol for add version and starting ingestion
            .put(CONTROLLER_ADD_VERSION_VIA_ADMIN_PROTOCOL, true)
            // The first cluster will always be the one to host system schemas...
            .put(CONTROLLER_SYSTEM_SCHEMA_CLUSTER_NAME, clusterNames[0])
            .put(TOPIC_CLEANUP_SEND_CONCURRENT_DELETES_REQUESTS, false)
            .put(CONTROLLER_ZK_SHARED_META_SYSTEM_SCHEMA_STORE_AUTO_CREATION_ENABLED, true)
            .put(CONTROLLER_ZK_SHARED_DAVINCI_PUSH_STATUS_SYSTEM_SCHEMA_STORE_AUTO_CREATION_ENABLED, true)
            .put(PUSH_STATUS_STORE_ENABLED, true)
            .put(extraProps.toProperties());

        if (sslToKafka) {
          builder.put(KAFKA_SECURITY_PROTOCOL, SecurityProtocol.SSL.name);
          builder.put(KafkaSSLUtils.getLocalCommonKafkaSSLConfig());
        }

        if (!extraProps.containsKey(ENABLE_TOPIC_REPLICATOR)) {
          builder.put(ENABLE_TOPIC_REPLICATOR, false);
        }

        if (isParent) {
          // Parent controller needs config to route per-cluster requests such as job status
          // This dummy parent controller wont support such requests until we make this config configurable.
          String clusterWhiteList = "";
          if (extraProps.containsKey(CHILD_CLUSTER_WHITELIST)) {
            clusterWhiteList = extraProps.getString(CHILD_CLUSTER_WHITELIST);
          }
          for (int dataCenterIndex = 0; dataCenterIndex < childControllers.length; dataCenterIndex++) {
            String childDataCenterName = createDataCenterNameWithIndex(dataCenterIndex);
            if (!clusterWhiteList.equals("")) {
              clusterWhiteList += ",";
            }
            clusterWhiteList += childDataCenterName;
            VeniceControllerWrapper childController = childControllers[dataCenterIndex];
            if (null == childController) {
              throw new IllegalArgumentException("child controller at index " + dataCenterIndex + " is null!");
            }
            builder.put(CHILD_CLUSTER_URL_PREFIX + "." + childDataCenterName, childController.getControllerUrl());
            builder.put(CHILD_DATA_CENTER_KAFKA_URL_PREFIX + "." + childDataCenterName, childController.getKafkaBootstrapServers(sslToKafka));
            builder.put(CHILD_DATA_CENTER_KAFKA_ZK_PREFIX + "." + childDataCenterName, childController.getKafkaZkAddress());
            logger.info("ControllerConfig: " + CHILD_DATA_CENTER_KAFKA_URL_PREFIX + "." + childDataCenterName
                + " KafkaUrl: " +  childController.getKafkaBootstrapServers(sslToKafka) + " kafkaZk: " + childController.getKafkaZkAddress());
          }
          builder.put(CHILD_CLUSTER_WHITELIST, clusterWhiteList);
          /**
           * In native replication, source fabric can be child fabrics as well as parent fabric;
           * and in parent fabric, there can be more than one Kafka clusters, so we might need more
           * than one parent fabric name even though logically there is only one parent fabric.
           */
          String parentDataCenterName1 = DEFAULT_PARENT_DATA_CENTER_REGION_NAME;
          String nativeReplicationSourceFabricWhitelist = clusterWhiteList + "," + parentDataCenterName1;
          builder.put(NATIVE_REPLICATION_FABRIC_WHITELIST, nativeReplicationSourceFabricWhitelist);
          builder.put(CHILD_DATA_CENTER_KAFKA_URL_PREFIX + "." + parentDataCenterName1, sslToKafka ? kafkaBroker.getSSLAddress() : kafkaBroker.getAddress());
          builder.put(CHILD_DATA_CENTER_KAFKA_ZK_PREFIX + "." + parentDataCenterName1, kafkaBroker.getZkAddress());
          builder.put(PARENT_KAFKA_CLUSTER_FABRIC_LIST, parentDataCenterName1);

          /**
           * The controller wrapper doesn't know about the new Kafka cluster created inside the test case; so if the test case
           * is trying to pass in information about the extra Kafka clusters, controller wrapper will try to include the urls
           * of the extra Kafka cluster with the parent Kafka cluster created in the wrapper.
           */
          if (extraProps.containsKey(PARENT_KAFKA_CLUSTER_FABRIC_LIST)) {
            nativeReplicationSourceFabricWhitelist = nativeReplicationSourceFabricWhitelist + "," + extraProps.getString(PARENT_KAFKA_CLUSTER_FABRIC_LIST);
            builder.put(NATIVE_REPLICATION_FABRIC_WHITELIST, nativeReplicationSourceFabricWhitelist);
            builder.put(PARENT_KAFKA_CLUSTER_FABRIC_LIST, parentDataCenterName1 + "," + extraProps.getString(PARENT_KAFKA_CLUSTER_FABRIC_LIST));
          }

          /**
           * If the test case doesn't nominate any region as the NR source region, dc-0 (the first child data center) will
           * be the NR source, not the parent Kafka cluster.
           */
          if (!extraProps.containsKey(NATIVE_REPLICATION_SOURCE_FABRIC)) {
            builder.put(NATIVE_REPLICATION_SOURCE_FABRIC, "dc-0");
          }
        }

        VeniceProperties props = builder.build();
        propertiesList.add(props);
      }

      List<D2Server> d2ServerList = new ArrayList<>();
      if (d2Enabled) {
        d2ServerList.add(createD2Server(zkAddress, adminPort));
      }

      D2Client d2Client = D2TestUtils.getAndStartD2Client(zkAddress);
      MetricsRepository metricsRepository =  TehutiUtils.getMetricsRepository(CONTROLLER_SERVICE_NAME);

      Optional<ClientConfig> consumerClientConfig = Optional.empty();
      Object clientConfig = extraProperties.get(VeniceServerWrapper.CLIENT_CONFIG_FOR_CONSUMER);
      if (clientConfig != null && clientConfig instanceof ClientConfig) {
        consumerClientConfig = Optional.of((ClientConfig) clientConfig);
      }

      VeniceController veniceController = new VeniceController(propertiesList, metricsRepository, d2ServerList, Optional.empty(), authorizerService, d2Client, consumerClientConfig);
      return new VeniceControllerWrapper(serviceName, dataDirectory, veniceController, adminPort, adminSecurePort, propertiesList, d2ServerList, zkAddress, metricsRepository);
    };
  }

  private static String createDataCenterNameWithIndex(int index) {
    return "dc-" + index;
  }

  static StatefulServiceProvider<VeniceControllerWrapper> generateService(String[] clusterNames, String zkAddress,
      KafkaBrokerWrapper kafkaBrokerWrapper, boolean isParent, int replicationFactor, int partitionSize,
      long rebalanceDelayMs, int minActiveReplica, VeniceControllerWrapper[] childControllers,
      Properties extraProperties, String clusterToD2, boolean sslToKafka, boolean d2Enable) {
    return generateService(clusterNames, zkAddress, kafkaBrokerWrapper, isParent, replicationFactor, partitionSize,
        rebalanceDelayMs, minActiveReplica, childControllers, extraProperties, clusterToD2, sslToKafka, d2Enable,
        Optional.empty());
  }

  @Override
  public String getHost() {
    return DEFAULT_HOST_NAME;
  }

  @Override
  public int getPort() {
    return port;
  }

  public int getSecurePort() {
    return securePort;
  }

  public String getControllerUrl() {
    return "http://" + getHost() + ":" + getPort();
  }

  /**
   * Secure controller url only allows SSL connection
   */
  public String getSecureControllerUrl() {
    return "https://" + getHost() + ":" + getSecurePort();
  }

  public String getKafkaBootstrapServers(boolean sslToKafka) {
    if (sslToKafka) {
      return configs.get(0).getString(SSL_KAFKA_BOOTSTRAP_SERVERS);
    }
    return configs.get(0).getString(KAFKA_BOOTSTRAP_SERVERS);
  }

  public String getKafkaZkAddress() {
    return configs.get(0).getString(KAFKA_ZK_ADDRESS);
  }

  @Override
  protected void internalStart() throws Exception {
    service.start();
  }

  @Override
  protected void internalStop() throws Exception {
    service.stop();
  }

  private static D2Server createD2Server(String zkAddress, int port) {
    return D2TestUtils.getD2Server(zkAddress, "http://localhost:" + port, D2TestUtils.CONTROLLER_CLUSTER_NAME);
  }

  @Override
  protected void newProcess() throws Exception {
    /**
     * {@link D2Server} can't be reused for restart because of the following exception:
     * Caused by: java.lang.IllegalStateException: Can not start ZKConnection when STOPPED
     *  at com.linkedin.d2.discovery.stores.zk.ZKPersistentConnection.start(ZKPersistentConnection.java:200)
     *  at com.linkedin.d2.balancer.servers.ZooKeeperConnectionManager.start(ZooKeeperConnectionManager.java:113)
     *  at com.linkedin.d2.spring.D2ServerManager.doStart(D2ServerManager.java:226)
     *  ... 36 more
     */
    if (!d2ServerList.isEmpty()) {
      d2ServerList.clear();
      d2ServerList.add(createD2Server(zkAddress, port));
    }
    D2Client d2Client = D2TestUtils.getAndStartD2Client(zkAddress);
    service = new VeniceController(configs, d2ServerList, Optional.empty(), d2Client);
  }

  /***
   * Sets a version to be active for a given store and version
   *
   * @param storeName
   * @param version
   */
  public void setActiveVersion(String clusterName, String storeName, int version) {
    try (ControllerClient controllerClient = new ControllerClient(clusterName, getControllerUrl())) {
      controllerClient.overrideSetActiveVersion(storeName, version);
    }
  }

  /***
   * Set a version to be active, parsing store name and version number from a kafka topic name
   *
   * @param kafkaTopic
   */
  public void setActiveVersion(String clusterName, String kafkaTopic) {
    String storeName = Version.parseStoreFromKafkaTopicName(kafkaTopic);
    int version = Version.parseVersionFromKafkaTopicName(kafkaTopic);
    setActiveVersion(clusterName, storeName, version);
  }

  public boolean isMasterController(String clusterName) {
    Admin admin = service.getVeniceControllerService().getVeniceHelixAdmin();
    return admin.isLeaderControllerFor(clusterName);
  }

  public boolean isMasterControllerOfControllerCluster() {
    Admin admin = service.getVeniceControllerService().getVeniceHelixAdmin();
    return admin.isMasterControllerOfControllerCluster();
  }

  public Admin getVeniceAdmin() {
    return service.getVeniceControllerService().getVeniceHelixAdmin();
  }

  // for test purpose
  public AdminConsumerService getAdminConsumerServiceByCluster(String cluster) {
    return service.getVeniceControllerService().getAdminConsumerServiceByCluster(cluster);
  }

  public MetricsRepository getMetricRepository() {
    return metricsRepository;
  }
}
