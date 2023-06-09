package com.linkedin.venice.integration.utils;

import static com.linkedin.venice.ConfigKeys.ADMIN_PORT;
import static com.linkedin.venice.ConfigKeys.ADMIN_SECURE_PORT;
import static com.linkedin.venice.ConfigKeys.ADMIN_TOPIC_REPLICATION_FACTOR;
import static com.linkedin.venice.ConfigKeys.CHILD_CLUSTER_ALLOWLIST;
import static com.linkedin.venice.ConfigKeys.CHILD_CLUSTER_URL_PREFIX;
import static com.linkedin.venice.ConfigKeys.CHILD_CLUSTER_WHITELIST;
import static com.linkedin.venice.ConfigKeys.CHILD_DATA_CENTER_KAFKA_URL_PREFIX;
import static com.linkedin.venice.ConfigKeys.CLUSTER_DISCOVERY_D2_SERVICE;
import static com.linkedin.venice.ConfigKeys.CLUSTER_TO_D2;
import static com.linkedin.venice.ConfigKeys.CLUSTER_TO_SERVER_D2;
import static com.linkedin.venice.ConfigKeys.CONCURRENT_INIT_ROUTINES_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_ADD_VERSION_VIA_ADMIN_PROTOCOL;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_NAME;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_PARENT_MODE;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_SSL_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_SYSTEM_SCHEMA_CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_ZK_SHARED_DAVINCI_PUSH_STATUS_SYSTEM_SCHEMA_STORE_AUTO_CREATION_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_ZK_SHARED_META_SYSTEM_SCHEMA_STORE_AUTO_CREATION_ENABLED;
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_SCAN_INTERVAL_IN_SECONDS;
import static com.linkedin.venice.ConfigKeys.DEFAULT_MAX_NUMBER_OF_PARTITIONS;
import static com.linkedin.venice.ConfigKeys.DEFAULT_NUMBER_OF_PARTITION;
import static com.linkedin.venice.ConfigKeys.DEFAULT_PARTITION_SIZE;
import static com.linkedin.venice.ConfigKeys.DEFAULT_REPLICA_FACTOR;
import static com.linkedin.venice.ConfigKeys.DELAY_TO_REBALANCE_MS;
import static com.linkedin.venice.ConfigKeys.ENABLE_HYBRID_PUSH_SSL_WHITELIST;
import static com.linkedin.venice.ConfigKeys.ENABLE_OFFLINE_PUSH_SSL_WHITELIST;
import static com.linkedin.venice.ConfigKeys.KAFKA_ADMIN_CLASS;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.KAFKA_REPLICATION_FACTOR;
import static com.linkedin.venice.ConfigKeys.KAFKA_SECURITY_PROTOCOL;
import static com.linkedin.venice.ConfigKeys.MIN_ACTIVE_REPLICA;
import static com.linkedin.venice.ConfigKeys.NATIVE_REPLICATION_FABRIC_ALLOWLIST;
import static com.linkedin.venice.ConfigKeys.NATIVE_REPLICATION_SOURCE_FABRIC;
import static com.linkedin.venice.ConfigKeys.OFFLINE_JOB_START_TIMEOUT_MS;
import static com.linkedin.venice.ConfigKeys.PARENT_KAFKA_CLUSTER_FABRIC_LIST;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.PUSH_STATUS_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.SSL_KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.SSL_TO_KAFKA_LEGACY;
import static com.linkedin.venice.ConfigKeys.STORAGE_ENGINE_OVERHEAD_RATIO;
import static com.linkedin.venice.ConfigKeys.TOPIC_CLEANUP_DELAY_FACTOR;
import static com.linkedin.venice.ConfigKeys.TOPIC_CLEANUP_SEND_CONCURRENT_DELETES_REQUESTS;
import static com.linkedin.venice.ConfigKeys.TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS;
import static com.linkedin.venice.ConfigKeys.TOPIC_CREATION_THROTTLING_TIME_WINDOW_MS;
import static com.linkedin.venice.SSLConfig.DEFAULT_CONTROLLER_SSL_ENABLED;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceController;
import com.linkedin.venice.controller.VeniceControllerContext;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controller.kafka.consumer.AdminConsumerService;
import com.linkedin.venice.controller.supersetschema.SupersetSchemaGenerator;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.d2.D2Server;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.adapter.kafka.admin.ApacheKafkaAdminAdapter;
import com.linkedin.venice.pubsub.api.PubSubClientsFactory;
import com.linkedin.venice.servicediscovery.ServiceDiscoveryAnnouncer;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.utils.KafkaSSLUtils;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A wrapper for the {@link VeniceControllerWrapper}.
 * <p>
 * Calling close() will clean up the controller's data directory.
 */
public class VeniceControllerWrapper extends ProcessWrapper {
  private static final Logger LOGGER = LogManager.getLogger(VeniceControllerWrapper.class);

  public static final String D2_CLUSTER_NAME = "ControllerD2Cluster";
  public static final String D2_SERVICE_NAME = "ChildController";
  public static final String PARENT_D2_CLUSTER_NAME = "ParentControllerD2Cluster";
  public static final String PARENT_D2_SERVICE_NAME = "ParentController";

  public static final String SUPERSET_SCHEMA_GENERATOR = "SupersetSchemaGenerator";

  public static final double DEFAULT_STORAGE_ENGINE_OVERHEAD_RATIO = 0.85d;
  public static final String DEFAULT_PARENT_DATA_CENTER_REGION_NAME = "dc-parent-0";

  private VeniceController service;
  private final List<VeniceProperties> configs;
  private final boolean isParent;
  private final int port;
  private final int securePort;
  private final String zkAddress;
  private final List<ServiceDiscoveryAnnouncer> d2ServerList;
  private final MetricsRepository metricsRepository;
  private final String regionName;

  private final PubSubClientsFactory pubSubClientsFactory;

  private VeniceControllerWrapper(
      String regionName,
      String serviceName,
      File dataDirectory,
      VeniceController service,
      int port,
      int securePort,
      List<VeniceProperties> configs,
      boolean isParent,
      List<ServiceDiscoveryAnnouncer> d2ServerList,
      String zkAddress,
      MetricsRepository metricsRepository,
      PubSubClientsFactory pubSubClientsFactory) {
    super(serviceName, dataDirectory);
    this.service = service;
    this.configs = configs;
    this.isParent = isParent;
    this.port = port;
    this.securePort = securePort;
    this.zkAddress = zkAddress;
    this.d2ServerList = d2ServerList;
    this.metricsRepository = metricsRepository;
    this.regionName = regionName;
    this.pubSubClientsFactory = pubSubClientsFactory;
  }

  static StatefulServiceProvider<VeniceControllerWrapper> generateService(VeniceControllerCreateOptions options) {
    return (serviceName, dataDirectory) -> {
      int adminPort = TestUtils.getFreePort();
      int adminSecurePort = TestUtils.getFreePort();
      List<VeniceProperties> propertiesList = new ArrayList<>();

      VeniceProperties extraProps = new VeniceProperties(options.getExtraProperties());
      final boolean sslEnabled = extraProps.getBoolean(CONTROLLER_SSL_ENABLED, DEFAULT_CONTROLLER_SSL_ENABLED);

      Map<String, String> clusterToD2;
      if (options.getClusterToD2() == null || options.getClusterToD2().isEmpty()) {
        clusterToD2 = Arrays.stream(options.getClusterNames())
            .collect(Collectors.toMap(c -> c, c -> Utils.getUniqueString("router_d2_service")));
      } else {
        clusterToD2 = options.getClusterToD2();
      }

      Map<String, String> clusterToServerD2;
      if (options.getClusterToServerD2() == null || options.getClusterToServerD2().isEmpty()) {
        clusterToServerD2 = Arrays.stream(options.getClusterNames())
            .collect(Collectors.toMap(c -> c, c -> Utils.getUniqueString("server_d2_service")));
      } else {
        clusterToServerD2 = options.getClusterToServerD2();
      }

      for (String clusterName: options.getClusterNames()) {
        VeniceProperties clusterProps = IntegrationTestUtils
            .getClusterProps(clusterName, options.getZkAddress(), options.getKafkaBroker(), options.isSslToKafka());

        // TODO: Validate that these configs are all still used.
        // TODO: Centralize default config values in a single place
        PropertyBuilder builder = new PropertyBuilder().put(clusterProps.toProperties())
            .put(KAFKA_REPLICATION_FACTOR, 1)
            .put(ADMIN_TOPIC_REPLICATION_FACTOR, 1)
            .put(CONTROLLER_NAME, "venice-controller") // Why is this configurable?
            .put(DEFAULT_REPLICA_FACTOR, options.getReplicationFactor())
            .put(ADMIN_PORT, adminPort)
            .put(ADMIN_SECURE_PORT, adminSecurePort)
            .put(DEFAULT_PARTITION_SIZE, options.getPartitionSize())
            .put(DEFAULT_NUMBER_OF_PARTITION, options.getNumberOfPartitions())
            .put(DEFAULT_MAX_NUMBER_OF_PARTITIONS, options.getMaxNumberOfPartitions())
            .put(CONTROLLER_PARENT_MODE, options.isParent())
            .put(DELAY_TO_REBALANCE_MS, options.getRebalanceDelayMs())
            .put(MIN_ACTIVE_REPLICA, options.getMinActiveReplica())
            .put(TOPIC_CREATION_THROTTLING_TIME_WINDOW_MS, 100)
            .put(STORAGE_ENGINE_OVERHEAD_RATIO, DEFAULT_STORAGE_ENGINE_OVERHEAD_RATIO)
            .put(CLUSTER_TO_D2, TestUtils.getClusterToD2String(clusterToD2))
            .put(CLUSTER_TO_SERVER_D2, TestUtils.getClusterToD2String(clusterToServerD2))
            .put(SSL_TO_KAFKA_LEGACY, options.isSslToKafka())
            .put(SSL_KAFKA_BOOTSTRAP_SERVERS, options.getKafkaBroker().getSSLAddress())
            .put(ENABLE_OFFLINE_PUSH_SSL_WHITELIST, false)
            .put(ENABLE_HYBRID_PUSH_SSL_WHITELIST, false)
            .put(KAFKA_BOOTSTRAP_SERVERS, options.getKafkaBroker().getAddress())
            .put(OFFLINE_JOB_START_TIMEOUT_MS, 120_000)
            // To speed up topic cleanup
            .put(TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS, 100)
            .put(TOPIC_CLEANUP_DELAY_FACTOR, 2)
            .put(KAFKA_ADMIN_CLASS, ApacheKafkaAdminAdapter.class.getName())
            .put(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB)
            // Moving from topic monitor to admin protocol for add version and starting ingestion
            .put(CONTROLLER_ADD_VERSION_VIA_ADMIN_PROTOCOL, true)
            // The first cluster will always be the one to host system schemas...
            .put(CONTROLLER_SYSTEM_SCHEMA_CLUSTER_NAME, options.getClusterNames()[0])
            .put(TOPIC_CLEANUP_SEND_CONCURRENT_DELETES_REQUESTS, false)
            .put(CONTROLLER_ZK_SHARED_META_SYSTEM_SCHEMA_STORE_AUTO_CREATION_ENABLED, true)
            .put(CONTROLLER_ZK_SHARED_DAVINCI_PUSH_STATUS_SYSTEM_SCHEMA_STORE_AUTO_CREATION_ENABLED, true)
            .put(PUSH_STATUS_STORE_ENABLED, true)
            .put(DAVINCI_PUSH_STATUS_SCAN_INTERVAL_IN_SECONDS, 5)
            .put(CONCURRENT_INIT_ROUTINES_ENABLED, true)
            .put(CLUSTER_DISCOVERY_D2_SERVICE, VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
            .put(extraProps.toProperties());

        if (sslEnabled) {
          builder.put(SslUtils.getVeniceLocalSslProperties());
        }

        if (options.isSslToKafka()) {
          builder.put(KAFKA_SECURITY_PROTOCOL, SecurityProtocol.SSL.name);
          builder.put(KafkaSSLUtils.getLocalCommonKafkaSSLConfig());
        }

        String fabricAllowList = "";
        if (options.isParent()) {
          // Parent controller needs config to route per-cluster requests such as job status
          // This dummy parent controller won't support such requests until we make this config configurable.
          // go/inclusivecode deferred(Reference will be removed when clients have migrated)
          fabricAllowList = extraProps.getStringWithAlternative(CHILD_CLUSTER_ALLOWLIST, CHILD_CLUSTER_WHITELIST, "");
        }

        /**
         * Check if the Venice setup is single or multi data center. It's valid for a single data center setup to not
         * have fabric allow list and child data center controller map for child controllers.
         */
        if (options.getChildControllers() != null) {
          for (int dcIndex = 0; dcIndex < options.getChildControllers().length; dcIndex++) {
            String dcName = createDataCenterNameWithIndex(dcIndex);
            if (!fabricAllowList.equals("")) {
              fabricAllowList += ",";
            }
            fabricAllowList += dcName;
            VeniceControllerWrapper childController = options.getChildControllers()[dcIndex];
            if (childController == null) {
              throw new IllegalArgumentException("child controller at index " + dcIndex + " is null!");
            }
            builder.put(CHILD_CLUSTER_URL_PREFIX + dcName, childController.getControllerUrl());
            if (options.isParent()) {
              builder.put(
                  CHILD_DATA_CENTER_KAFKA_URL_PREFIX + "." + dcName,
                  childController.getKafkaBootstrapServers(options.isSslToKafka()));
              LOGGER.info(
                  "ControllerConfig: {}.{} KafkaUrl: {}",
                  CHILD_DATA_CENTER_KAFKA_URL_PREFIX,
                  dcName,
                  childController.getKafkaBootstrapServers(options.isSslToKafka()));
            }
          }
        }
        builder.put(CHILD_CLUSTER_ALLOWLIST, fabricAllowList);

        if (options.isParent()) {
          /**
           * In native replication, source fabric can be child fabrics as well as parent fabric;
           * and in parent fabric, there can be more than one Kafka clusters, so we might need more
           * than one parent fabric name even though logically there is only one parent fabric.
           */
          String parentDataCenterName1 = DEFAULT_PARENT_DATA_CENTER_REGION_NAME;
          String nativeReplicationSourceFabricAllowlist = fabricAllowList + "," + parentDataCenterName1;
          builder.put(NATIVE_REPLICATION_FABRIC_ALLOWLIST, nativeReplicationSourceFabricAllowlist);
          builder.put(
              CHILD_DATA_CENTER_KAFKA_URL_PREFIX + "." + parentDataCenterName1,
              options.isSslToKafka()
                  ? options.getKafkaBroker().getSSLAddress()
                  : options.getKafkaBroker().getAddress());
          builder.put(PARENT_KAFKA_CLUSTER_FABRIC_LIST, parentDataCenterName1);

          /**
           * The controller wrapper doesn't know about the new Kafka cluster created inside the test case; so if the test case
           * is trying to pass in information about the extra Kafka clusters, controller wrapper will try to include the urls
           * of the extra Kafka cluster with the parent Kafka cluster created in the wrapper.
           */
          if (extraProps.containsKey(PARENT_KAFKA_CLUSTER_FABRIC_LIST)) {
            nativeReplicationSourceFabricAllowlist =
                nativeReplicationSourceFabricAllowlist + "," + extraProps.getString(PARENT_KAFKA_CLUSTER_FABRIC_LIST);
            builder.put(NATIVE_REPLICATION_FABRIC_ALLOWLIST, nativeReplicationSourceFabricAllowlist);
            builder.put(
                PARENT_KAFKA_CLUSTER_FABRIC_LIST,
                parentDataCenterName1 + "," + extraProps.getString(PARENT_KAFKA_CLUSTER_FABRIC_LIST));
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
      List<ServiceDiscoveryAnnouncer> d2ServerList = new ArrayList<>();
      if (options.isD2Enabled()) {
        d2ServerList.add(createD2Server(options.getZkAddress(), adminPort, false, options.isParent()));
        if (sslEnabled) {
          d2ServerList.add(createD2Server(options.getZkAddress(), adminSecurePort, true, options.isParent()));
        }
      }

      D2Client d2Client = D2TestUtils.getAndStartD2Client(options.getZkAddress());
      MetricsRepository metricsRepository = TehutiUtils.getMetricsRepository(D2_SERVICE_NAME);

      Optional<ClientConfig> consumerClientConfig = Optional.empty();
      Object clientConfig = options.getExtraProperties().get(VeniceServerWrapper.CLIENT_CONFIG_FOR_CONSUMER);
      if (clientConfig != null && clientConfig instanceof ClientConfig) {
        consumerClientConfig = Optional.of((ClientConfig) clientConfig);
      }
      Optional<SupersetSchemaGenerator> supersetSchemaGenerator = Optional.empty();
      Object passedSupersetSchemaGenerator = options.getExtraProperties().get(SUPERSET_SCHEMA_GENERATOR);
      if (passedSupersetSchemaGenerator != null && passedSupersetSchemaGenerator instanceof SupersetSchemaGenerator) {
        supersetSchemaGenerator = Optional.of((SupersetSchemaGenerator) passedSupersetSchemaGenerator);
      }
      VeniceControllerContext ctx = new VeniceControllerContext.Builder().setPropertiesList(propertiesList)
          .setMetricsRepository(metricsRepository)
          .setServiceDiscoveryAnnouncers(d2ServerList)
          .setAuthorizerService(options.getAuthorizerService())
          .setD2Client(d2Client)
          .setRouterClientConfig(consumerClientConfig.orElse(null))
          .setExternalSupersetSchemaGenerator(supersetSchemaGenerator.orElse(null))
          .setPubSubClientsFactory(options.getKafkaBroker().getPubSubClientsFactory())
          .build();
      VeniceController veniceController = new VeniceController(ctx);
      return new VeniceControllerWrapper(
          options.getRegionName(),
          serviceName,
          dataDirectory,
          veniceController,
          adminPort,
          adminSecurePort,
          propertiesList,
          options.isParent(),
          d2ServerList,
          options.getZkAddress(),
          metricsRepository,
          options.getKafkaBroker().getPubSubClientsFactory());
    };
  }

  private static String createDataCenterNameWithIndex(int index) {
    return "dc-" + index;
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

  @Override
  protected void internalStart() throws Exception {
    service.start();
  }

  @Override
  protected void internalStop() throws Exception {
    service.stop();
  }

  private static D2Server createD2Server(String zkAddress, int port, boolean https, boolean isParent) {
    String scheme = https ? "https" : "http";
    String d2ClusterName = isParent ? PARENT_D2_CLUSTER_NAME : D2_CLUSTER_NAME;
    return D2TestUtils.createD2Server(zkAddress, scheme + "://localhost:" + port, d2ClusterName);
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
      d2ServerList.add(createD2Server(zkAddress, port, false, isParent));
      d2ServerList.add(createD2Server(zkAddress, securePort, true, isParent));
    }
    D2Client d2Client = D2TestUtils.getAndStartD2Client(zkAddress);
    service = new VeniceController(
        new VeniceControllerContext.Builder().setPropertiesList(configs)
            .setServiceDiscoveryAnnouncers(d2ServerList)
            .setD2Client(d2Client)
            .setPubSubClientsFactory(pubSubClientsFactory)
            .build());
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

  public boolean isLeaderController(String clusterName) {
    Admin admin = service.getVeniceControllerService().getVeniceHelixAdmin();
    return admin.isLeaderControllerFor(clusterName);
  }

  public boolean isLeaderControllerOfControllerCluster() {
    Admin admin = service.getVeniceControllerService().getVeniceHelixAdmin();
    return admin.isLeaderControllerOfControllerCluster();
  }

  public Admin getVeniceAdmin() {
    return service.getVeniceControllerService().getVeniceHelixAdmin();
  }

  public VeniceHelixAdmin getVeniceHelixAdmin() {
    return (VeniceHelixAdmin) getVeniceAdmin();
  }

  // for test purpose
  public AdminConsumerService getAdminConsumerServiceByCluster(String cluster) {
    return service.getVeniceControllerService().getAdminConsumerServiceByCluster(cluster);
  }

  public String getZkAddress() {
    return this.zkAddress;
  }

  public MetricsRepository getMetricRepository() {
    return metricsRepository;
  }

  @Override
  public String getComponentTagForLogging() {
    return new StringBuilder(getComponentTagPrefix(regionName)).append(super.getComponentTagForLogging()).toString();
  }
}
