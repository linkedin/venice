package com.linkedin.venice.integration.utils;

import com.linkedin.d2.server.factory.D2Server;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceController;
import com.linkedin.venice.controller.kafka.consumer.AdminConsumerService;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.Version;

import com.linkedin.venice.utils.KafkaSSLUtils;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.protocol.SecurityProtocol;

import static com.linkedin.venice.ConfigKeys.*;


/**
 * A wrapper for the {@link VeniceControllerWrapper}.
 * <p>
 * Calling close() will clean up the controller's data directory.
 */
public class VeniceControllerWrapper extends ProcessWrapper {
  public static final String SERVICE_NAME = "VeniceController";
  public static final double DEFAULT_STORAGE_ENGINE_OVERHEAD_RATIO = 0.85d;

  private final List<VeniceProperties> configs;
  private VeniceController service;
  private final int port;
  private final int securePort;
  private final List<D2Server> d2ServerList;
  private final String zkAddress;

  VeniceControllerWrapper(String serviceName, File dataDirectory, VeniceController service, int port, int securePort,
      List<VeniceProperties> configs, List<D2Server> d2ServerList, String zkAddress) {
    super(serviceName, dataDirectory);
    this.service = service;
    this.port = port;
    this.securePort = securePort;
    this.configs = configs;
    this.d2ServerList = d2ServerList;
    this.zkAddress = zkAddress;
  }

  static StatefulServiceProvider<VeniceControllerWrapper> generateService(String[] clusterNames, String zkAddress,
      KafkaBrokerWrapper kafkaBrokerWrapper, boolean isParent, int replicaFactor, int partitionSize,
      long delayToReblanceMS, int minActiveReplica, VeniceControllerWrapper[] childControllers,
      VeniceProperties extraProps, String clusterToD2, boolean sslToKafka, boolean d2Enabled) {
    return (serviceName, port, dataDirectory) -> {
      List<VeniceProperties> propertiesList = new ArrayList<>();
      int adminPort = IntegrationTestUtils.getFreePort();
      int adminSecurePort = IntegrationTestUtils.getFreePort();
      for(String clusterName:clusterNames) {
        VeniceProperties clusterProps =
            IntegrationTestUtils.getClusterProps(clusterName, dataDirectory, zkAddress, kafkaBrokerWrapper, sslToKafka);

        // TODO: Validate that these configs are all still used.
        // TODO: Centralize default config values in a single place
        PropertyBuilder builder = new PropertyBuilder().put(clusterProps.toProperties())
            .put(KAFKA_REPLICATION_FACTOR, 1)
            .put(ADMIN_TOPIC_REPLICATION_FACTOR, 1)
            .put(KAFKA_ZK_ADDRESS, kafkaBrokerWrapper.getZkAddress())
            .put(CONTROLLER_NAME, "venice-controller") // Why is this configurable?
            .put(DEFAULT_REPLICA_FACTOR, replicaFactor)
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
            .put(TOPIC_MONITOR_POLL_INTERVAL_MS, 100)
            .put(CONTROLLER_PARENT_MODE, isParent)
            .put(DELAY_TO_REBALANCE_MS, delayToReblanceMS)
            .put(MIN_ACTIVE_REPLICA, minActiveReplica)
            .put(TOPIC_CREATION_THROTTLING_TIME_WINDOW_MS, 100)
            .put(STORAGE_ENGINE_OVERHEAD_RATIO, DEFAULT_STORAGE_ENGINE_OVERHEAD_RATIO)
            .put(CLUSTER_TO_D2,
                Utils.isNullOrEmpty(clusterToD2) ? TestUtils.getClusterToDefaultD2String(clusterName) : clusterToD2)
            .put(SSL_TO_KAFKA, sslToKafka)
            .put(SSL_KAFKA_BOOTSTRAP_SERVERS, kafkaBrokerWrapper.getSSLAddress())
            .put(ENABLE_OFFLINE_PUSH_SSL_WHITELIST, false)
            .put(ENABLE_HYBRID_PUSH_SSL_WHITELIST, false)
            .put(KAFKA_BOOTSTRAP_SERVERS, kafkaBrokerWrapper.getAddress())
            .put(OFFLINE_JOB_START_TIMEOUT_MS, 60 * 1000)
            // To speed up topic cleanup
            .put(TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS, 100)
            .put(PERSISTENCE_TYPE, PersistenceType.BDB)
            // Moving from topic monitor to admin protocol for add version and starting ingestion
            .put(CONTROLLER_ADD_VERSION_VIA_ADMIN_PROTOCOL, true)
            .put(CONTROLLER_ADD_VERSION_VIA_TOPIC_MONITOR, false)
            // The first cluster will always be the one to host system schemas...
            .put(CONTROLLER_SYSTEM_SCHEMA_CLUSTER_NAME, clusterNames[0])
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
          for (int dataCenterIndex = 0; dataCenterIndex < childControllers.length; dataCenterIndex++) {
            String childDataCenterName = "dc-" + dataCenterIndex;
            if (!clusterWhiteList.equals("")) {
              clusterWhiteList += ",";
            }
            clusterWhiteList += childDataCenterName;
            VeniceControllerWrapper childController = childControllers[dataCenterIndex];
            if (null == childController) {
              throw new IllegalArgumentException("child controller at index " + dataCenterIndex + " is null!");
            }
            builder.put(CHILD_CLUSTER_URL_PREFIX + "." + childDataCenterName, childController.getControllerUrl());
          }
          builder.put(CHILD_CLUSTER_WHITELIST, clusterWhiteList);
        }

        VeniceProperties props = builder.build();
        propertiesList.add(props);
      }
      List<D2Server> d2ServerList = new ArrayList<>();
      if (d2Enabled) {
        d2ServerList.add(createD2Server(zkAddress, adminPort));
      }
      VeniceController veniceController = new VeniceController(propertiesList, d2ServerList);
      return new VeniceControllerWrapper(serviceName, dataDirectory, veniceController, adminPort, adminSecurePort, propertiesList, d2ServerList, zkAddress);
    };
  }

  static StatefulServiceProvider<VeniceControllerWrapper> generateService(String clusterName, String zkAddress,
      KafkaBrokerWrapper kafkaBrokerWrapper, boolean isParent, int replicaFactor, int partitionSize,
      long delayToReblanceMS, int minActiveReplica, VeniceControllerWrapper[] childControllers,
      VeniceProperties extraProps, boolean sslToKafka) {

    return generateService(new String[]{clusterName}, zkAddress, kafkaBrokerWrapper, isParent, replicaFactor,
        partitionSize, delayToReblanceMS, minActiveReplica, childControllers, extraProps, null, sslToKafka, false);
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

    service = new VeniceController(configs, d2ServerList);
  }

  /***
   * Sets a version to be active for a given store and version
   *
   * @param storeName
   * @param version
   */
  public void setActiveVersion(String clusterName, String storeName, int version) {
    try(ControllerClient controllerClient = new ControllerClient(clusterName, getControllerUrl())) {
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
    return admin.isMasterController(clusterName);
  }

  public boolean isMasterControllerForControllerCluster() {
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
}
