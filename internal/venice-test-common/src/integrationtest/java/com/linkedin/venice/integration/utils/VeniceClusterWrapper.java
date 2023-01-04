package com.linkedin.venice.integration.utils;

import static com.linkedin.venice.ConfigKeys.DEFAULT_MAX_NUMBER_OF_PARTITIONS;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.SERVER_ENABLE_KAFKA_OPENSSL;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;
import static com.linkedin.venice.integration.utils.VeniceServerWrapper.CLIENT_CONFIG_FOR_CONSUMER;
import static com.linkedin.venice.integration.utils.VeniceServerWrapper.SERVER_ENABLE_SERVER_ALLOW_LIST;
import static com.linkedin.venice.integration.utils.VeniceServerWrapper.SERVER_ENABLE_SSL;
import static com.linkedin.venice.integration.utils.VeniceServerWrapper.SERVER_IS_AUTO_JOIN;
import static com.linkedin.venice.integration.utils.VeniceServerWrapper.SERVER_SSL_TO_KAFKA;
import static com.linkedin.venice.utils.ByteUtils.BYTES_PER_KB;
import static com.linkedin.venice.utils.ByteUtils.BYTES_PER_MB;
import static com.linkedin.venice.utils.TestPushUtils.defaultVPJProps;
import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static com.linkedin.venice.utils.TestUtils.writeBatchData;

import com.github.luben.zstd.ZstdDictTrainer;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.init.ClusterLeaderInitializationRoutine;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoClusterException;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.helix.Replica;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.utils.ExceptionUtils;
import com.linkedin.venice.utils.ForkedJavaProcess;
import com.linkedin.venice.utils.KafkaSSLUtils;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.lazy.LazyResettable;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;


/**
 * This is the whole enchilada:
 * - {@link ZkServerWrapper}
 * - {@link KafkaBrokerWrapper}
 * - {@link VeniceControllerWrapper}
 * - {@link VeniceServerWrapper}
 */
public class VeniceClusterWrapper extends ProcessWrapper {
  private static final Logger LOGGER = LogManager.getLogger(VeniceClusterWrapper.class);
  public static final String SERVICE_NAME = "VeniceCluster";

  // Forked process constants
  public static final String FORKED_PROCESS_EXCEPTION = "exception";
  public static final String FORKED_PROCESS_STORE_NAME = "storeName";
  public static final String FORKED_PROCESS_ZK_ADDRESS = "zkAddress";
  public static final int NUM_RECORDS = 1_000_000;

  private final String clusterName;
  private final boolean standalone;
  private final ZkServerWrapper zkServerWrapper;
  private final KafkaBrokerWrapper kafkaBrokerWrapper;

  private final int defaultReplicaFactor;
  private final int defaultPartitionSize;
  private final long defaultDelayToRebalanceMS;
  private final int defaultMinActiveReplica;
  private final Map<Integer, VeniceControllerWrapper> veniceControllerWrappers;
  private final Map<Integer, VeniceServerWrapper> veniceServerWrappers;
  private final Map<Integer, VeniceRouterWrapper> veniceRouterWrappers;
  private final LazyResettable<ControllerClient> controllerClient =
      LazyResettable.of(this::getControllerClient, ControllerClient::close);
  private final boolean sslToStorageNodes;
  private final boolean sslToKafka;
  private final Map<String, String> clusterToD2;

  private static Process veniceClusterProcess;
  // Controller discovery URLs are controllers that's created outside of this cluster wrapper but are overseeing the
  // cluster. e.g. controllers in a multi cluster wrapper.
  private String externalControllerDiscoveryURL = "";

  private static final List<AvroProtocolDefinition> CLUSTER_LEADER_INITIALIZATION_ROUTINES = Arrays.asList(
      AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE,
      AvroProtocolDefinition.PARTITION_STATE,
      AvroProtocolDefinition.STORE_VERSION_STATE,
      AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE,
      AvroProtocolDefinition.PUSH_STATUS_SYSTEM_SCHEMA_STORE);

  private static final AvroProtocolDefinition[] hybridRequiredSystemStores = new AvroProtocolDefinition[] {
      AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE, AvroProtocolDefinition.PUSH_STATUS_SYSTEM_SCHEMA_STORE };
  private static final Set<AvroProtocolDefinition> hybridRequiredSystemStoresSet =
      new HashSet<>(Arrays.asList(hybridRequiredSystemStores));

  VeniceClusterWrapper(
      String clusterName,
      boolean standalone,
      ZkServerWrapper zkServerWrapper,
      KafkaBrokerWrapper kafkaBrokerWrapper,
      Map<Integer, VeniceControllerWrapper> veniceControllerWrappers,
      Map<Integer, VeniceServerWrapper> veniceServerWrappers,
      Map<Integer, VeniceRouterWrapper> veniceRouterWrappers,
      int defaultReplicaFactor,
      int defaultPartitionSize,
      long defaultDelayToRebalanceMS,
      int mintActiveReplica,
      boolean sslToStorageNodes,
      boolean sslToKafka,
      Map<String, String> clusterToD2) {

    super(SERVICE_NAME, null);
    this.standalone = standalone;
    this.clusterName = clusterName;
    this.zkServerWrapper = zkServerWrapper;
    this.kafkaBrokerWrapper = kafkaBrokerWrapper;
    this.veniceControllerWrappers = veniceControllerWrappers;
    this.veniceServerWrappers = veniceServerWrappers;
    this.veniceRouterWrappers = veniceRouterWrappers;
    this.defaultReplicaFactor = defaultReplicaFactor;
    this.defaultPartitionSize = defaultPartitionSize;
    this.defaultDelayToRebalanceMS = defaultDelayToRebalanceMS;
    this.defaultMinActiveReplica = mintActiveReplica;
    this.sslToStorageNodes = sslToStorageNodes;
    this.sslToKafka = sslToKafka;
    this.clusterToD2 = clusterToD2;
  }

  static ServiceProvider<VeniceClusterWrapper> generateService(VeniceClusterCreateOptions options) {
    Map<Integer, VeniceControllerWrapper> veniceControllerWrappers = new HashMap<>();
    Map<Integer, VeniceServerWrapper> veniceServerWrappers = new HashMap<>();
    Map<Integer, VeniceRouterWrapper> veniceRouterWrappers = new HashMap<>();

    Map<String, String> clusterToD2;
    if (options.getClusterToD2() == null || options.getClusterToD2().isEmpty()) {
      clusterToD2 = Collections.singletonMap(options.getClusterName(), Utils.getUniqueString("router_d2_service"));
    } else {
      clusterToD2 = options.getClusterToD2();
    }

    ZkServerWrapper zkServerWrapper = options.getZkServerWrapper();
    KafkaBrokerWrapper kafkaBrokerWrapper = options.getKafkaBrokerWrapper();
    try {
      if (zkServerWrapper == null) {
        zkServerWrapper = ServiceFactory.getZkServer();
      }
      if (kafkaBrokerWrapper == null) {
        kafkaBrokerWrapper = ServiceFactory.getKafkaBroker(zkServerWrapper);
      }

      // Setup D2 for controller
      String zkAddress = zkServerWrapper.getAddress();
      D2TestUtils.setupD2Config(
          zkAddress,
          false,
          VeniceControllerWrapper.D2_CLUSTER_NAME,
          VeniceControllerWrapper.D2_SERVICE_NAME);
      for (int i = 0; i < options.getNumberOfControllers(); i++) {
        if (options.getNumberOfRouters() > 0) {
          ClientConfig clientConfig = new ClientConfig().setVeniceURL(zkAddress)
              .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
              .setSslFactory(SslUtils.getVeniceLocalSslFactory())
              .setStoreName("dummy");
          options.getExtraProperties().put(CLIENT_CONFIG_FOR_CONSUMER, clientConfig);
        }

        VeniceControllerWrapper veniceControllerWrapper = ServiceFactory.getVeniceController(
            new VeniceControllerCreateOptions.Builder(options.getClusterName(), kafkaBrokerWrapper)
                .replicationFactor(options.getReplicationFactor())
                .partitionSize(options.getPartitionSize())
                .rebalanceDelayMs(options.getRebalanceDelayMs())
                .minActiveReplica(options.getMinActiveReplica())
                .clusterToD2(clusterToD2)
                .sslToKafka(options.isSslToKafka())
                .d2Enabled(true)
                .extraProperties(options.getExtraProperties())
                .build());
        LOGGER.info(
            "[{}][{}] Created child controller on port {}",
            options.getColoName(),
            options.getClusterName(),
            veniceControllerWrapper.getPort());
        veniceControllerWrappers.put(veniceControllerWrapper.getPort(), veniceControllerWrapper);
      }

      for (int i = 0; i < options.getNumberOfRouters(); i++) {
        VeniceRouterWrapper veniceRouterWrapper = ServiceFactory.getVeniceRouter(
            options.getClusterName(),
            zkServerWrapper,
            kafkaBrokerWrapper,
            options.isSslToStorageNodes(),
            clusterToD2,
            options.getExtraProperties());
        LOGGER.info(
            "[{}][{}] Created router on port {}",
            options.getColoName(),
            options.getClusterName(),
            veniceRouterWrapper.getPort());
        veniceRouterWrappers.put(veniceRouterWrapper.getPort(), veniceRouterWrapper);
      }

      for (int i = 0; i < options.getNumberOfServers(); i++) {
        Properties featureProperties = new Properties();
        featureProperties.setProperty(SERVER_ENABLE_SERVER_ALLOW_LIST, Boolean.toString(options.isEnableAllowlist()));
        featureProperties.setProperty(SERVER_IS_AUTO_JOIN, Boolean.toString(options.isEnableAutoJoinAllowlist()));
        featureProperties.setProperty(SERVER_ENABLE_SSL, Boolean.toString(options.isSslToStorageNodes()));
        featureProperties.setProperty(SERVER_SSL_TO_KAFKA, Boolean.toString(options.isSslToKafka()));
        if (!veniceRouterWrappers.isEmpty()) {
          ClientConfig clientConfig = new ClientConfig().setVeniceURL(zkAddress)
              .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
              .setSslFactory(SslUtils.getVeniceLocalSslFactory());
          featureProperties.put(CLIENT_CONFIG_FOR_CONSUMER, clientConfig);
        }
        featureProperties.setProperty(SERVER_ENABLE_KAFKA_OPENSSL, Boolean.toString(options.isKafkaOpenSSLEnabled()));

        String serverName = "";
        if (!options.getColoName().isEmpty() && !options.getClusterName().isEmpty()) {
          serverName = options.getColoName() + ":" + options.getClusterName() + ":sn-" + i;
        }
        VeniceServerWrapper veniceServerWrapper = ServiceFactory.getVeniceServer(
            options.getClusterName(),
            kafkaBrokerWrapper,
            zkAddress,
            featureProperties,
            options.getExtraProperties(),
            options.isForkServer(),
            serverName,
            options.getKafkaClusterMap());
        LOGGER.info(
            "[{}][{}] Created server on port {}",
            options.getColoName(),
            options.getClusterName(),
            veniceServerWrapper.getPort());
        veniceServerWrappers.put(veniceServerWrapper.getPort(), veniceServerWrapper);
      }

      /**
       * We get the various dependencies outside of the lambda, to avoid having a time
       * complexity of O(N^2) on the amount of retries. The calls have their own retries,
       * so we can assume they're reliable enough.
       */
      ZkServerWrapper finalZkServerWrapper = zkServerWrapper;
      KafkaBrokerWrapper finalKafkaBrokerWrapper = kafkaBrokerWrapper;
      return (serviceName) -> {
        VeniceClusterWrapper veniceClusterWrapper = null;
        try {
          veniceClusterWrapper = new VeniceClusterWrapper(
              options.getClusterName(),
              options.isStandalone(),
              finalZkServerWrapper,
              finalKafkaBrokerWrapper,
              veniceControllerWrappers,
              veniceServerWrappers,
              veniceRouterWrappers,
              options.getReplicationFactor(),
              options.getPartitionSize(),
              options.getRebalanceDelayMs(),
              options.getMinActiveReplica(),
              options.isSslToStorageNodes(),
              options.isSslToKafka(),
              clusterToD2);
          // Wait for all the asynchronous ClusterLeaderInitializationRoutine to complete before returning the
          // VeniceClusterWrapper to tests.
          if (!veniceClusterWrapper.getVeniceControllers().isEmpty()) {
            final VeniceClusterWrapper finalClusterWrapper = veniceClusterWrapper;
            TestUtils.waitForNonDeterministicAssertion(2, TimeUnit.MINUTES, true, () -> {
              try {
                for (AvroProtocolDefinition avroProtocolDefinition: CLUSTER_LEADER_INITIALIZATION_ROUTINES) {
                  Store store = finalClusterWrapper.getLeaderVeniceController()
                      .getVeniceAdmin()
                      .getStore(options.getClusterName(), avroProtocolDefinition.getSystemStoreName());
                  Assert.assertNotNull(
                      store,
                      "Store: " + avroProtocolDefinition.getSystemStoreName() + " should be initialized by "
                          + ClusterLeaderInitializationRoutine.class.getSimpleName());
                  if (hybridRequiredSystemStoresSet.contains(avroProtocolDefinition)) {
                    // Check against the HelixReadOnlyZKSharedSystemStoreRepository instead of the
                    // ReadWriteStoreRepository because of the way we implemented getStore for meta system stores in
                    // HelixReadOnlyStoreRepositoryAdapter.
                    Store readOnlyStore = finalClusterWrapper.getLeaderVeniceController()
                        .getVeniceAdmin()
                        .getReadOnlyZKSharedSystemStoreRepository()
                        .getStore(avroProtocolDefinition.getSystemStoreName());
                    Assert.assertNotNull(
                        readOnlyStore,
                        "Store: " + avroProtocolDefinition.getSystemStoreName() + "should be initialized by "
                            + ClusterLeaderInitializationRoutine.class.getSimpleName());
                    Assert.assertTrue(
                        readOnlyStore.isHybrid(),
                        "Store: " + avroProtocolDefinition.getSystemStoreName() + " should be configured to hybrid by "
                            + ClusterLeaderInitializationRoutine.class.getSimpleName()
                            + ". Store is hybrid in write repo: " + store.isHybrid());
                  }
                }
              } catch (VeniceNoClusterException e) {
                /**
                 * This is possible if the async cluster resource initialization is not fully complete yet. i.e.
                 * {@link com.linkedin.venice.controller.VeniceHelixAdmin}#getHelixVeniceClusterResources(clusterName)
                 * will throw VeniceNoClusterException in VeniceHelixAdmin#throwClusterNotInitialized.
                 */
                Assert.fail("Cluster: " + options.getClusterName() + " is not initialized yet");
              }
            });
          }
        } catch (Throwable e) {
          LOGGER.error("Caught Throwable while creating the {}", VeniceClusterWrapper.class.getSimpleName(), e);
          Utils.closeQuietlyWithErrorLogged(veniceClusterWrapper);
          throw e;
        }
        return veniceClusterWrapper;
      };
    } catch (Throwable e) {
      veniceRouterWrappers.values().forEach(Utils::closeQuietlyWithErrorLogged);
      veniceServerWrappers.values().forEach(Utils::closeQuietlyWithErrorLogged);
      veniceControllerWrappers.values().forEach(Utils::closeQuietlyWithErrorLogged);
      IOUtils.closeQuietly(kafkaBrokerWrapper);
      IOUtils.closeQuietly(zkServerWrapper);
      throw e;
    }
  }

  static synchronized void generateServiceInAnotherProcess(String clusterInfoFilePath, int waitTimeInSeconds)
      throws IOException, InterruptedException {
    if (veniceClusterProcess != null) {
      LOGGER.warn(
          "Received a request to spawn a venice cluster in another process for testing "
              + "but one has already been running. Will not spawn a new one.");
      return;
    }

    veniceClusterProcess = ForkedJavaProcess.exec(VeniceClusterWrapper.class, clusterInfoFilePath);

    try {
      // wait some time to make sure all the services have started in the forked process
      if (veniceClusterProcess.waitFor(waitTimeInSeconds, TimeUnit.SECONDS)) {
        veniceClusterProcess.destroy();
        throw new VeniceException(
            "Venice cluster exited unexpectedly with the code " + veniceClusterProcess.exitValue());
      }
    } catch (InterruptedException e) {
      LOGGER.warn("Waiting for veniceClusterProcess to start is interrupted", e);
      Thread.currentThread().interrupt();
      return;
    }
    LOGGER.info("Venice cluster is started in a remote process!");
  }

  static synchronized void stopServiceInAnotherProcess() {
    veniceClusterProcess.destroy();
    veniceClusterProcess = null;
  }

  @Override
  protected void internalStart() throws Exception {
    // Everything should already be started. So this is a no-op.
  }

  @Override
  protected void internalStop() throws Exception {
    controllerClient.ifPresent(Utils::closeQuietlyWithErrorLogged);
    veniceRouterWrappers.values().forEach(Utils::closeQuietlyWithErrorLogged);
    veniceServerWrappers.values().forEach(Utils::closeQuietlyWithErrorLogged);
    veniceControllerWrappers.values().forEach(Utils::closeQuietlyWithErrorLogged);
    if (standalone) {
      Utils.closeQuietlyWithErrorLogged(kafkaBrokerWrapper);
      Utils.closeQuietlyWithErrorLogged(zkServerWrapper);
    }

    if (veniceClusterProcess != null) {
      veniceClusterProcess.destroy();
    }
  }

  @Override
  protected void newProcess() throws Exception {
    throw new UnsupportedOperationException("Cluster does not support to create new process.");
  }

  @Override
  public String getHost() {
    throw new VeniceException("Not applicable since this is a whole cluster of many different services.");
  }

  @Override
  public int getPort() {
    throw new VeniceException("Not applicable since this is a whole cluster of many different services.");
  }

  public String getClusterName() {
    return clusterName;
  }

  public ZkServerWrapper getZk() {
    return zkServerWrapper;
  }

  public KafkaBrokerWrapper getKafka() {
    return kafkaBrokerWrapper;
  }

  public synchronized List<VeniceControllerWrapper> getVeniceControllers() {
    return new ArrayList<>(veniceControllerWrappers.values());
  }

  public synchronized List<VeniceServerWrapper> getVeniceServers() {
    return new ArrayList<>(veniceServerWrappers.values());
  }

  public synchronized List<VeniceRouterWrapper> getVeniceRouters() {
    return new ArrayList<>(veniceRouterWrappers.values());
  }

  public synchronized VeniceRouterWrapper getRandomVeniceRouter() {
    // TODO might use D2 to get router in the future
    return getRandomRunningVeniceComponent(veniceRouterWrappers);
  }

  public String getRandomRouterURL() {
    return "http://" + getRandomVeniceRouter().getAddress();
  }

  public String getRandomRouterSslURL() {
    VeniceRouterWrapper router = getRandomVeniceRouter();
    return "https://" + router.getHost() + ":" + router.getSslPort();
  }

  public synchronized void refreshAllRouterMetaData() {
    veniceRouterWrappers.values().stream().filter(ProcessWrapper::isRunning).forEach(VeniceRouterWrapper::refresh);
  }

  public synchronized VeniceControllerWrapper getRandomVeniceController() {
    return getRandomRunningVeniceComponent(veniceControllerWrappers);
  }

  public void setExternalControllerDiscoveryURL(String externalControllerDiscoveryURL) {
    this.externalControllerDiscoveryURL = externalControllerDiscoveryURL;
  }

  public synchronized String getAllControllersURLs() {
    return veniceControllerWrappers.isEmpty()
        ? externalControllerDiscoveryURL
        : veniceControllerWrappers.values()
            .stream()
            .map(VeniceControllerWrapper::getControllerUrl)
            .collect(Collectors.joining(","));
  }

  public VeniceControllerWrapper getLeaderVeniceController() {
    return getLeaderVeniceController(60 * Time.MS_PER_SECOND);
  }

  public synchronized VeniceControllerWrapper getLeaderVeniceController(long timeoutMs) {
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    while (System.nanoTime() < deadline) {
      for (VeniceControllerWrapper controller: veniceControllerWrappers.values()) {
        if (controller.isRunning() && controller.isLeaderController(clusterName)) {
          return controller;
        }
      }
      Utils.sleep(Time.MS_PER_SECOND);
    }
    throw new VeniceException("Leader controller does not exist, cluster=" + clusterName);
  }

  public VeniceControllerWrapper addVeniceController(Properties properties) {
    VeniceControllerWrapper veniceControllerWrapper = ServiceFactory.getVeniceController(
        new VeniceControllerCreateOptions.Builder(clusterName, kafkaBrokerWrapper)
            .replicationFactor(defaultReplicaFactor)
            .partitionSize(defaultPartitionSize)
            .rebalanceDelayMs(defaultDelayToRebalanceMS)
            .minActiveReplica(defaultMinActiveReplica)
            .sslToKafka(sslToKafka)
            .clusterToD2(clusterToD2)
            .extraProperties(properties)
            .build());
    synchronized (this) {
      veniceControllerWrappers.put(veniceControllerWrapper.getPort(), veniceControllerWrapper);
      setExternalControllerDiscoveryURL(
          veniceControllerWrappers.values()
              .stream()
              .map(VeniceControllerWrapper::getControllerUrl)
              .collect(Collectors.joining(",")));
    }
    return veniceControllerWrapper;
  }

  public void addVeniceControllerWrapper(VeniceControllerWrapper veniceControllerWrapper) {
    synchronized (this) {
      veniceControllerWrappers.put(veniceControllerWrapper.getPort(), veniceControllerWrapper);
      setExternalControllerDiscoveryURL(
          veniceControllerWrappers.values()
              .stream()
              .map(VeniceControllerWrapper::getControllerUrl)
              .collect(Collectors.joining(",")));
    }
  }

  public VeniceRouterWrapper addVeniceRouter(Properties properties) {
    VeniceRouterWrapper veniceRouterWrapper = ServiceFactory
        .getVeniceRouter(clusterName, zkServerWrapper, kafkaBrokerWrapper, sslToStorageNodes, clusterToD2, properties);
    synchronized (this) {
      veniceRouterWrappers.put(veniceRouterWrapper.getPort(), veniceRouterWrapper);
    }
    return veniceRouterWrapper;
  }

  /**
   * @deprecated Future use should consider {@link #addVeniceServer(Properties, Properties)}
   *
   * @param enableAllowlist
   * @param enableAutoJoinAllowList
   * @return
   */
  public VeniceServerWrapper addVeniceServer(boolean enableAllowlist, boolean enableAutoJoinAllowList) {
    Properties featureProperties = new Properties();
    featureProperties.setProperty(SERVER_ENABLE_SERVER_ALLOW_LIST, Boolean.toString(enableAllowlist));
    featureProperties.setProperty(SERVER_IS_AUTO_JOIN, Boolean.toString(enableAutoJoinAllowList));
    VeniceServerWrapper veniceServerWrapper = ServiceFactory.getVeniceServer(
        clusterName,
        kafkaBrokerWrapper,
        getKafka().getZkAddress(),
        featureProperties,
        new Properties());
    synchronized (this) {
      veniceServerWrappers.put(veniceServerWrapper.getPort(), veniceServerWrapper);
    }
    return veniceServerWrapper;
  }

  /**
   * @deprecated Future use should consider {@link #addVeniceServer(Properties, Properties)}
   *
   * @param properties
   * @return
   */
  public VeniceServerWrapper addVeniceServer(Properties properties) {
    VeniceServerWrapper veniceServerWrapper = ServiceFactory
        .getVeniceServer(clusterName, kafkaBrokerWrapper, getKafka().getZkAddress(), new Properties(), properties);
    synchronized (this) {
      veniceServerWrappers.put(veniceServerWrapper.getPort(), veniceServerWrapper);
    }
    return veniceServerWrapper;
  }

  public VeniceServerWrapper addVeniceServer(Properties featureProperties, Properties configProperties) {
    VeniceServerWrapper veniceServerWrapper = ServiceFactory.getVeniceServer(
        clusterName,
        kafkaBrokerWrapper,
        getKafka().getZkAddress(),
        featureProperties,
        configProperties);
    synchronized (this) {
      veniceServerWrappers.put(veniceServerWrapper.getPort(), veniceServerWrapper);
    }
    return veniceServerWrapper;
  }

  /**
   * Find the leader controller, stop it and return its port.
   * @return
   */
  public synchronized int stopLeaderVeniceController() {
    try {
      VeniceControllerWrapper leaderController = getLeaderVeniceController();
      int port = leaderController.getPort();
      leaderController.stop();
      return port;
    } catch (Exception e) {
      throw new VeniceException("Can not stop leader controller.", e);
    } finally {
      controllerClient.reset();
    }
  }

  public synchronized void stopVeniceController(int port) {
    stopVeniceComponent(veniceControllerWrappers, port);
    controllerClient.reset();
  }

  public synchronized void restartVeniceController(int port) {
    restartVeniceComponent(veniceControllerWrappers, port);
    controllerClient.reset();
  }

  public synchronized void removeVeniceController(int port) {
    stopVeniceController(port);
    IOUtils.closeQuietly(veniceControllerWrappers.remove(port));
    controllerClient.reset();
  }

  public synchronized void stopVeniceRouter(int port) {
    stopVeniceComponent(veniceRouterWrappers, port);
  }

  public synchronized void restartVeniceRouter(int port) {
    restartVeniceComponent(veniceRouterWrappers, port);
  }

  public synchronized void removeVeniceRouter(int port) {
    stopVeniceRouter(port);
    IOUtils.closeQuietly(veniceRouterWrappers.remove(port));
  }

  /**
   * Stop the venice server listen on given port.
   *
   * @return the replicas which are effected after stopping this server.
   */
  public synchronized List<Replica> stopVeniceServer(int port) {
    Admin admin = getLeaderVeniceController().getVeniceAdmin();
    List<Replica> effectedReplicas = admin.getReplicasOfStorageNode(clusterName, Utils.getHelixNodeIdentifier(port));
    stopVeniceComponent(veniceServerWrappers, port);
    return effectedReplicas;
  }

  /**
   * Stop and remove a venice server from the cluster
   * @param port Port number that the server is listening on.
   * @return
   */
  public synchronized List<Replica> removeVeniceServer(int port) {
    List<Replica> effectedReplicas = stopVeniceServer(port);
    IOUtils.closeQuietly(veniceServerWrappers.remove(port));
    return effectedReplicas;
  }

  public synchronized void restartVeniceServer(int port) {
    restartVeniceComponent(veniceServerWrappers, port);
  }

  public synchronized void stopAndRestartVeniceServer(int port) {
    stopVeniceComponent(veniceServerWrappers, port);
    restartVeniceComponent(veniceServerWrappers, port);
  }

  private <T extends ProcessWrapper> void stopVeniceComponent(Map<Integer, T> components, int port) {
    if (components.containsKey(port)) {
      T component = components.get(port);
      try {
        component.stop();
      } catch (Exception e) {
        throw new VeniceException("Can not stop " + component.getClass() + " on port:" + port, e);
      }
    } else {
      throw new VeniceException("Can not find a running venice component on port:" + port);
    }
  }

  private <T extends ProcessWrapper> void restartVeniceComponent(Map<Integer, T> components, int port) {
    if (components.containsKey(port)) {
      T component = components.get(port);
      try {
        component.restart();
      } catch (Exception e) {
        throw new VeniceException("Can not restart " + component.getClass() + " on port:" + port, e);
      }
    } else {
      throw new VeniceException("Can not find a venice component assigned to port:" + port);
    }
  }

  private <T extends ProcessWrapper> T getRandomRunningVeniceComponent(Map<Integer, T> components) {
    Objects.requireNonNull(components, "components map cannot be null");
    if (components.isEmpty()) {
      throw new IllegalArgumentException("components map cannot be empty");
    }
    List<Integer> runningComponentPorts =
        components.values().stream().filter(ProcessWrapper::isRunning).map(T::getPort).collect(Collectors.toList());
    if (runningComponentPorts.isEmpty()) {
      String componentName = components.values().iterator().next().getClass().getSimpleName();
      throw new IllegalArgumentException(
          "components map contains no running " + componentName + " out of the " + components.size() + " provided.");
    }
    int selectedPort = runningComponentPorts.get((int) (Math.random() * runningComponentPorts.size()));
    return components.get(selectedPort);
  }

  /**
   * @deprecated consider using {@link #useControllerClient(Consumer)} instead for guaranteed resource cleanup
   */
  @Deprecated
  public ControllerClient getControllerClient() {
    return new ControllerClient(clusterName, getAllControllersURLs());
  }

  public void useControllerClient(Consumer<ControllerClient> controllerClientConsumer) {
    controllerClientConsumer.accept(controllerClient.get());
  }

  /**
   * Get a venice writer to write string key-value pairs to given version for this cluster.
   * @return
   */
  public VeniceWriter<String, String, byte[]> getVeniceWriter(String storeVersionName) {
    Properties properties = new Properties();
    properties.put(KAFKA_BOOTSTRAP_SERVERS, kafkaBrokerWrapper.getAddress());
    properties.put(ZOOKEEPER_ADDRESS, zkServerWrapper.getAddress());
    VeniceWriterFactory factory = TestUtils.getVeniceWriterFactory(properties);
    String stringSchema = "\"string\"";
    VeniceKafkaSerializer keySerializer = new VeniceAvroKafkaSerializer(stringSchema);
    VeniceKafkaSerializer valueSerializer = new VeniceAvroKafkaSerializer(stringSchema);

    return factory.createVeniceWriter(storeVersionName, keySerializer, valueSerializer);
  }

  public VeniceWriter<String, String, byte[]> getSslVeniceWriter(String storeVersionName) {
    Properties properties = new Properties();
    properties.put(KAFKA_BOOTSTRAP_SERVERS, kafkaBrokerWrapper.getSSLAddress());
    properties.put(ZOOKEEPER_ADDRESS, zkServerWrapper.getAddress());
    properties.putAll(KafkaSSLUtils.getLocalKafkaClientSSLConfig());
    VeniceWriterFactory factory = TestUtils.getVeniceWriterFactory(properties);

    String stringSchema = "\"string\"";
    VeniceKafkaSerializer keySerializer = new VeniceAvroKafkaSerializer(stringSchema);
    VeniceKafkaSerializer valueSerializer = new VeniceAvroKafkaSerializer(stringSchema);

    return factory.createVeniceWriter(storeVersionName, keySerializer, valueSerializer);
  }

  /**
   * Create a new store and a version for that store
   * uses "string" as both key and value schemas
   * @return
   */
  public VersionCreationResponse getNewStoreVersion() {
    return getNewStoreVersion("\"string\"", "\"string\"", true);
  }

  public VersionCreationResponse getNewStoreVersion(String keySchema, String valueSchema) {
    return getNewStoreVersion(keySchema, valueSchema, true);
  }

  public VersionCreationResponse getNewStoreVersion(String keySchema, String valueSchema, boolean sendStartOfPush) {
    String storeName = Utils.getUniqueString("venice-store");
    String storeOwner = Utils.getUniqueString("store-owner");
    long storeSize = 1024;

    // Create new store
    NewStoreResponse newStoreResponse =
        assertCommand(controllerClient.get().createNewStore(storeName, storeOwner, keySchema, valueSchema));
    // Create new version
    return assertCommand(
        controllerClient.get()
            .requestTopicForWrites(
                storeName,
                storeSize,
                Version.PushType.BATCH,
                Version.guidBasedDummyPushId(),
                sendStartOfPush,
                false,
                false,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                false,
                -1));
  }

  public NewStoreResponse getNewStore(String storeName) {
    return getNewStore(storeName, "\"string\"", "\"string\"");
  }

  public NewStoreResponse getNewStore(String storeName, String keySchema, String valueSchema) {
    return assertCommand(
        controllerClient.get().createNewStore(storeName, getClass().getName(), keySchema, valueSchema));
  }

  public VersionCreationResponse getNewVersion(String storeName, int dataSize) {
    return getNewVersion(storeName, dataSize, true);
  }

  public VersionCreationResponse getNewVersion(String storeName, int dataSize, boolean sendStartOfPush) {
    return assertCommand(
        controllerClient.get()
            .requestTopicForWrites(
                storeName,
                dataSize,
                Version.PushType.BATCH,
                Version.guidBasedDummyPushId(),
                sendStartOfPush,
                // This function is expected to be called by tests that bypass the push job and write data directly,
                // therefore, it's safe to assume that it'll be written in arbitrary order, rather than sorted...
                false,
                false,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                false,
                -1));
  }

  public ControllerResponse updateStore(String storeName, UpdateStoreQueryParams params) {
    return assertCommand(controllerClient.get().updateStore(storeName, params));
  }

  public static final String DEFAULT_KEY_SCHEMA = "\"int\"";
  public static final String DEFAULT_VALUE_SCHEMA = "\"int\"";

  public String createStore(int keyCount) {
    int nextVersionId = 1;
    return createStore(IntStream.range(0, keyCount).mapToObj(i -> new AbstractMap.SimpleEntry<>(i, nextVersionId)));
  }

  // Pass the dictionary and the training samples as well
  public String createStoreWithZstdDictionary(int keyCount) {

    return createStore(
        DEFAULT_KEY_SCHEMA,
        "\"string\"",
        IntStream.range(0, keyCount).mapToObj(i -> new AbstractMap.SimpleEntry<>(i, i + "val")),
        CompressionStrategy.ZSTD_WITH_DICT,
        topic -> {
          ZstdDictTrainer trainer = new ZstdDictTrainer(1 * BYTES_PER_MB, 10 * BYTES_PER_KB);
          for (int i = 0; i < 100000; i++) {
            trainer.addSample((i + "val").getBytes(StandardCharsets.UTF_8));
          }
          byte[] compressionDictionaryBytes = trainer.trainSamples();
          return ByteBuffer.wrap(compressionDictionaryBytes);
        });
  }

  public String createStore(Stream<Map.Entry> batchData) {
    return createStore(DEFAULT_KEY_SCHEMA, DEFAULT_VALUE_SCHEMA, batchData, CompressionStrategy.NO_OP, null);
  }

  public String createStore(int keyCount, GenericRecord record) {
    return createStore(
        DEFAULT_KEY_SCHEMA,
        record.getSchema().toString(),
        IntStream.range(0, keyCount).mapToObj(i -> new AbstractMap.SimpleEntry<>(i, record)),
        CompressionStrategy.NO_OP,
        null);
  }

  public String createStore(String keySchema, String valueSchema, Stream<Map.Entry> batchData) {
    return createStore(keySchema, valueSchema, batchData, CompressionStrategy.NO_OP, null);
  }

  public String createStore(
      String keySchema,
      String valueSchema,
      Stream<Map.Entry> batchData,
      CompressionStrategy compressionStrategy,
      Function<String, ByteBuffer> compressionDictionaryGenerator) {
    String storeName = Utils.getUniqueString("store");
    assertCommand(controllerClient.get().createNewStore(storeName, getClass().getName(), keySchema, valueSchema));
    if (compressionStrategy == CompressionStrategy.ZSTD_WITH_DICT && compressionDictionaryGenerator != null) {
      updateStore(storeName, new UpdateStoreQueryParams().setCompressionStrategy(CompressionStrategy.ZSTD_WITH_DICT));
    } else if (compressionStrategy == CompressionStrategy.GZIP) {
      updateStore(storeName, new UpdateStoreQueryParams().setCompressionStrategy(CompressionStrategy.GZIP));
    }
    createVersion(storeName, keySchema, valueSchema, batchData, compressionStrategy, compressionDictionaryGenerator);
    return storeName;
  }

  public int createVersion(String storeName, int keyCount) {
    StoreResponse response = assertCommand(controllerClient.get().getStore(storeName));
    int nextVersionId = response.getStore().getLargestUsedVersionNumber() + 1;
    return createVersion(
        storeName,
        IntStream.range(0, keyCount).mapToObj(i -> new AbstractMap.SimpleEntry<>(i, nextVersionId)));
  }

  public int createVersion(String storeName, Stream<Map.Entry> batchData) {
    return createVersion(storeName, DEFAULT_KEY_SCHEMA, DEFAULT_VALUE_SCHEMA, batchData);
  }

  public int createVersion(String storeName, String keySchema, String valueSchema, Stream<Map.Entry> batchData) {
    return createVersion(storeName, keySchema, valueSchema, batchData, CompressionStrategy.NO_OP, null);
  }

  public int createVersion(
      String storeName,
      String keySchema,
      String valueSchema,
      Stream<Map.Entry> batchData,
      CompressionStrategy compressionStrategy,
      Function<String, ByteBuffer> compressionDictionaryGenerator) {
    VersionCreationResponse response = assertCommand(
        controllerClient.get()
            .requestTopicForWrites(
                storeName,
                1024, // estimate of the version size in bytes
                Version.PushType.BATCH,
                Version.guidBasedDummyPushId(),
                compressionStrategy == CompressionStrategy.NO_OP,
                false,
                false,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                false,
                -1));

    writeBatchData(
        response,
        keySchema,
        valueSchema,
        batchData,
        HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID,
        compressionStrategy,
        compressionDictionaryGenerator);

    int versionId = response.getVersion();
    waitVersion(storeName, versionId, controllerClient.get());
    return versionId;
  }

  public void waitVersion(String storeName, int versionId) {
    waitVersion(storeName, versionId, controllerClient.get());
  }

  public void waitVersion(String storeName, int versionId, ControllerClient client) {
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
      String kafkaTopic = Version.composeKafkaTopic(storeName, versionId);
      JobStatusQueryResponse response = TestUtils.assertCommand(client.queryJobStatus(kafkaTopic));
      if (response.getStatus().equals(ExecutionStatus.ERROR.toString())) {
        throw new VeniceException("Unexpected push failure, kafkaTopic=" + kafkaTopic);
      }

      StoreResponse storeResponse = TestUtils.assertCommand(client.getStore(storeName));
      Assert.assertEquals(
          storeResponse.getStore().getCurrentVersion(),
          versionId,
          "The current version of store " + storeName + " does not have the expected value of '" + versionId + "'.");
    });
    refreshAllRouterMetaData();
    LOGGER.info("Finished waiting for version {} of store {} to become available.", versionId, storeName);
  }

  /**
   * Having a main func here to be called by {@link #generateServiceInAnotherProcess}
   * to spawn a testing cluster in another process if one wants an isolated environment, e.g. for benchmark
   * @param args - args[0] (cluster info file path) is the only and must have parameter
   *             to work as IPC to pass back needed cluster info
   */
  public static void main(String[] args) throws IOException {
    if (args.length != 1) {
      throw new VeniceException("Need to provide a file path to write cluster info.");
    }
    /**
     * write some cluster info to a file, which will be used by another process to make connection to this cluster
     * e.g. {@link com.linkedin.venice.benchmark.IngestionBenchmarkWithTwoProcesses#parseClusterInfoFile()}
     */
    String clusterInfoConfigPath = args[0];
    PropertyBuilder propertyBuilder = new PropertyBuilder();
    File configFile = new File(clusterInfoConfigPath);

    try {

      int numberOfPartitions = 16;

      Utils.thisIsLocalhost();
      Properties extraProperties = new Properties();
      extraProperties.put(DEFAULT_MAX_NUMBER_OF_PARTITIONS, numberOfPartitions);
      VeniceClusterWrapper veniceClusterWrapper =
          ServiceFactory.getVeniceCluster(1, 1, 1, 1, 10 * 1024 * 1024, false, false, extraProperties);

      String storeName = Utils.getUniqueString("storeForMainMethodOf" + VeniceClusterWrapper.class.getSimpleName());
      String controllerUrl = veniceClusterWrapper.getRandomVeniceController().getControllerUrl();
      String KEY_SCHEMA = Schema.create(Schema.Type.STRING).toString();
      String VALUE_SCHEMA = Schema.create(Schema.Type.STRING).toString();
      File inputDir = TestPushUtils.getTempDataDirectory();

      TestPushUtils.writeSimpleAvroFileWithCustomSize(inputDir, NUM_RECORDS, 10, 20);

      try (ControllerClient client = new ControllerClient(veniceClusterWrapper.clusterName, controllerUrl)) {
        TestUtils.assertCommand(client.createNewStore(storeName, "ownerOf" + storeName, KEY_SCHEMA, VALUE_SCHEMA));

        TestUtils.assertCommand(
            client.updateStore(
                storeName,
                new UpdateStoreQueryParams().setLeaderFollowerModel(true)
                    .setPartitionCount(numberOfPartitions)
                    .setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)));
      }

      String inputDirPath = "file://" + inputDir.getAbsolutePath();
      Properties props = defaultVPJProps(controllerUrl, inputDirPath, storeName);
      TestPushUtils.runPushJob("Test Batch push job", props);

      propertyBuilder.put(FORKED_PROCESS_STORE_NAME, storeName);
      propertyBuilder.put(FORKED_PROCESS_ZK_ADDRESS, veniceClusterWrapper.getZk().getAddress());
      // Store properties into config file.
      propertyBuilder.build().storeFlattened(configFile);
      LOGGER.info("Configs are stored into: {}", clusterInfoConfigPath);
    } catch (Exception e) {
      propertyBuilder.put(FORKED_PROCESS_EXCEPTION, ExceptionUtils.stackTraceToString(e));
      propertyBuilder.build().storeFlattened(configFile);
      LOGGER.info("Exception stored into: {}", clusterInfoConfigPath);
      throw new VeniceException(e);
    }
  }
}
