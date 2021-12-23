package com.linkedin.venice.integration.utils;

import com.linkedin.venice.client.store.ClientConfig;
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
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.helix.Replica;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.utils.ForkedJavaProcess;
import com.linkedin.venice.utils.KafkaSSLUtils;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.io.File;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.testng.Assert;

import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.integration.utils.VeniceServerWrapper.*;


/**
 * This is the whole enchilada:
 * - {@link ZkServerWrapper}
 * - {@link KafkaBrokerWrapper}
 * - {@link VeniceControllerWrapper}
 * - {@link VeniceServerWrapper}
 */
public class VeniceClusterWrapper extends ProcessWrapper {
  public static final Logger logger = Logger.getLogger(VeniceClusterWrapper.class);
  public static final String SERVICE_NAME = "VeniceCluster";

  private static final int VALUE_LENGTH = 100;
  private static final int NUM_RECORDS = 100_000;
  private static final String FLOAT_VECTOR_VALUE_SCHEMA = "{" +
          "  \"namespace\" : \"example.avro\",  " +
          "  \"type\": \"record\",   " +
          "  \"name\": \"FloatVector\",     " +
          "  \"fields\": [           " +
          "       { \"name\": \"value\", \"type\": {\"type\": \"array\", \"items\": \"float\"} }  " +
          "  ] " +
          " } ";

  private final String clusterName;
  private final boolean standalone;
  private final ZkServerWrapper zkServerWrapper;
  private final KafkaBrokerWrapper kafkaBrokerWrapper;
  private final BrooklinWrapper brooklinWrapper;

  private final int defaultReplicaFactor;
  private final int defaultPartitionSize;
  private final long defaultDelayToRebalanceMS;
  private final int defaultMinActiveReplica;
  private final Map<Integer, VeniceControllerWrapper> veniceControllerWrappers;
  private final Map<Integer, VeniceServerWrapper> veniceServerWrappers;
  private final Map<Integer, VeniceRouterWrapper> veniceRouterWrappers;
  private final boolean sslToStorageNodes;
  private final boolean sslToKafka;

  private static Process veniceClusterProcess;
  // Controller discovery URLs are controllers that's created outside of this cluster wrapper but are overseeing the
  // cluster. e.g. controllers in a multi cluster wrapper.
  private String externalControllerDiscoveryURL = "";

  private static final List<AvroProtocolDefinition> CLUSTER_LEADER_INITIALIZATION_ROUTINES = Arrays.asList(
      AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE,
      AvroProtocolDefinition.PARTITION_STATE,
      AvroProtocolDefinition.STORE_VERSION_STATE,
      AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE,
      AvroProtocolDefinition.PUSH_STATUS_SYSTEM_SCHEMA_STORE
  );

  private static final AvroProtocolDefinition[] hybridRequiredSystemStores = new AvroProtocolDefinition[]{
      AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE, AvroProtocolDefinition.PUSH_STATUS_SYSTEM_SCHEMA_STORE};
  private static final Set<AvroProtocolDefinition> hybridRequiredSystemStoresSet =
      new HashSet<>(Arrays.asList(hybridRequiredSystemStores));

  VeniceClusterWrapper(
      String clusterName,
      boolean standalone,
      ZkServerWrapper zkServerWrapper,
      KafkaBrokerWrapper kafkaBrokerWrapper,
      BrooklinWrapper brooklinWrapper,
      Map<Integer, VeniceControllerWrapper> veniceControllerWrappers,
      Map<Integer, VeniceServerWrapper> veniceServerWrappers,
      Map<Integer, VeniceRouterWrapper> veniceRouterWrappers,
      int defaultReplicaFactor,
      int defaultPartitionSize,
      long defaultDelayToRebalanceMS,
      int mintActiveReplica,
      boolean sslToStorageNodes,
      boolean sslToKafka) {

    super(SERVICE_NAME, null);
    this.standalone = standalone;
    this.clusterName = clusterName;
    this.zkServerWrapper = zkServerWrapper;
    this.kafkaBrokerWrapper = kafkaBrokerWrapper;
    this.brooklinWrapper = brooklinWrapper;
    this.veniceControllerWrappers = veniceControllerWrappers;
    this.veniceServerWrappers = veniceServerWrappers;
    this.veniceRouterWrappers = veniceRouterWrappers;
    this.defaultReplicaFactor = defaultReplicaFactor;
    this.defaultPartitionSize = defaultPartitionSize;
    this.defaultDelayToRebalanceMS = defaultDelayToRebalanceMS;
    this.defaultMinActiveReplica = mintActiveReplica;
    this.sslToStorageNodes = sslToStorageNodes;
    this.sslToKafka = sslToKafka;
  }

  static ServiceProvider<VeniceClusterWrapper> generateService(
      String coloName,
      boolean standalone,
      ZkServerWrapper zkServerWrapper,
      KafkaBrokerWrapper kafkaBrokerWrapper,
      BrooklinWrapper brooklinWrapper,
      String clusterName,
      String clusterToD2,
      int numberOfControllers,
      int numberOfServers,
      int numberOfRouters,
      int replicationFactor,
      int partitionSize,
      boolean enableWhitelist,
      boolean enableAutoJoinWhitelist,
      long rebalanceDelayMs,
      int minActiveReplica,
      boolean sslToStorageNodes,
      boolean sslToKafka,
      boolean isKafkaOpenSSLEnabled,
      Properties extraProperties,
      boolean forkServer,
      Optional<Map<String, Map<String, String>>> kafkaClusterMap
  ) {

    Map<Integer, VeniceControllerWrapper> veniceControllerWrappers = new HashMap<>();
    Map<Integer, VeniceServerWrapper> veniceServerWrappers = new HashMap<>();
    Map<Integer, VeniceRouterWrapper> veniceRouterWrappers = new HashMap<>();
    try {
      // Setup D2 for controller
      String zkAddress = zkServerWrapper.getAddress();
      D2TestUtils.setupD2Config(zkAddress, false, D2TestUtils.CONTROLLER_CLUSTER_NAME, D2TestUtils.CONTROLLER_SERVICE_NAME, false);
      for (int i = 0; i < numberOfControllers; i++) {
        if (numberOfRouters > 0) {
          ClientConfig clientConfig = new ClientConfig().setVeniceURL(zkAddress)
              .setD2ServiceName(D2TestUtils.getD2ServiceName(clusterToD2, clusterName))
              .setSslEngineComponentFactory(SslUtils.getLocalSslFactory())
              .setStoreName("dummy");
          extraProperties.put(CLIENT_CONFIG_FOR_CONSUMER, clientConfig);
        }
        VeniceControllerWrapper veniceControllerWrapper =
            ServiceFactory.getVeniceController(new String[]{clusterName}, kafkaBrokerWrapper, replicationFactor, partitionSize,
                rebalanceDelayMs, minActiveReplica, brooklinWrapper, clusterToD2, sslToKafka, true, extraProperties);
        veniceControllerWrappers.put(veniceControllerWrapper.getPort(), veniceControllerWrapper);
      }

      for (int i = 0; i < numberOfRouters; i++) {
        VeniceRouterWrapper veniceRouterWrapper =
            ServiceFactory.getVeniceRouter(clusterName, kafkaBrokerWrapper, sslToStorageNodes, clusterToD2, extraProperties);
        veniceRouterWrappers.put(veniceRouterWrapper.getPort(), veniceRouterWrapper);
      }

      for (int i = 0; i < numberOfServers; i++) {
        Properties featureProperties = new Properties();
        featureProperties.setProperty(SERVER_ENABLE_SERVER_WHITE_LIST, Boolean.toString(enableWhitelist));
        featureProperties.setProperty(SERVER_IS_AUTO_JOIN, Boolean.toString(enableAutoJoinWhitelist));
        featureProperties.setProperty(SERVER_ENABLE_SSL, Boolean.toString(sslToStorageNodes));
        featureProperties.setProperty(SERVER_SSL_TO_KAFKA, Boolean.toString(sslToKafka));
        if (!veniceRouterWrappers.isEmpty()) {
          ClientConfig clientConfig = new ClientConfig()
              .setVeniceURL(zkAddress)
              .setD2ServiceName(D2TestUtils.getD2ServiceName(clusterToD2, clusterName))
              .setSslEngineComponentFactory(SslUtils.getLocalSslFactory());
          featureProperties.put(CLIENT_CONFIG_FOR_CONSUMER, clientConfig);
        }
        featureProperties.setProperty(SERVER_ENABLE_KAFKA_OPENSSL, Boolean.toString(isKafkaOpenSSLEnabled));

        String serverName = "";
        if (!coloName.isEmpty() && !clusterName.isEmpty()) {
          serverName = coloName + ":" + clusterName + ":sn-" + i;
        }
        VeniceServerWrapper veniceServerWrapper =
            ServiceFactory.getVeniceServer(clusterName, kafkaBrokerWrapper, zkAddress, featureProperties, extraProperties, forkServer, serverName, kafkaClusterMap);
        veniceServerWrappers.put(veniceServerWrapper.getPort(), veniceServerWrapper);
      }

      /**
       * We get the various dependencies outside of the lambda, to avoid having a time
       * complexity of O(N^2) on the amount of retries. The calls have their own retries,
       * so we can assume they're reliable enough.
       */
      return (serviceName) -> {
        VeniceClusterWrapper veniceClusterWrapper = null;
        try {
          veniceClusterWrapper = new VeniceClusterWrapper(clusterName, standalone, zkServerWrapper, kafkaBrokerWrapper, brooklinWrapper,
              veniceControllerWrappers, veniceServerWrappers, veniceRouterWrappers, replicationFactor, partitionSize,
              rebalanceDelayMs, minActiveReplica, sslToStorageNodes, sslToKafka);
          // Wait for all the asynchronous ClusterLeaderInitializationRoutine to complete before returning the
          // VeniceClusterWrapper to tests.
          if (!veniceClusterWrapper.getVeniceControllers().isEmpty()) {
            final VeniceClusterWrapper finalClusterWrapper = veniceClusterWrapper;
            for (AvroProtocolDefinition avroProtocolDefinition : CLUSTER_LEADER_INITIALIZATION_ROUTINES) {
              TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
                Store store = finalClusterWrapper.getMasterVeniceController().getVeniceAdmin().getStore(clusterName,
                    avroProtocolDefinition.getSystemStoreName());
                Assert.assertNotNull(store, "Store: " + avroProtocolDefinition.getSystemStoreName()
                    + " should be initialized by " + ClusterLeaderInitializationRoutine.class.getSimpleName());
                if (hybridRequiredSystemStoresSet.contains(avroProtocolDefinition)) {
                  // Check against the HelixReadOnlyZKSharedSystemStoreRepository instead of the
                  // ReadWriteStoreRepository because of the way we implemented getStore for meta system stores in
                  // HelixReadOnlyStoreRepositoryAdapter.
                  Store readOnlyStore = finalClusterWrapper.getMasterVeniceController().getVeniceAdmin()
                      .getReadOnlyZKSharedSystemStoreRepository().getStore(avroProtocolDefinition.getSystemStoreName());
                  Assert.assertNotNull(readOnlyStore, "Store: " + avroProtocolDefinition.getSystemStoreName()
                      + "should be initialized by " + ClusterLeaderInitializationRoutine.class.getSimpleName());
                  Assert.assertTrue(readOnlyStore.isHybrid(), "Store: " + avroProtocolDefinition.getSystemStoreName()
                      + " should be configured to hybrid by " + ClusterLeaderInitializationRoutine.class.getSimpleName()
                      +  ". Store is hybrid in write repo: " + store.isHybrid());
                }
              });
            }
          }
        } catch (Throwable e) {
          logger.error("Caught Throwable while creating the " + VeniceClusterWrapper.class.getSimpleName(), e);
          IOUtils.closeQuietly(veniceClusterWrapper);
          throw e;
        }
        return veniceClusterWrapper;
      };

    } catch (Exception e) {
      veniceRouterWrappers.values().forEach(IOUtils::closeQuietly);
      veniceServerWrappers.values().forEach(IOUtils::closeQuietly);
      veniceControllerWrappers.values().forEach(IOUtils::closeQuietly);
      throw e;
    }
  }

  static ServiceProvider<VeniceClusterWrapper> generateService(
      String clusterName,
      int numberOfControllers,
      int numberOfServers,
      int numberOfRouters,
      int replicationFactor,
      int partitionSize,
      boolean enableWhitelist,
      boolean enableAutoJoinWhitelist,
      long rebalanceDelayMs,
      int minActiveReplica,
      boolean sslToStorageNodes,
      boolean sslToKafka,
      boolean isKafkaOpenSSLEnabled,
      Properties extraProperties) {

    ZkServerWrapper zkServerWrapper = null;
    KafkaBrokerWrapper kafkaBrokerWrapper = null;
    BrooklinWrapper brooklinWrapper = null;
    try {
      zkServerWrapper = ServiceFactory.getZkServer();
      kafkaBrokerWrapper = ServiceFactory.getKafkaBroker(zkServerWrapper);
      brooklinWrapper = ServiceFactory.getBrooklinWrapper(kafkaBrokerWrapper);

      /**
       * We get the various dependencies outside of the lambda, to avoid having a time
       * complexity of O(N^2) on the amount of retries. The calls have their own retries,
       * so we can assume they're reliable enough.
       */
      return generateService(
          "",
          true,
          zkServerWrapper,
          kafkaBrokerWrapper,
          brooklinWrapper,
          clusterName,
          null,
          numberOfControllers,
          numberOfServers,
          numberOfRouters,
          replicationFactor,
          partitionSize,
          enableWhitelist,
          enableAutoJoinWhitelist,
          rebalanceDelayMs,
          minActiveReplica,
          sslToStorageNodes,
          sslToKafka,
          isKafkaOpenSSLEnabled,
          extraProperties, false, Optional.empty());

    } catch (Exception e) {
      IOUtils.closeQuietly(brooklinWrapper);
      IOUtils.closeQuietly(kafkaBrokerWrapper);
      IOUtils.closeQuietly(zkServerWrapper);
      throw e;
    }
  }

  static synchronized void generateServiceInAnotherProcess(String clusterInfoFilePath, int waitTimeInSeconds) throws IOException, InterruptedException {
    if (veniceClusterProcess != null) {
      logger.warn("Received a request to spawn a venice cluster in another process for testing" +
              "but one has already been running. Will not spawn a new one.");
      return;
    }

    veniceClusterProcess = ForkedJavaProcess.exec(VeniceClusterWrapper.class, clusterInfoFilePath);

    try {
      // wait some time to make sure all the services have started in the forked process
      if (veniceClusterProcess.waitFor(waitTimeInSeconds, TimeUnit.SECONDS)) {
        veniceClusterProcess.destroy();
        throw new VeniceException("Venice cluster exited unexpectedly with the code " + veniceClusterProcess.exitValue());
      }
    } catch (InterruptedException e) {
      logger.warn("Waiting for veniceClusterProcess to start is interrupted", e);
      Thread.currentThread().interrupt();
      return;
    }
    logger.info("Venice cluster is started in a remote process!");
  }

  static synchronized void stopServiceInAnotherProcess() {
    veniceClusterProcess.destroy();
    veniceClusterProcess = null;
  }

  private static String buildFloatVectorStore(VeniceClusterWrapper cluster) {
    Schema schema = Schema.parse(FLOAT_VECTOR_VALUE_SCHEMA);
    GenericRecord record = new GenericData.Record(schema);
    List<Float> floatVector = new ArrayList<>();
    for (int i = 0; i < VALUE_LENGTH; i++) {
      floatVector.add((float)(i * 1.0));
    }
    record.put("value", floatVector);
    try {
      return cluster.createStore(NUM_RECORDS, record);
    } catch (Exception e) {
      throw new VeniceException("Error in creating store", e);
    }
  }

  @Override
  protected void internalStart() throws Exception {
    // Everything should already be started. So this is a no-op.
  }

  @Override
  protected void internalStop() throws Exception {
    veniceRouterWrappers.values().forEach(IOUtils::closeQuietly);
    veniceServerWrappers.values().forEach(IOUtils::closeQuietly);
    veniceControllerWrappers.values().forEach(IOUtils::closeQuietly);
    if (standalone) {
      IOUtils.closeQuietly(brooklinWrapper);
      IOUtils.closeQuietly(kafkaBrokerWrapper);
      IOUtils.closeQuietly(zkServerWrapper);
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
    veniceRouterWrappers.values().stream()
        .filter(ProcessWrapper::isRunning)
        .forEach(VeniceRouterWrapper::refresh);
  }

  public synchronized VeniceControllerWrapper getRandmonVeniceController() {
    return getRandomRunningVeniceComponent(veniceControllerWrappers);
  }

  public void setExternalControllerDiscoveryURL(String externalControllerDiscoveryURL) {
    this.externalControllerDiscoveryURL = externalControllerDiscoveryURL;
  }

  public synchronized String getAllControllersURLs() {
    return veniceControllerWrappers.isEmpty() ?
        externalControllerDiscoveryURL : veniceControllerWrappers.values().stream()
        .map(VeniceControllerWrapper::getControllerUrl)
        .collect(Collectors.joining(","));
  }

  public VeniceControllerWrapper getMasterVeniceController() {
    return getMasterVeniceController(60 * Time.MS_PER_SECOND);
  }

  public synchronized VeniceControllerWrapper getMasterVeniceController(long timeoutMs) {
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    while (System.nanoTime() < deadline) {
      for (VeniceControllerWrapper controller : veniceControllerWrappers.values()) {
        if (controller.isRunning() && controller.isMasterController(clusterName)) {
          return controller;
        }
      }
      Utils.sleep(Time.MS_PER_SECOND);
    }
    throw new VeniceException("Master controller does not exist, cluster=" + clusterName);
  }

  public VeniceControllerWrapper addVeniceController(Properties properties) {
    VeniceControllerWrapper veniceControllerWrapper =
        ServiceFactory.getVeniceController(new String[]{clusterName}, kafkaBrokerWrapper, defaultReplicaFactor, defaultPartitionSize,
            defaultDelayToRebalanceMS, defaultMinActiveReplica, null, null, sslToKafka, false, properties);
    synchronized (this) {
      veniceControllerWrappers.put(veniceControllerWrapper.getPort(), veniceControllerWrapper);
      setExternalControllerDiscoveryURL(veniceControllerWrappers.values().stream()
          .map(VeniceControllerWrapper::getControllerUrl).collect(Collectors.joining(",")));
    }
    return veniceControllerWrapper;
  }

  public void addVeniceControllerWrapper(VeniceControllerWrapper veniceControllerWrapper) {
    synchronized (this) {
      veniceControllerWrappers.put(veniceControllerWrapper.getPort(), veniceControllerWrapper);
      setExternalControllerDiscoveryURL(veniceControllerWrappers.values().stream()
          .map(VeniceControllerWrapper::getControllerUrl).collect(Collectors.joining(",")));
    }
  }

  public VeniceRouterWrapper addVeniceRouter(Properties properties) {
    VeniceRouterWrapper veniceRouterWrapper = ServiceFactory.getVeniceRouter(clusterName, kafkaBrokerWrapper, sslToStorageNodes, properties);
    synchronized (this) {
      veniceRouterWrappers.put(veniceRouterWrapper.getPort(), veniceRouterWrapper);
    }
    return veniceRouterWrapper;
  }

  /**
   * @deprecated Future use should consider {@link #addVeniceServer(Properties, Properties)}
   *
   * @param enableWhitelist
   * @param enableAutoJoinWhiteList
   * @return
   */
  public VeniceServerWrapper addVeniceServer(boolean enableWhitelist, boolean enableAutoJoinWhiteList) {
    Properties featureProperties = new Properties();
    featureProperties.setProperty(SERVER_ENABLE_SERVER_WHITE_LIST, Boolean.toString(enableWhitelist));
    featureProperties.setProperty(SERVER_IS_AUTO_JOIN, Boolean.toString(enableAutoJoinWhiteList));
    VeniceServerWrapper veniceServerWrapper =
        ServiceFactory.getVeniceServer(clusterName, kafkaBrokerWrapper, getKafka().getZkAddress(), featureProperties, new Properties());
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
    VeniceServerWrapper veniceServerWrapper = ServiceFactory.getVeniceServer(clusterName, kafkaBrokerWrapper, getKafka().getZkAddress(), new Properties(), properties);
    synchronized (this) {
      veniceServerWrappers.put(veniceServerWrapper.getPort(), veniceServerWrapper);
    }
    return veniceServerWrapper;
  }

  public VeniceServerWrapper addVeniceServer(Properties featureProperties, Properties configProperties) {
    VeniceServerWrapper veniceServerWrapper = ServiceFactory.getVeniceServer(clusterName, kafkaBrokerWrapper, getKafka().getZkAddress(), featureProperties, configProperties);
    synchronized (this) {
      veniceServerWrappers.put(veniceServerWrapper.getPort(), veniceServerWrapper);
    }
    return veniceServerWrapper;
  }

  /**
   * Find the master controller, stop it and return its port.
   * @return
   */
  public synchronized int stopMasterVeniceControler() {
    try {
      VeniceControllerWrapper masterController = getMasterVeniceController();
      int port = masterController.getPort();
      masterController.stop();
      return port;
    } catch (Exception e) {
      throw new VeniceException("Can not stop master controller.", e);
    }
  }

  public synchronized void stopVeniceController(int port) {
    stopVeniceComponent(veniceControllerWrappers, port);
  }

  public synchronized void restartVeniceController(int port) {
    restartVeniceComponent(veniceControllerWrappers, port);
  }

  public synchronized void removeVeniceController(int port) {
    stopVeniceController(port);
    IOUtils.closeQuietly(veniceControllerWrappers.remove(port));
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
    Admin admin = getMasterVeniceController().getVeniceAdmin();
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

  /**
   * Find the venice servers which has been assigned to the given resource and partition.
   * After getting these servers, you can fail some of them to simulate the server failure. Otherwise you might not
   * know which server you should fail.
   */
  public synchronized List<VeniceServerWrapper> findVeniceServer(String resourceName, int partition, HelixState state) {
    Admin admin = getMasterVeniceController().getVeniceAdmin();

    List<Replica> replicas = admin.getReplicas(clusterName, resourceName);
    List<VeniceServerWrapper> result = new ArrayList<>();
    for (Replica replica : replicas) {
      if (replica.getPartitionId() == partition && replica.getStatus().equals(state.toString())) {
        int port = replica.getInstance().getPort();
        if (veniceServerWrappers.containsKey(port)) {
          result.add(veniceServerWrappers.get(port));
        } else {
          throw new VeniceException("Can not find a venice server on port:" + port);
        }
      }
    }
    return result;
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

  private synchronized <T extends ProcessWrapper> T getRandomRunningVeniceComponent(Map<Integer, T> components) {
    List<Integer> runningComponentPorts = components.values()
        .stream()
        .filter(component -> component.isRunning())
        .map(T::getPort)
        .collect(Collectors.toList());
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
    try (ControllerClient controllerClient = getControllerClient()) {
      controllerClientConsumer.accept(controllerClient);
    }
  }

  /**
   * Get a venice writer to write string key-value pairs to given version for this cluster.
   * @return
   */
  public VeniceWriter<String, String, byte[]> getVeniceWriter(String storeVersionName) {
    Properties properties = new Properties();
    properties.put(KAFKA_BOOTSTRAP_SERVERS, kafkaBrokerWrapper.getAddress());
    properties.put(ZOOKEEPER_ADDRESS, zkServerWrapper.getAddress());
    properties.put(CLUSTER_NAME, clusterName);
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
    properties.put(CLUSTER_NAME, clusterName);
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
    return getNewStoreVersion("\"string\"", "\"string\"");
  }

  public VersionCreationResponse getNewStoreVersion(String keySchema, String valueSchema) {
    String storeName = Utils.getUniqueString("venice-store");
    String storeOwner = Utils.getUniqueString("store-owner");
    long storeSize =  1024;

    try (ControllerClient controllerClient = getControllerClient()) {
      // Create new store
      NewStoreResponse newStoreResponse = controllerClient.createNewStore(storeName, storeOwner, keySchema, valueSchema);
      if (newStoreResponse.isError()) {
        throw new VeniceException(newStoreResponse.getError());
      }
      // Create new version
      VersionCreationResponse newVersion =
          controllerClient.requestTopicForWrites(storeName, storeSize, Version.PushType.BATCH,
              Version.guidBasedDummyPushId(), false, false, false, Optional.empty(),
              Optional.empty(), Optional.empty(), false, -1);
      if (newVersion.isError()) {
        throw new VeniceException(newVersion.getError());
      }
      return newVersion;
    }
  }

  public NewStoreResponse getNewStore(String storeName) {
    String keySchema = "\"string\"";
    String valueSchema = "\"string\"";
    try (ControllerClient controllerClient = getControllerClient()) {
      NewStoreResponse response = controllerClient.createNewStore(storeName, getClass().getName(), keySchema, valueSchema);
      if (response.isError()) {
        throw new VeniceException(response.getError());
      }
      return response;
    }
  }

  public VersionCreationResponse getNewVersion(String storeName, int dataSize) {
    try (ControllerClient controllerClient = getControllerClient()) {
      VersionCreationResponse newVersion =
          controllerClient.requestTopicForWrites(
              storeName,
              dataSize,
              Version.PushType.BATCH,
              Version.guidBasedDummyPushId(),
              false,
              // This function is expected to be called by tests that bypass the push job and write data directly,
              // therefore, it's safe to assume that it'll be written in arbitrary order, rather than sorted...
              false,
              false,
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              false,
              -1);
      if (newVersion.isError()) {
        throw new VeniceException(newVersion.getError());
      }
      return newVersion;
    }
  }

  public ControllerResponse updateStore(String storeName, UpdateStoreQueryParams params) {
    try (ControllerClient controllerClient = getControllerClient()) {
      ControllerResponse response = controllerClient.updateStore(storeName, params);
      if (response.isError()) {
        throw new VeniceException(response.getError());
      }
      return response;
    }
  }

  public static final String DEFAULT_KEY_SCHEMA = "\"int\"";
  public static final String DEFAULT_VALUE_SCHEMA = "\"int\"";

  public String createStore(int keyCount) throws Exception {
    int nextVersionId = 1;
    return createStore(IntStream.range(0, keyCount).mapToObj(i -> new AbstractMap.SimpleEntry<>(i, nextVersionId)));
  }

  public String createStore(Stream<Map.Entry> batchData) throws Exception {
    return createStore(DEFAULT_KEY_SCHEMA, DEFAULT_VALUE_SCHEMA, batchData);
  }

  public String createStore(int keyCount, GenericRecord record) throws Exception {
    return createStore(DEFAULT_KEY_SCHEMA, record.getSchema().toString(),
        IntStream.range(0, keyCount).mapToObj(i -> new AbstractMap.SimpleEntry<>(i, record))
    );
  }

  public String createStore(String keySchema, String valueSchema, Stream<Map.Entry> batchData) throws Exception {
    try (ControllerClient client = getControllerClient()) {
      String storeName = Utils.getUniqueString("store");
      NewStoreResponse response = client.createNewStore(
          storeName,
          getClass().getName(),
          keySchema,
          valueSchema);
      if (response.isError()) {
        throw new VeniceException(response.getError());
      }

      createVersion(storeName, keySchema, valueSchema, batchData);
      return storeName;
    }
  }

  public int createVersion(String storeName, int keyCount) throws Exception {
    try (ControllerClient client = getControllerClient()) {
      StoreResponse response = client.getStore(storeName);
      if (response.isError()) {
        throw new VeniceException(response.getError());
      }
      int nextVersionId = response.getStore().getLargestUsedVersionNumber() + 1;
      return createVersion(storeName, IntStream.range(0, keyCount).mapToObj(i -> new AbstractMap.SimpleEntry<>(i, nextVersionId)));
    }
  }

  public int createVersion(String storeName, Stream<Map.Entry> batchData) throws Exception {
    return createVersion(storeName, DEFAULT_KEY_SCHEMA, DEFAULT_VALUE_SCHEMA, batchData);
  }

  public int createVersion(String storeName, String keySchema, String valueSchema, Stream<Map.Entry> batchData) throws Exception {
    try (ControllerClient client = getControllerClient()) {
      VersionCreationResponse response = client.requestTopicForWrites(
          storeName,
          1024, // estimate of the version size in bytes
          Version.PushType.BATCH,
          Version.guidBasedDummyPushId(),
          true,
          false,
          false,
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          false,
          -1);
      if (response.isError()) {
        throw new VeniceException(response.getError());
      }
      TestUtils.writeBatchData(response, keySchema, valueSchema, batchData, HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID);

      int versionId = response.getVersion();
      waitVersion(storeName, versionId);
      return versionId;
    }
  }

  public void waitVersion(String storeName, int versionId) {
    try (ControllerClient client = getControllerClient()) {
      TestUtils.waitForNonDeterministicCompletion(30, TimeUnit.SECONDS, () -> {
        String kafkaTopic = Version.composeKafkaTopic(storeName, versionId);
        JobStatusQueryResponse response = client.queryJobStatus(kafkaTopic);
        if (response.isError()) {
          throw new VeniceException(response.getError());
        }
        if (response.getStatus().equals(ExecutionStatus.ERROR.toString())) {
          throw new VeniceException("Unexpected push failure, kafkaTopic=" + kafkaTopic);
        }

        StoreResponse storeResponse = client.getStore(storeName);
        if (storeResponse.isError()) {
          throw new VeniceException(storeResponse.getError());
        }
        return storeResponse.getStore().getCurrentVersion() == versionId;
      });
    }
    refreshAllRouterMetaData();
  }

  /**
   * Having a main func here to be called by {@link #generateServiceInAnotherProcess}
   * to spawn a testing cluster in another process if one wants an isolated environment, e.g. for benchmark
   * @param args - args[0] (cluster info file path) is the only and must have parameter
   *             to work as IPC to pass back needed cluster info
   */
  public static void main(String[] args) {
    if (args.length != 1) {
      throw new VeniceException("Need to provide a file path to write cluster info.");
    }

    Utils.thisIsLocalhost();
    VeniceClusterWrapper veniceClusterWrapper = ServiceFactory.getVeniceCluster();
    /**
     * write some cluster info to a file, which will be used by another process to make connection to this cluster
     * e.g. {@link com.linkedin.venice.benchmark.IngestionBenchmarkWithTwoProcesses#parseClusterInfoFile()}
     */
    String storeName = buildFloatVectorStore(veniceClusterWrapper);
    String zkAddress = veniceClusterWrapper.getZk().getAddress();
    String clusterInfoConfigPath = args[0];
    PropertyBuilder propertyBuilder = new PropertyBuilder();
    propertyBuilder.put("storeName", storeName);
    propertyBuilder.put("zkAddress", zkAddress);
    VeniceProperties veniceProperties = propertyBuilder.build();
    File configFile = new File(clusterInfoConfigPath);
    // Store properties into config file.
    try {
      veniceProperties.storeFlattened(configFile);
      logger.info("Configs are stored into: " + clusterInfoConfigPath);
    } catch (IOException e) {
      throw new VeniceException(e);
    }
  }
}
