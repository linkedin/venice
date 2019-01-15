package com.linkedin.venice.integration.utils;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerApiConstants;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;

import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.helix.Replica;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroGenericSerializer;
import com.linkedin.venice.utils.KafkaSSLUtils;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;

import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;
import static com.linkedin.venice.integration.utils.VeniceServerWrapper.*;


/**
 * This is the whole enchilada:
 * - {@link ZkServerWrapper}
 * - {@link KafkaBrokerWrapper}
 * - {@link VeniceControllerWrapper}
 * - {@link VeniceServerWrapper}
 */
public class VeniceClusterWrapper extends ProcessWrapper {
  public static final String SERVICE_NAME = "VeniceCluster";
  public static final long DEFAULT_GET_MASTER_TIMEOUT_MS = 3000;
  private final String clusterName;
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
  private final AtomicInteger storeCount;

  private final boolean sslToStorageNodes;
  private final boolean sslToKafka;

  VeniceClusterWrapper(File dataDirectory,
                       String clusterName,
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
    super(SERVICE_NAME, dataDirectory);
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
    this.storeCount = new AtomicInteger(0);
    this.sslToStorageNodes = sslToStorageNodes;
    this.sslToKafka = sslToKafka;
  }

  static ServiceProvider<VeniceClusterWrapper> generateService(ZkServerWrapper zkServerWrapper,
      KafkaBrokerWrapper kafkaBrokerWrapper, BrooklinWrapper brooklinWrapper, String clusterName, String clusterToD2,
      int numberOfControllers, int numberOfServers, int numberOfRouters, int replicaFactor, int partitionSize,
      boolean enableWhitelist, boolean enableAutoJoinWhitelist, long delayToReblanceMS, int minActiveReplica,
      boolean sslToStorageNodes, boolean sslToKafka) {
    Map<Integer, VeniceControllerWrapper> veniceControllerWrappers = new HashMap<>();
    Map<Integer, VeniceServerWrapper> veniceServerWrappers = new HashMap<>();
    Map<Integer, VeniceRouterWrapper> veniceRouterWrappers = new HashMap<>();
    try {
      // Setup D2 for controller
      String zkAddress = zkServerWrapper.getAddress();
      D2TestUtils.setupD2Config(zkAddress, false, D2TestUtils.CONTROLLER_CLUSTER_NAME, D2TestUtils.CONTROLLER_SERVICE_NAME, false);
      for (int i = 0; i < numberOfControllers; i++) {
        VeniceControllerWrapper veniceControllerWrapper =
            ServiceFactory.getVeniceController(new String[]{clusterName}, kafkaBrokerWrapper, replicaFactor, partitionSize,
                delayToReblanceMS, minActiveReplica, brooklinWrapper, clusterToD2, sslToKafka, true);
        veniceControllerWrappers.put(veniceControllerWrapper.getPort(), veniceControllerWrapper);
      }

      for (int i = 0; i < numberOfServers; i++) {
        VeniceServerWrapper veniceServerWrapper =
            ServiceFactory.getVeniceServer(clusterName, kafkaBrokerWrapper, enableWhitelist, enableAutoJoinWhitelist,
                sslToStorageNodes, sslToKafka, new Properties());
        veniceServerWrappers.put(veniceServerWrapper.getPort(), veniceServerWrapper);
      }

      for (int i = 0; i < numberOfRouters; i++) {
        VeniceRouterWrapper veniceRouterWrapper =
            ServiceFactory.getVeniceRouter(clusterName, kafkaBrokerWrapper, sslToStorageNodes, clusterToD2);
        veniceRouterWrappers.put(veniceRouterWrapper.getPort(), veniceRouterWrapper);
      }

      /**
       * We get the various dependencies outside of the lambda, to avoid having a time
       * complexity of O(N^2) on the amount of retries. The calls have their own retries,
       * so we can assume they're reliable enough.
       */
      return (serviceName, port) -> new VeniceClusterWrapper(null, clusterName, zkServerWrapper, kafkaBrokerWrapper,
          brooklinWrapper, veniceControllerWrappers, veniceServerWrappers, veniceRouterWrappers, replicaFactor,
          partitionSize, delayToReblanceMS, minActiveReplica, sslToStorageNodes, sslToKafka);
    } catch (Exception e) {
      veniceRouterWrappers.values().stream().forEach(IOUtils::closeQuietly);
      veniceServerWrappers.values().stream().forEach(IOUtils::closeQuietly);
      veniceControllerWrappers.values().stream().forEach(IOUtils::closeQuietly);
      throw e;
    }
  }

  static ServiceProvider<VeniceClusterWrapper> generateService(String clusterName, int numberOfControllers, int numberOfServers,
      int numberOfRouters, int replicaFactor, int partitionSize, boolean enableWhitelist,
      boolean enableAutoJoinWhitelist, long delayToReblanceMS, int minActiveReplica, boolean sslToStorageNodes, boolean sslToKafka) {
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
      return generateService(zkServerWrapper, kafkaBrokerWrapper, brooklinWrapper, clusterName, null, numberOfControllers,
          numberOfServers, numberOfRouters, replicaFactor, partitionSize, enableWhitelist, enableAutoJoinWhitelist,
          delayToReblanceMS, minActiveReplica, sslToStorageNodes, sslToKafka);
    } catch (Exception e) {
      IOUtils.closeQuietly(zkServerWrapper);
      IOUtils.closeQuietly(kafkaBrokerWrapper);
      IOUtils.closeQuietly(brooklinWrapper);
      throw e;
    }
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

  public String getTopicReplicationConnectionString() {
    return brooklinWrapper.getBrooklinDmsUri();
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

  /**
   * Choose one of running venice router randomly.
   */
  public synchronized VeniceRouterWrapper getRandomVeniceRouter() {
    // TODO might use D2 to get router in the future
    return getRandomRunningVeniceComponent(veniceRouterWrappers);
  }

  public synchronized VeniceControllerWrapper getMasterVeniceController() {
    return getMasterVeniceController(DEFAULT_GET_MASTER_TIMEOUT_MS);
  }

  public synchronized VeniceControllerWrapper getRandmonVeniceController() {
    return getRandomRunningVeniceComponent(veniceControllerWrappers);
  }

  public synchronized String getAllControllersURLs() {
    String URLs = "";
    for (VeniceControllerWrapper controllerWrapper : veniceControllerWrappers.values()) {
      if (!URLs.isEmpty()) {
        URLs += ",";
      }
      URLs += controllerWrapper.getControllerUrl();
    }
    return URLs;
  }

  // After introducing latency into ZK socket, we might need to specify the timeout value based on how many time
  // latency we introduced.
  public synchronized VeniceControllerWrapper getMasterVeniceController(long timeoutMS) {
    List<VeniceControllerWrapper> masterControllers = new ArrayList<>();
    TestUtils.waitForNonDeterministicCompletion(timeoutMS, TimeUnit.MILLISECONDS, () -> {
      masterControllers.addAll(veniceControllerWrappers.values().stream().filter(veniceControllerWrapper -> {
        try {
          return veniceControllerWrapper.isMasterController(clusterName);
        } catch (VeniceException e) {
          return false;
        }
      })
          .collect(Collectors.toList()));
      if(masterControllers.size() == 1){
        return true;
      }else{
        masterControllers.clear();
        return false;
      }
    });
    return masterControllers.get(0);
  }

  public VeniceControllerWrapper addVeniceController() {
    VeniceControllerWrapper veniceControllerWrapper =
        ServiceFactory.getVeniceController(clusterName, kafkaBrokerWrapper, defaultReplicaFactor, defaultPartitionSize,
            defaultDelayToRebalanceMS, defaultMinActiveReplica, null, sslToKafka);
    veniceControllerWrappers.put(veniceControllerWrapper.getPort(), veniceControllerWrapper);
    return veniceControllerWrapper;
  }

  public VeniceRouterWrapper addVeniceRouter(Properties properties) {
    VeniceRouterWrapper veniceRouterWrapper = ServiceFactory.getVeniceRouter(clusterName, kafkaBrokerWrapper, sslToStorageNodes, properties);
    veniceRouterWrappers.put(veniceRouterWrapper.getPort(), veniceRouterWrapper);
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
        ServiceFactory.getVeniceServer(clusterName, kafkaBrokerWrapper, featureProperties, new Properties());
    veniceServerWrappers.put(veniceServerWrapper.getPort(), veniceServerWrapper);
    return veniceServerWrapper;
  }

  /**
   * @deprecated Future use should consider {@link #addVeniceServer(Properties, Properties)}
   *
   * @param properties
   * @return
   */
  public VeniceServerWrapper addVeniceServer(Properties properties) {
    VeniceServerWrapper veniceServerWrapper = ServiceFactory.getVeniceServer(clusterName, kafkaBrokerWrapper, new Properties(), properties);
    veniceServerWrappers.put(veniceServerWrapper.getPort(), veniceServerWrapper);
    return veniceServerWrapper;
  }

  public VeniceServerWrapper addVeniceServer(Properties featureProperties, Properties configProperties) {
    VeniceServerWrapper veniceServerWrapper = ServiceFactory.getVeniceServer(clusterName, kafkaBrokerWrapper, featureProperties, configProperties);
    veniceServerWrappers.put(veniceServerWrapper.getPort(), veniceServerWrapper);
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

  public synchronized void stopVeniceRouter(int port) {
    stopVeniceComponent(veniceRouterWrappers, port);
  }

  public synchronized void restartVeniceRouter(int port) {
    restartVeniceComponent(veniceRouterWrappers, port);
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

  public synchronized void restartVeniceServer(int port){
    restartVeniceComponent(veniceServerWrappers, port);
  }

  /**
   * Find the venice servers which has been assigned to the given resource and partition.
   * After getting these servers, you can fail some of them to simulate the server failure. Otherwise you might not
   * know which server you should fail.
   */
  public synchronized List<VeniceServerWrapper> findVeniceServer(String resourceName, int partition, HelixState state){
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
    if(components.containsKey(port)){
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
    // Stop called in reverse order of dependency
    veniceRouterWrappers.values().stream().forEach(IOUtils::closeQuietly);
    veniceServerWrappers.values().stream().forEach(IOUtils::closeQuietly);
    veniceControllerWrappers.values().stream().forEach(IOUtils::closeQuietly);
    IOUtils.closeQuietly(zkServerWrapper);
    IOUtils.closeQuietly(kafkaBrokerWrapper);
    IOUtils.closeQuietly(brooklinWrapper);
  }

  @Override
  protected void newProcess()
      throws Exception {
    throw new UnsupportedOperationException("Cluster does not support to create new process.");
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
    String storeName = TestUtils.getUniqueString("venice-store");
    String storeOwner = TestUtils.getUniqueString("store-owner");
    long storeSize =  1024;

    // Create new store
    ControllerClient controllerClient = new ControllerClient(clusterName, getAllControllersURLs());

    NewStoreResponse response = controllerClient.createNewStore(storeName, storeOwner, keySchema, valueSchema);
    // Create new version
    VersionCreationResponse newVersion = controllerClient.requestTopicForWrites(storeName, storeSize,
        ControllerApiConstants.PushType.BATCH, Version.guidBasedDummyPushId(), false);
    if (newVersion.isError()) {
      throw new VeniceException(newVersion.getError());
    }
    storeCount.getAndIncrement();
    return newVersion;
  }

  public ControllerResponse updateStore(String storeName, UpdateStoreQueryParams params) {
    // Create new store
    ControllerClient controllerClient = new ControllerClient(clusterName, getAllControllersURLs());
    ControllerResponse response = controllerClient.updateStore(storeName, params);
    if (response.isError()) {
      throw new VeniceException(response.getError());
    }
    return response;
  }

  /**
   * Get a venice writer to write string key-value pairs to given version for this cluster.
   * @return
   */
  public VeniceWriter<String, String> getVeniceWriter(String storeVersionName) {
    Properties properties = new Properties();
    properties.put(KAFKA_BOOTSTRAP_SERVERS, kafkaBrokerWrapper.getAddress());
    properties.put(ZOOKEEPER_ADDRESS, zkServerWrapper.getAddress());
    properties.put(CLUSTER_NAME, clusterName);
    TestUtils.VeniceTestWriterFactory factory = new TestUtils.VeniceTestWriterFactory(properties);
    String stringSchema = "\"string\"";
    VeniceKafkaSerializer keySerializer = new VeniceAvroGenericSerializer(stringSchema);
    VeniceKafkaSerializer valueSerializer = new VeniceAvroGenericSerializer(stringSchema);

    return factory.getVeniceWriter(storeVersionName, keySerializer, valueSerializer);

  }

  public VeniceWriter<String, String> getSslVeniceWriter(String storeVersionName) {

    Properties properties = new Properties();
    properties.put(KAFKA_BOOTSTRAP_SERVERS, kafkaBrokerWrapper.getSSLAddress());
    properties.put(ZOOKEEPER_ADDRESS, zkServerWrapper.getAddress());
    properties.put(CLUSTER_NAME, clusterName);
    properties.putAll(KafkaSSLUtils.getLocalKafkaClientSSLConfig());
    TestUtils.VeniceTestWriterFactory factory = new TestUtils.VeniceTestWriterFactory(properties);

    String stringSchema = "\"string\"";
    VeniceKafkaSerializer keySerializer = new VeniceAvroGenericSerializer(stringSchema);
    VeniceKafkaSerializer valueSerializer = new VeniceAvroGenericSerializer(stringSchema);

    return factory.getVeniceWriter(storeVersionName, keySerializer, valueSerializer);
  }

  public NewStoreResponse getNewStore(String storeName) {
    return getNewStore(storeName, "store-owner");
  }

  public VersionCreationResponse getNewVersion(String storeName, int dataSize) {
    return getNewVersion(getAllControllersURLs(), storeName, dataSize);
  }

  public NewStoreResponse getNewStore(String storeName, String owner) {
    String keySchema = "\"string\"";
    String valueSchema = "\"string\"";
    ControllerClient controllerClient = new ControllerClient(clusterName, getAllControllersURLs());

    NewStoreResponse response = controllerClient.createNewStore(storeName, owner, keySchema, valueSchema);
    if (response.isError()) {
      throw new VeniceException(response.getError());
    }

    storeCount.getAndIncrement();
    return response;
  }

  public VersionCreationResponse getNewVersion(String url, String storeName, int dataSize) {
    ControllerClient controllerClient = new ControllerClient(clusterName, getAllControllersURLs());
    VersionCreationResponse newVersion =
        controllerClient.requestTopicForWrites(storeName, dataSize, ControllerApiConstants.PushType.BATCH,
            Version.guidBasedDummyPushId(), false);
    if (newVersion.isError()) {
      throw new VeniceException(newVersion.getError());
    }
    return newVersion;
  }

  public String getRandomRouterURL() {
    return "http://" + getRandomVeniceRouter().getAddress();
  }

  public String getRandomRouterSslURL() {
    VeniceRouterWrapper router = getRandomVeniceRouter();
    return "https://" + router.getHost() + ":" + router.getSslPort();
  }

  public int getStoreCount() {
    return storeCount.get();
  }

  public void increaseStoreCount() {
    storeCount.getAndIncrement();
  }

  public String clusterConnectionInformation() {
    StringJoiner joiner = new StringJoiner("\n*****", "*****", "");
    joiner.add("Zookeeper: " + zkServerWrapper.getAddress());
    joiner.add("Kafka: " + kafkaBrokerWrapper.getAddress());
    joiner.add("Router: " + getRandomRouterURL());
    joiner.add("Cluster: " + clusterName);
    return joiner.toString();
  }
}
