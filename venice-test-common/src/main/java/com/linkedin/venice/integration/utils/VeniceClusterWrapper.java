package com.linkedin.venice.integration.utils;

import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.controller.Admin;
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
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.utils.KafkaSSLUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import java.io.File;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;

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
  public static final String SERVICE_NAME = "VeniceCluster";

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
  private final boolean sslToStorageNodes;
  private final boolean sslToKafka;

  VeniceClusterWrapper(
      File dataDirectory,
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
    this.sslToStorageNodes = sslToStorageNodes;
    this.sslToKafka = sslToKafka;
  }

  static ServiceProvider<VeniceClusterWrapper> generateService(
      ZkServerWrapper zkServerWrapper,
      KafkaBrokerWrapper kafkaBrokerWrapper,
      BrooklinWrapper brooklinWrapper,
      String clusterName,
      String clusterToD2,
      int numberOfControllers,
      int numberOfServers,
      int numberOfRouters,
      int replicaFactor,
      int partitionSize,
      boolean enableWhitelist,
      boolean enableAutoJoinWhitelist,
      long delayToReblanceMS,
      int minActiveReplica,
      boolean sslToStorageNodes,
      boolean sslToKafka,
      Properties extraProperties) {

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
                delayToReblanceMS, minActiveReplica, brooklinWrapper, clusterToD2, sslToKafka, true, extraProperties);
        veniceControllerWrappers.put(veniceControllerWrapper.getPort(), veniceControllerWrapper);
      }

      for (int i = 0; i < numberOfRouters; i++) {
        VeniceRouterWrapper veniceRouterWrapper =
            ServiceFactory.getVeniceRouter(clusterName, kafkaBrokerWrapper, sslToStorageNodes, clusterToD2);
        veniceRouterWrappers.put(veniceRouterWrapper.getPort(), veniceRouterWrapper);
      }

      for (int i = 0; i < numberOfServers; i++) {
        Properties featureProperties = new Properties();
        featureProperties.setProperty(SERVER_ENABLE_SERVER_WHITE_LIST, Boolean.toString(enableWhitelist));
        featureProperties.setProperty(SERVER_IS_AUTO_JOIN, Boolean.toString(enableAutoJoinWhitelist));
        featureProperties.setProperty(SERVER_ENABLE_SSL, Boolean.toString(sslToStorageNodes));
        featureProperties.setProperty(SERVER_SSL_TO_KAFKA, Boolean.toString(sslToKafka));
        if (!veniceRouterWrappers.isEmpty()) {
          featureProperties.put(CLIENT_CONFIG_FOR_CONSUMER, ClientConfig.defaultGenericClientConfig("")
                  .setVeniceURL("http://" + veniceRouterWrappers.values().stream().findFirst().get().getAddress())

              // TODO: Figure out why the D2-based config doesn't work...
//            .setD2Client(D2TestUtils.getAndStartD2Client(zkAddress))
//            .setD2ServiceName(D2TestUtils.DEFAULT_TEST_SERVICE_NAME)
          );
        }

        VeniceServerWrapper veniceServerWrapper =
            ServiceFactory.getVeniceServer(clusterName, kafkaBrokerWrapper, featureProperties, extraProperties);
        veniceServerWrappers.put(veniceServerWrapper.getPort(), veniceServerWrapper);
      }

      /**
       * We get the various dependencies outside of the lambda, to avoid having a time
       * complexity of O(N^2) on the amount of retries. The calls have their own retries,
       * so we can assume they're reliable enough.
       */
      return (serviceName, port) ->
                 new VeniceClusterWrapper(
                     null,
                     clusterName,
                     zkServerWrapper,
                     kafkaBrokerWrapper,
                     brooklinWrapper,
                     veniceControllerWrappers,
                     veniceServerWrappers,
                     veniceRouterWrappers,
                     replicaFactor,
                     partitionSize,
                     delayToReblanceMS,
                     minActiveReplica,
                     sslToStorageNodes,
                     sslToKafka);

    } catch (Exception e) {
      veniceRouterWrappers.values().stream().forEach(IOUtils::closeQuietly);
      veniceServerWrappers.values().stream().forEach(IOUtils::closeQuietly);
      veniceControllerWrappers.values().stream().forEach(IOUtils::closeQuietly);
      throw e;
    }
  }

  static ServiceProvider<VeniceClusterWrapper> generateService(
      String clusterName,
      int numberOfControllers,
      int numberOfServers,
      int numberOfRouters,
      int replicaFactor,
      int partitionSize,
      boolean enableWhitelist,
      boolean enableAutoJoinWhitelist,
      long delayToReblanceMS,
      int minActiveReplica,
      boolean sslToStorageNodes,
      boolean sslToKafka,
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
          zkServerWrapper,
          kafkaBrokerWrapper,
          brooklinWrapper,
          clusterName,
          null,
          numberOfControllers,
          numberOfServers,
          numberOfRouters,
          replicaFactor,
          partitionSize,
          enableWhitelist,
          enableAutoJoinWhitelist,
          delayToReblanceMS,
          minActiveReplica,
          sslToStorageNodes,
          sslToKafka,extraProperties);

    } catch (Exception e) {
      IOUtils.closeQuietly(zkServerWrapper);
      IOUtils.closeQuietly(kafkaBrokerWrapper);
      IOUtils.closeQuietly(brooklinWrapper);
      throw e;
    }
  }

  @Override
  protected void internalStart() throws Exception {
    // Everything should already be started. So this is a no-op.
  }

  @Override
  protected void internalStop() throws Exception {
    CompletableFuture.runAsync(() -> {
      synchronized (this) {
        veniceRouterWrappers.values().stream().forEach(IOUtils::closeQuietly);
        veniceServerWrappers.values().stream().forEach(IOUtils::closeQuietly);
        veniceControllerWrappers.values().stream().forEach(IOUtils::closeQuietly);
        IOUtils.closeQuietly(zkServerWrapper);
        IOUtils.closeQuietly(kafkaBrokerWrapper);
        IOUtils.closeQuietly(brooklinWrapper);
      }
    });
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

  public synchronized String getAllControllersURLs() {
    return veniceControllerWrappers.values().stream()
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
    }
    return veniceControllerWrapper;
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
        ServiceFactory.getVeniceServer(clusterName, kafkaBrokerWrapper, featureProperties, new Properties());
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
    VeniceServerWrapper veniceServerWrapper = ServiceFactory.getVeniceServer(clusterName, kafkaBrokerWrapper, new Properties(), properties);
    synchronized (this) {
      veniceServerWrappers.put(veniceServerWrapper.getPort(), veniceServerWrapper);
    }
    return veniceServerWrapper;
  }

  public VeniceServerWrapper addVeniceServer(Properties featureProperties, Properties configProperties) {
    VeniceServerWrapper veniceServerWrapper = ServiceFactory.getVeniceServer(clusterName, kafkaBrokerWrapper, featureProperties, configProperties);
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

  public synchronized void stopVeniceRouter(int port) {
    stopVeniceComponent(veniceRouterWrappers, port);
  }

  public synchronized void restartVeniceRouter(int port) {
    restartVeniceComponent(veniceRouterWrappers, port);
  }

  public synchronized void removeVeniceRouter(int port) {
    stopVeniceRouter(port);
    veniceRouterWrappers.remove(port);
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
    veniceServerWrappers.remove(port);
    return effectedReplicas;
  }

  public synchronized void restartVeniceServer(int port) {
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

  public ControllerClient getControllerClient() {
    return new ControllerClient(clusterName, getAllControllersURLs());
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
    TestUtils.VeniceTestWriterFactory factory = new TestUtils.VeniceTestWriterFactory(properties);
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
    TestUtils.VeniceTestWriterFactory factory = new TestUtils.VeniceTestWriterFactory(properties);

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
    String storeName = TestUtils.getUniqueString("venice-store");
    String storeOwner = TestUtils.getUniqueString("store-owner");
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
              Version.guidBasedDummyPushId(), false, false, Optional.empty());
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
              Optional.empty());
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

  public String createStore(int keyCount, GenericRecord record) throws Exception {
    return createStore(DEFAULT_KEY_SCHEMA, record.getSchema().toString(),
        IntStream.range(0, keyCount).mapToObj(i -> new AbstractMap.SimpleEntry<>(i, record))
      );
  }

  public String createStore(Stream<Map.Entry> batchData) throws Exception {
    return createStore(DEFAULT_KEY_SCHEMA, DEFAULT_VALUE_SCHEMA, batchData);
  }

  public String createStore(String keySchema, String valueSchema, Stream<Map.Entry> batchData) throws Exception {
    try (ControllerClient client = getControllerClient()) {
      String storeName = TestUtils.getUniqueString("store");
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
          Optional.empty());
      if (response.isError()) {
        throw new VeniceException(response.getError());
      }

      String kafkaTopic = response.getKafkaTopic();
      writeBatchData(kafkaTopic, keySchema, valueSchema, batchData);

      int versionId = Version.parseVersionFromKafkaTopicName(kafkaTopic);
      waitVersion(storeName, versionId);
      return versionId;
    }
  }

  public void waitVersion(String storeName, int versionId) {
    try (ControllerClient client = getControllerClient()) {
      TestUtils.waitForNonDeterministicCompletion(30, TimeUnit.SECONDS, () -> {
        JobStatusQueryResponse response = client.queryJobStatus(Version.composeKafkaTopic(storeName, versionId));
        if (response.isError()) {
          throw new VeniceException(response.getError());
        }
        if (response.getStatus().equals(ExecutionStatus.ERROR.toString())) {
          throw new VeniceException("Unexpected push failure");
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

  protected void writeBatchData(String kafkaTopic, String keySchema, String valueSchema, Stream<Map.Entry> batchData) throws Exception {
    TestUtils.VeniceTestWriterFactory writerFactory = TestUtils.getVeniceTestWriterFactory(getKafka().getAddress());
    try (
        VeniceKafkaSerializer keySerializer = new VeniceAvroKafkaSerializer(keySchema);
        VeniceKafkaSerializer valueSerializer = new VeniceAvroKafkaSerializer(valueSchema);
        VeniceWriter<Object, Object, byte[]> writer = writerFactory.createVeniceWriter(kafkaTopic, keySerializer, valueSerializer)) {

      int valueSchemaId = HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID;
      writer.broadcastStartOfPush(Collections.emptyMap());
      for (Map.Entry e : (Iterable<Map.Entry>) batchData::iterator) {
        writer.put(e.getKey(), e.getValue(), valueSchemaId).get();
      }
      writer.broadcastEndOfPush(Collections.emptyMap());
    }
  }
}
