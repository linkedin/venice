package com.linkedin.venice.integration.utils;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;

import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.helix.Replica;
import com.linkedin.venice.serialization.VeniceSerializer;
import com.linkedin.venice.serialization.avro.AvroGenericSerializer;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;


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
  private final int defaultReplicaFactor;
  private final int defaultPartitionSize;
  private final Map<Integer, VeniceControllerWrapper> veniceControllerWrappers;
  private final Map<Integer, VeniceServerWrapper> veniceServerWrappers;
  private final Map<Integer, VeniceRouterWrapper> veniceRouterWrappers;

  VeniceClusterWrapper(File dataDirectory,
                       String clusterName,
                       ZkServerWrapper zkServerWrapper,
                       KafkaBrokerWrapper kafkaBrokerWrapper,
                       Map<Integer, VeniceControllerWrapper> veniceControllerWrappers,
                       Map<Integer, VeniceServerWrapper> veniceServerWrappers,
                       Map<Integer, VeniceRouterWrapper> veniceRouterWrappers,
                       int defaultReplicaFactor,
                       int defaultPartitionSize) {
    super(SERVICE_NAME, dataDirectory);
    this.clusterName = clusterName;
    this.zkServerWrapper = zkServerWrapper;
    this.kafkaBrokerWrapper = kafkaBrokerWrapper;
    this.veniceControllerWrappers = veniceControllerWrappers;
    this.veniceServerWrappers = veniceServerWrappers;
    this.veniceRouterWrappers = veniceRouterWrappers;
    this.defaultReplicaFactor = defaultReplicaFactor;
    this.defaultPartitionSize = defaultPartitionSize;
  }

  static ServiceProvider<VeniceClusterWrapper> generateService(int numberOfControllers, int numberOfServers,
      int numberOfRouters, int replicaFactor, int partitionSize, boolean enableWhitelist, boolean enableAutoJoinWhitelist) {
    /**
     * We get the various dependencies outside of the lambda, to avoid having a time
     * complexity of O(N^2) on the amount of retries. The calls have their own retries,
     * so we can assume they're reliable enough.
     */
    String clusterName = TestUtils.getUniqueString("venice-cluster");
    ZkServerWrapper zkServerWrapper = ServiceFactory.getZkServer();
    KafkaBrokerWrapper kafkaBrokerWrapper = ServiceFactory.getKafkaBroker(zkServerWrapper);

    Map<Integer, VeniceControllerWrapper> veniceControllerWrappers = new HashMap<>();
    for (int i = 0; i < numberOfControllers; i++) {
      VeniceControllerWrapper veniceControllerWrapper =
          ServiceFactory.getVeniceController(clusterName, kafkaBrokerWrapper, replicaFactor, partitionSize);
      veniceControllerWrappers.put(veniceControllerWrapper.getPort(), veniceControllerWrapper);
    }

    Map<Integer, VeniceServerWrapper> veniceServerWrappers = new HashMap<>();
    for (int i = 0; i < numberOfServers; i++) {
      VeniceServerWrapper veniceServerWrapper =
          ServiceFactory.getVeniceServer(clusterName, kafkaBrokerWrapper, enableWhitelist, enableAutoJoinWhitelist);
      veniceServerWrappers.put(veniceServerWrapper.getPort(), veniceServerWrapper);
    }

    Map<Integer, VeniceRouterWrapper> veniceRouterWrappers = new HashMap<>();
    for (int i = 0; i < numberOfRouters; i++) {
      VeniceRouterWrapper veniceRouterWrapper = ServiceFactory.getVeniceRouter(clusterName, kafkaBrokerWrapper);
      veniceRouterWrappers.put(veniceRouterWrapper.getPort(), veniceRouterWrapper);
    }

    return (serviceName, port) -> new VeniceClusterWrapper(null, clusterName, zkServerWrapper, kafkaBrokerWrapper,
        veniceControllerWrappers, veniceServerWrappers, veniceRouterWrappers, replicaFactor, partitionSize);
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
    TestUtils.waitForNonDeterministicCompletion(timeoutMS, TimeUnit.MILLISECONDS, ()->{
      masterControllers.addAll(veniceControllerWrappers.values()
          .stream()
          .filter(veniceControllerWrapper -> veniceControllerWrapper.isMasterController(clusterName))
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
        ServiceFactory.getVeniceController(clusterName, kafkaBrokerWrapper, defaultReplicaFactor, defaultPartitionSize);
    veniceControllerWrappers.put(veniceControllerWrapper.getPort(), veniceControllerWrapper);
    return veniceControllerWrapper;
  }

  public VeniceRouterWrapper addVeniceRouter() {
    VeniceRouterWrapper veniceRouterWrapper = ServiceFactory.getVeniceRouter(clusterName, kafkaBrokerWrapper);
    veniceRouterWrappers.put(veniceRouterWrapper.getPort(), veniceRouterWrapper);
    return veniceRouterWrapper;
  }

  public VeniceServerWrapper addVeniceServer(boolean enableWhitelist, boolean enableAutoJoinWhiteList) {
    VeniceServerWrapper veniceServerWrapper =
        ServiceFactory.getVeniceServer(clusterName, kafkaBrokerWrapper, enableWhitelist, enableAutoJoinWhiteList);
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
    for (VeniceRouterWrapper veniceRouterWrapper : veniceRouterWrappers.values()) {
      veniceRouterWrapper.stop();
    }
    for (VeniceServerWrapper veniceServerWrapper : veniceServerWrappers.values()) {
      veniceServerWrapper.stop();
    }
    for (VeniceControllerWrapper veniceControllerWrapper : veniceControllerWrappers.values()) {
      veniceControllerWrapper.stop();
    }
    kafkaBrokerWrapper.stop();
    zkServerWrapper.stop();
  }

  @Override
  protected void newProcess()
      throws Exception {
    throw new UnsupportedOperationException("Cluster does not support to create new process.");
  }

  public VersionCreationResponse getNewStoreVersion() {
    String storeName = TestUtils.getUniqueString("venice-store");
    String storeOwner = TestUtils.getUniqueString("store-owner");
    long storeSize = 10 * 1024 * 1024;
    String keySchema = "\"string\"";
    String valueSchema = "\"string\"";
    // Create new store
    ControllerClient.createNewStore(getAllControllersURLs(), clusterName, storeName, storeOwner, keySchema, valueSchema);
    // Create new version
    VersionCreationResponse newVersion =
        ControllerClient.createNewStoreVersion(getAllControllersURLs(), clusterName, storeName, storeSize);
    if (newVersion.isError()) {
      throw new VeniceException(newVersion.getError());
    }
    return newVersion;
  }

  /**
   * Get a venice writer to write string key-value pairs to given version for this cluster.
   * @return
   */
  public VeniceWriter<String, String> getVeniceWriter(String storeVersionName) {
    VeniceProperties clientProps = new PropertyBuilder().put(KAFKA_BOOTSTRAP_SERVERS, kafkaBrokerWrapper.getAddress())
        .put(ZOOKEEPER_ADDRESS, zkServerWrapper.getAddress())
        .put(CLUSTER_NAME, clusterName)
        .build();

    String stringSchema = "\"string\"";
    VeniceSerializer keySerializer = new AvroGenericSerializer(stringSchema);
    VeniceSerializer valueSerializer = new AvroGenericSerializer(stringSchema);

    return new VeniceWriter<>(clientProps, storeVersionName, keySerializer, valueSerializer);
  }

  public NewStoreResponse getNewStore(String storeName, int dataSize) {
    return getNewStore(getAllControllersURLs(), storeName, dataSize);
  }

  public VersionCreationResponse getNewVersion(String storeName, int dataSize) {
    return getNewVersion(getAllControllersURLs(), storeName, dataSize);
  }

  public NewStoreResponse getNewStore(String url, String storeName, int dataSize) {
    String storeOwner = TestUtils.getUniqueString("store-owner");
    String keySchema = "\"string\"";
    String valueSchema = "\"string\"";
    NewStoreResponse response =
        ControllerClient.createNewStore(url, clusterName, storeName, storeOwner, keySchema, valueSchema);
    if (response.isError()) {
      throw new VeniceException(response.getError());
    }
    return response;
  }

  public VersionCreationResponse getNewVersion(String url, String storeName, int dataSize) {
    VersionCreationResponse newVersion =
        ControllerClient.createNewStoreVersion(url, clusterName, storeName, dataSize);
    if (newVersion.isError()) {
      throw new VeniceException(newVersion.getError());
    }
    return newVersion;
  }

  public String getRandomRouterURL() {
    return "http://" + getRandomVeniceRouter().getAddress();
  }
}
