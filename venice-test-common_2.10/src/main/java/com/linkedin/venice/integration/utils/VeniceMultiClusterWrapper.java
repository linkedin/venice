package com.linkedin.venice.integration.utils;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.TestUtils;
import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;


public class VeniceMultiClusterWrapper extends ProcessWrapper {
  public static final String SERVICE_NAME = "VeniceMultiCluster";
  private final Map<String, VeniceClusterWrapper> clusters;
  private final Map<Integer, VeniceControllerWrapper> controllers;
  private final ZkServerWrapper zkServerWrapper;
  private final KafkaBrokerWrapper kafkaBrokerWrapper;
  private final BrooklinWrapper brooklinWrapper;

  VeniceMultiClusterWrapper(File dataDirectory, ZkServerWrapper zkServerWrapper, KafkaBrokerWrapper kafkaBrokerWrapper,
      BrooklinWrapper brooklinWrapper, Map<String, VeniceClusterWrapper> clusters,
      Map<Integer, VeniceControllerWrapper> controllers) {
    super(SERVICE_NAME, dataDirectory);
    this.zkServerWrapper = zkServerWrapper;
    this.kafkaBrokerWrapper = kafkaBrokerWrapper;
    this.brooklinWrapper = brooklinWrapper;
    this.controllers = controllers;
    this.clusters = clusters;
  }

  static ServiceProvider<VeniceMultiClusterWrapper> generateService(int numberOfClusters, int numberOfControllers,
      int numberOfServers, int numberOfRouters, int replicaFactor, int partitionSize, boolean enableWhitelist,
      boolean enableAutoJoinWhitelist, long delayToReblanceMS, int minActiveReplica, boolean sslToStorageNodes, Optional<Integer> zkPort, boolean randomizeClusterName) {
    ZkServerWrapper zkServerWrapper = zkPort.isPresent() ? ServiceFactory.getZkServer(zkPort.get()) : ServiceFactory.getZkServer();
    KafkaBrokerWrapper kafkaBrokerWrapper = ServiceFactory.getKafkaBroker(zkServerWrapper);
    BrooklinWrapper brooklinWrapper = ServiceFactory.getBrooklinWrapper(kafkaBrokerWrapper);
    String clusterToD2="";
    String[] clusterNames = new String[numberOfClusters];
    for (int i = 0; i < numberOfClusters; i++) {
      String clusterName = randomizeClusterName ? TestUtils.getUniqueString("venice-cluster" + i) : "venice-cluster" + i;
      clusterNames[i] = clusterName;
      clusterToD2+=TestUtils.getClusterToDefaultD2String(clusterName)+",";
    }
    clusterToD2 = clusterToD2.substring(0, clusterToD2.length()-1);

    // Create controllers for multi-cluster
    Map<Integer, VeniceControllerWrapper> controllerMap = new HashMap<>();

    for (int i = 0; i < numberOfControllers; i++) {
      VeniceControllerWrapper controllerWrapper = ServiceFactory.getVeniceController(clusterNames, kafkaBrokerWrapper, replicaFactor, partitionSize,
          delayToReblanceMS, minActiveReplica, brooklinWrapper, clusterToD2, false, false);
      controllerMap.put(controllerWrapper.getPort(), controllerWrapper);
    }
    Map<String, VeniceClusterWrapper> clusterWrapperMap = new HashMap<>();
    for (int i = 0; i < numberOfClusters; i++) {
      // Create a wrapper for cluster without controller.
      VeniceClusterWrapper clusterWrapper =
          ServiceFactory.getVeniceClusterWrapperForMultiCluster(zkServerWrapper, kafkaBrokerWrapper, brooklinWrapper,
              clusterNames[i], clusterToD2, 0, numberOfServers, numberOfRouters, replicaFactor, partitionSize, enableWhitelist,
              enableAutoJoinWhitelist, delayToReblanceMS, minActiveReplica, sslToStorageNodes, false);
      clusterWrapperMap.put(clusterWrapper.getClusterName(), clusterWrapper);
    }
    return (serviceName, port) -> new VeniceMultiClusterWrapper(null, zkServerWrapper, kafkaBrokerWrapper,
        brooklinWrapper, clusterWrapperMap, controllerMap);
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
  protected void internalStart()
      throws Exception {
    // Everything should already be started. So this is a no-op.
  }

  @Override
  protected void internalStop()
      throws Exception {
    Iterator<String> clusterIter = clusters.keySet().iterator();
    while (clusterIter.hasNext()) {
      String cluster = clusterIter.next();
      Executors.newCachedThreadPool().execute(() -> {
        clusters.get(cluster).close();
      });
      clusterIter.remove();
    }
  }

  @Override
  protected void newProcess()
      throws Exception {
    throw new UnsupportedOperationException("Cluster does not support to create new process.");
  }

  public Map<String, VeniceClusterWrapper> getClusters() {
    return clusters;
  }

  public Map<Integer, VeniceControllerWrapper> getControllers() {
    return controllers;
  }

  public ZkServerWrapper getZkServerWrapper() {
    return zkServerWrapper;
  }

  public KafkaBrokerWrapper getKafkaBrokerWrapper() {
    return kafkaBrokerWrapper;
  }

  public BrooklinWrapper getBrooklinWrapper() {
    return brooklinWrapper;
  }

  public VeniceControllerWrapper getRandomController() {
    return this.controllers.values().stream().filter(controller -> controller.isRunning()).findAny().get();
  }

  public VeniceControllerWrapper getMasterController(String clusterName) {
    return this.controllers.values()
        .stream()
        .filter(controller -> controller.isRunning())
        .filter(c -> c.isMasterController(clusterName))
        .findAny()
        .get();
  }

  public String getControllerConnectString(){
    StringBuilder connectStr=new StringBuilder("");
    for(VeniceControllerWrapper controllerWrapper:controllers.values()){
      connectStr.append(controllerWrapper.getControllerUrl());
      connectStr.append(',');
    }
    if(connectStr.length() != 0){
      connectStr.deleteCharAt(connectStr.length()-1);
    }
    return connectStr.toString();
  }

  public String[] getClusterNames() {
    return clusters.keySet().toArray(new String[clusters.keySet().size()]);
  }

  public void restartControllers() {
    controllers.values().stream().forEach(veniceControllerWrapper -> {
      try {
        veniceControllerWrapper.stop();
        veniceControllerWrapper.restart();
      } catch (Exception e) {
        throw new VeniceException("Can not restart controller " + veniceControllerWrapper.getControllerUrl(), e);
      }
    });
  }
}
