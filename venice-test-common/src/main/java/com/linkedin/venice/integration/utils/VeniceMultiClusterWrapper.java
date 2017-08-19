package com.linkedin.venice.integration.utils;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.TestUtils;
import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


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
      boolean enableAutoJoinWhitelist, long delayToReblanceMS, int minActiveReplica, boolean sslToStorageNodes) {
    ZkServerWrapper zkServerWrapper = ServiceFactory.getZkServer();
    KafkaBrokerWrapper kafkaBrokerWrapper = ServiceFactory.getKafkaBroker(zkServerWrapper);
    BrooklinWrapper brooklinWrapper = ServiceFactory.getBrooklinWrapper(kafkaBrokerWrapper);

    String[] clusterNames = new String[numberOfClusters];
    for (int i = 0; i < numberOfClusters; i++) {
      String clusterName = TestUtils.getUniqueString("venice-cluster");
      clusterNames[i] = clusterName;
    }
    // Create controllers for multi-cluster
    Map<Integer, VeniceControllerWrapper> controllerMap = new HashMap<>();
    for (int i = 0; i < numberOfControllers; i++) {
      VeniceControllerWrapper controllerWrapper = ServiceFactory.getVeniceController(clusterNames, kafkaBrokerWrapper, replicaFactor, partitionSize,
          delayToReblanceMS, minActiveReplica, brooklinWrapper);
      controllerMap.put(controllerWrapper.getPort(), controllerWrapper);
    }
    Map<String, VeniceClusterWrapper> clusterWrapperMap = new HashMap<>();
    for (int i = 0; i < numberOfClusters; i++) {
      // Create a wrapper for cluster without controller.
      VeniceClusterWrapper clusterWrapper =
          ServiceFactory.getVeniceClusterWrapperForMultiCluster(zkServerWrapper, kafkaBrokerWrapper, brooklinWrapper,
              clusterNames[i], 0, numberOfServers, numberOfRouters, replicaFactor, partitionSize, enableWhitelist,
              enableAutoJoinWhitelist, delayToReblanceMS, minActiveReplica, sslToStorageNodes);
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
      clusters.get(cluster).stop();
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

  public String[] getClusterNames() {
    return clusters.keySet().toArray(new String[clusters.keySet().size()]);
  }
}
