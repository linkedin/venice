package com.linkedin.venice.integration.utils;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;

import com.linkedin.venice.utils.VeniceProperties;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;


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
      int numberOfServers, int numberOfRouters, int replicationFactor, int partitionSize, boolean enableWhitelist,
      boolean enableAutoJoinWhitelist, long delayToReblanceMS, int minActiveReplica, boolean sslToStorageNodes,
      Optional<Integer> zkPort, boolean randomizeClusterName, boolean multiColoSetup, Optional<VeniceProperties> veniceProperties,
      boolean multiD2) {
    ZkServerWrapper zkServerWrapper = null;
    KafkaBrokerWrapper kafkaBrokerWrapper = null;
    BrooklinWrapper brooklinWrapper = null;
    Map<String, VeniceClusterWrapper> clusterWrapperMap = new HashMap<>();
    Map<Integer, VeniceControllerWrapper> controllerMap = new HashMap<>();

    try {
      zkServerWrapper = zkPort.isPresent() ? ServiceFactory.getZkServer(zkPort.get()) : ServiceFactory.getZkServer();
      kafkaBrokerWrapper = ServiceFactory.getKafkaBroker(zkServerWrapper);
      brooklinWrapper = ServiceFactory.getBrooklinWrapper(kafkaBrokerWrapper);
      String clusterToD2 = "";
      String[] clusterNames = new String[numberOfClusters];
      for (int i = 0; i < numberOfClusters; i++) {
        String clusterName = randomizeClusterName ? TestUtils.getUniqueString("venice-cluster" + i) : "venice-cluster" + i;
        clusterNames[i] = clusterName;
        if (multiD2) {
          clusterToD2 += clusterName + ":venice-" + i + ",";
        } else {
          clusterToD2 += TestUtils.getClusterToDefaultD2String(clusterName) + ",";
        }
      }
      clusterToD2 = clusterToD2.substring(0, clusterToD2.length()-1);

      // Create controllers for multi-cluster
      Properties controllerProperties = new Properties();
      if (multiColoSetup) {
        // In multi-colo setup, we don't allow batch push to each individual child colo, but just parent colo
        controllerProperties.put(ConfigKeys.CONTROLLER_ENABLE_BATCH_PUSH_FROM_ADMIN_IN_CHILD, "false");
      }
      for (int i = 0; i < numberOfControllers; i++) {
        VeniceControllerWrapper controllerWrapper = ServiceFactory.getVeniceController(clusterNames, kafkaBrokerWrapper, replicationFactor, partitionSize,
            delayToReblanceMS, minActiveReplica, brooklinWrapper, clusterToD2, false, false, controllerProperties);
        controllerMap.put(controllerWrapper.getPort(), controllerWrapper);
      }
      for (int i = 0; i < numberOfClusters; i++) {
        // Create a wrapper for cluster without controller.
        VeniceClusterWrapper clusterWrapper =
            ServiceFactory.getVeniceClusterWrapperForMultiCluster(zkServerWrapper, kafkaBrokerWrapper, brooklinWrapper,
                clusterNames[i], clusterToD2, 0, numberOfServers, numberOfRouters, replicationFactor, partitionSize, enableWhitelist,
                enableAutoJoinWhitelist, delayToReblanceMS, minActiveReplica, sslToStorageNodes, false, veniceProperties);
        clusterWrapperMap.put(clusterWrapper.getClusterName(), clusterWrapper);
      }
      final ZkServerWrapper finalZkServerWrapper = zkServerWrapper;
      final KafkaBrokerWrapper finalKafkaBrokerWrapper = kafkaBrokerWrapper;
      final BrooklinWrapper finalBrooklinWrapper = brooklinWrapper;
      return (serviceName, port) -> new VeniceMultiClusterWrapper(null, finalZkServerWrapper, finalKafkaBrokerWrapper,
          finalBrooklinWrapper, clusterWrapperMap, controllerMap);
    } catch (Exception e) {
      IOUtils.closeQuietly(zkServerWrapper);
      IOUtils.closeQuietly(kafkaBrokerWrapper);
      IOUtils.closeQuietly(brooklinWrapper);
      clusterWrapperMap.values().forEach(ProcessWrapper::close);
      controllerMap.values().forEach(ProcessWrapper::close);
      throw e;
    }
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
    CompletableFuture.runAsync(() -> clusters.values().stream().forEach(IOUtils::closeQuietly));
  }

  @Override
  protected void newProcess() throws Exception {
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
    return getMasterController(clusterName, 60 * Time.MS_PER_SECOND);
  }

  public VeniceControllerWrapper getMasterController(String clusterName, long timeoutMs) {
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    while (System.nanoTime() < deadline) {
      for (VeniceControllerWrapper controller : controllers.values()) {
        if (controller.isRunning() && controller.isMasterController(clusterName)) {
          return controller;
        }
      }
      Utils.sleep(Time.MS_PER_SECOND);
    }
    throw new VeniceException("Master controller does not exist, cluster=" + clusterName);
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

  public void removeOneController() {
    if (controllers.size() > 1) {
      VeniceControllerWrapper controllerWrapper = controllers.values().stream().findFirst().get();
      controllerWrapper.close();
      controllers.remove(controllerWrapper.getPort());
    }
  }
}
