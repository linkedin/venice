package com.linkedin.venice.integration.utils;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.SslUtils;
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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.integration.utils.VeniceServerWrapper.*;


public class VeniceMultiClusterWrapper extends ProcessWrapper {
  public static final String SERVICE_NAME = "VeniceMultiCluster";
  private final Map<String, VeniceClusterWrapper> clusters;
  private final Map<Integer, VeniceControllerWrapper> controllers;
  private final ZkServerWrapper zkServerWrapper;
  private final KafkaBrokerWrapper kafkaBrokerWrapper;
  private final BrooklinWrapper brooklinWrapper;
  private final String clusterToD2;

  VeniceMultiClusterWrapper(File dataDirectory, ZkServerWrapper zkServerWrapper, KafkaBrokerWrapper kafkaBrokerWrapper,
      BrooklinWrapper brooklinWrapper, Map<String, VeniceClusterWrapper> clusters,
      Map<Integer, VeniceControllerWrapper> controllers, String clusterToD2) {
    super(SERVICE_NAME, dataDirectory);
    this.zkServerWrapper = zkServerWrapper;
    this.kafkaBrokerWrapper = kafkaBrokerWrapper;
    this.brooklinWrapper = brooklinWrapper;
    this.controllers = controllers;
    this.clusters = clusters;
    this.clusterToD2 = clusterToD2;
  }

  static ServiceProvider<VeniceMultiClusterWrapper> generateService(
      String coloName,
      int numberOfClusters, int numberOfControllers, int numberOfServers, int numberOfRouters, int replicationFactor,
      int partitionSize, boolean enableWhitelist, boolean enableAutoJoinWhitelist, long rebalanceDelayMs,
      int minActiveReplica, boolean sslToStorageNodes, boolean randomizeClusterName, boolean multiColoSetup,
      Optional<Properties> childControllerProperties, Optional<VeniceProperties> veniceProperties, boolean multiD2,
      boolean forkServer) {
    ZkServerWrapper zkServerWrapper = null;
    KafkaBrokerWrapper kafkaBrokerWrapper = null;
    BrooklinWrapper brooklinWrapper = null;
    Map<String, VeniceClusterWrapper> clusterWrapperMap = new HashMap<>();
    Map<Integer, VeniceControllerWrapper> controllerMap = new HashMap<>();

    try {
      zkServerWrapper = ServiceFactory.getZkServer();
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
      Properties controllerProperties;
      if (!childControllerProperties.isPresent()) {
        controllerProperties = new Properties();
      } else {
        controllerProperties = childControllerProperties.get();
      }
      if (multiColoSetup) {
        // In multi-colo setup, we don't allow batch push to each individual child colo, but just parent colo
        controllerProperties.put(ConfigKeys.CONTROLLER_ENABLE_BATCH_PUSH_FROM_ADMIN_IN_CHILD, "false");
      }
      if (coloName != null) {
        controllerProperties.setProperty(LOCAL_REGION_NAME, coloName);
      }

      // Setup D2 for controller
      String zkAddress = zkServerWrapper.getAddress();
      D2TestUtils.setupD2Config(zkAddress, false, D2TestUtils.CONTROLLER_CLUSTER_NAME, D2TestUtils.CONTROLLER_SERVICE_NAME, false);
      for (int i = 0; i < numberOfControllers; i++) {
        VeniceControllerWrapper controllerWrapper = ServiceFactory.getVeniceController(clusterNames, kafkaBrokerWrapper, replicationFactor, partitionSize,
            rebalanceDelayMs, minActiveReplica, brooklinWrapper, clusterToD2, false, true, controllerProperties);
        controllerMap.put(controllerWrapper.getPort(), controllerWrapper);
      }
      // Specify the system store cluster name
      Properties extraProperties = veniceProperties.isPresent() ? veniceProperties.get().toProperties() : new Properties();
      extraProperties.put(SYSTEM_SCHEMA_CLUSTER_NAME, clusterNames[0]);
      veniceProperties = Optional.of(new VeniceProperties(extraProperties));

      for (int i = 0; i < numberOfClusters; i++) {
        // Create a wrapper for cluster without controller.
        VeniceClusterWrapper clusterWrapper =
            ServiceFactory.getVeniceClusterWrapperForMultiCluster(coloName, zkServerWrapper, kafkaBrokerWrapper, brooklinWrapper,
                clusterNames[i], clusterToD2, 0, numberOfServers, numberOfRouters, replicationFactor, partitionSize, enableWhitelist,
                enableAutoJoinWhitelist, rebalanceDelayMs, minActiveReplica, sslToStorageNodes, false, veniceProperties, forkServer);
        controllerMap.values().stream().forEach(clusterWrapper::addVeniceControllerWrapper);
        clusterWrapperMap.put(clusterWrapper.getClusterName(), clusterWrapper);
        clusterWrapper.setExternalControllerDiscoveryURL(controllerMap.values().stream()
            .map(VeniceControllerWrapper::getControllerUrl).collect(Collectors.joining(",")));
      }
      final ZkServerWrapper finalZkServerWrapper = zkServerWrapper;
      final KafkaBrokerWrapper finalKafkaBrokerWrapper = kafkaBrokerWrapper;
      final BrooklinWrapper finalBrooklinWrapper = brooklinWrapper;
      final String finalClusterToD2 = clusterToD2;
      return (serviceName) -> new VeniceMultiClusterWrapper(null, finalZkServerWrapper, finalKafkaBrokerWrapper,
          finalBrooklinWrapper, clusterWrapperMap, controllerMap, finalClusterToD2);
    } catch (Exception e) {
      controllerMap.values().forEach(Utils::closeQuietlyWithErrorLogged);
      clusterWrapperMap.values().forEach(Utils::closeQuietlyWithErrorLogged);
      Utils.closeQuietlyWithErrorLogged(brooklinWrapper);
      Utils.closeQuietlyWithErrorLogged(kafkaBrokerWrapper);
      Utils.closeQuietlyWithErrorLogged(zkServerWrapper);
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
    controllers.values().forEach(IOUtils::closeQuietly);
    clusters.values().forEach(IOUtils::closeQuietly);
    IOUtils.closeQuietly(brooklinWrapper);
    IOUtils.closeQuietly(kafkaBrokerWrapper);
    IOUtils.closeQuietly(zkServerWrapper);
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

  public String getClusterToD2() {
    return clusterToD2;
  }

  public void restartControllers() {
    controllers.values().forEach(veniceControllerWrapper -> {
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
