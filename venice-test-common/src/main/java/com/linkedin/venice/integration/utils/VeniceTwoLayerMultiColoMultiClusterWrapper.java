package com.linkedin.venice.integration.utils;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;


public class VeniceTwoLayerMultiColoMultiClusterWrapper extends ProcessWrapper {
  public static final String SERVICE_NAME = "VeniceTwoLayerMultiCluster";
  private final List<VeniceMultiClusterWrapper> clusters;
  private final List<VeniceControllerWrapper> parentControllers;
  private final List<MirrorMakerWrapper> mirrorMakers;
  private final ZkServerWrapper zkServerWrapper;
  private final KafkaBrokerWrapper parentKafkaBrokerWrapper;

  VeniceTwoLayerMultiColoMultiClusterWrapper(File dataDirectory, ZkServerWrapper zkServerWrapper,
      KafkaBrokerWrapper parentKafkaBrokerWrapper, List<VeniceMultiClusterWrapper> clusters,
      List<VeniceControllerWrapper> parentControllers, List<MirrorMakerWrapper> mirrorMakers) {
    super(SERVICE_NAME, dataDirectory);
    this.zkServerWrapper = zkServerWrapper;
    this.parentKafkaBrokerWrapper = parentKafkaBrokerWrapper;
    this.parentControllers = parentControllers;
    this.clusters = clusters;
    this.mirrorMakers = mirrorMakers;
  }

  static ServiceProvider<VeniceTwoLayerMultiColoMultiClusterWrapper> generateService(int numberOfColos,
      int numberOfClustersInEachColo, int numberOfParentControllers, int numberOfControllers, int numberOfServers,
      int numberOfRouters, int replicationFactor, Optional<Integer> zkPort, Optional<VeniceProperties> parentControllerProperties,
      Optional<VeniceProperties> serverProperties) {
    return generateService(numberOfColos, numberOfClustersInEachColo, numberOfParentControllers, numberOfControllers, numberOfServers,
        numberOfRouters, replicationFactor, zkPort, parentControllerProperties, serverProperties, false, MirrorMakerWrapper.DEFAULT_TOPIC_WHITELIST);
  }

  static ServiceProvider<VeniceTwoLayerMultiColoMultiClusterWrapper> generateService(int numberOfColos,
      int numberOfClustersInEachColo, int numberOfParentControllers, int numberOfControllers, int numberOfServers,
      int numberOfRouters, int replicationFactor, Optional<Integer> zkPort, Optional<VeniceProperties> parentControllerProperties,
      Optional<VeniceProperties> serverProperties, boolean multiD2, String whitelistConfigForKMM) {

    final List<VeniceControllerWrapper> parentControllers = new ArrayList<>(numberOfParentControllers);
    final List<VeniceMultiClusterWrapper> multiClusters = new ArrayList<>(numberOfColos);
    final List<MirrorMakerWrapper> mirrorMakers = new ArrayList<>(numberOfColos);

    ZkServerWrapper zk = zkPort.isPresent() ? ServiceFactory.getZkServer(zkPort.get()) : ServiceFactory.getZkServer();
    KafkaBrokerWrapper parentKafka = ServiceFactory.getKafkaBroker(zk);

    String clusterToD2 = "";
    String[] clusterNames = new String[numberOfClustersInEachColo];
    for (int i = 0; i < numberOfClustersInEachColo; i++) {
      String clusterName = "venice-cluster" + i;
      clusterNames[i] = clusterName;
      if (multiD2) {
        clusterToD2 += clusterName + ":venice-" + i + ",";
      } else {
        clusterToD2 += TestUtils.getClusterToDefaultD2String(clusterName) + ",";
      }
    }
    clusterToD2 = clusterToD2.substring(0, clusterToD2.length() - 1);

    // Create multiclusters
    for (int i = 0; i < numberOfColos; i++) {
      VeniceMultiClusterWrapper multiClusterWrapper =
          ServiceFactory.getVeniceMultiClusterWrapper(numberOfClustersInEachColo, numberOfControllers, numberOfServers,
              numberOfRouters, replicationFactor, false, true, multiD2, serverProperties);
      multiClusters.add(multiClusterWrapper);
    }

    VeniceControllerWrapper[] childControllers =
        multiClusters.stream().map(cluster -> cluster.getRandomController()).toArray(VeniceControllerWrapper[]::new);

    // Create parentControllers for multi-cluster
    for (int i = 0; i < numberOfParentControllers; i++) {
      if (parentControllerProperties.isPresent()) {
        VeniceControllerWrapper parentController =
            ServiceFactory.getVeniceParentController(clusterNames, parentKafka.getZkAddress(), parentKafka,
                childControllers,
                // random controller from each multicluster, in reality this should include all controllers, not just one
                clusterToD2, false, replicationFactor, parentControllerProperties.get());
        parentControllers.add(parentController);
      } else {
        VeniceControllerWrapper parentController =
            ServiceFactory.getVeniceParentController(clusterNames, parentKafka.getZkAddress(), parentKafka,
                childControllers,
                // random controller from each multicluster, in reality this should include all controllers, not just one
                clusterToD2, false, replicationFactor);
        parentControllers.add(parentController);
      }
    }

    // Create MirrorMakers
    for (VeniceMultiClusterWrapper multicluster : multiClusters) {
      MirrorMakerWrapper mirrorMakerWrapper = ServiceFactory.getKafkaMirrorMaker(parentKafka, multicluster.getKafkaBrokerWrapper(), whitelistConfigForKMM);
      mirrorMakers.add(mirrorMakerWrapper);
    }

    return (serviceName, port) -> new VeniceTwoLayerMultiColoMultiClusterWrapper(null, zk, parentKafka,
        multiClusters, parentControllers, mirrorMakers);
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
    CompletableFuture.runAsync(() -> {
      clusters.stream().forEach(IOUtils::closeQuietly);
      parentControllers.stream().forEach(IOUtils::closeQuietly);
      mirrorMakers.stream().forEach(IOUtils::closeQuietly);
      parentKafkaBrokerWrapper.close();
      zkServerWrapper.close();
    });
  }

  @Override
  protected void newProcess() throws Exception {
    throw new UnsupportedOperationException("Cluster does not support to create new process.");
  }

  public List<VeniceMultiClusterWrapper> getClusters() {
    return clusters;
  }

  public List<VeniceControllerWrapper> getParentControllers() {
    return parentControllers;
  }

  public ZkServerWrapper getZkServerWrapper() {
    return zkServerWrapper;
  }

  public KafkaBrokerWrapper getParentKafkaBrokerWrapper() {
    return parentKafkaBrokerWrapper;
  }

  public VeniceControllerWrapper getMasterController(String clusterName) {
    return getMasterController(clusterName, 60 * Time.MS_PER_SECOND);
  }

  public VeniceControllerWrapper getMasterController(String clusterName, long timeoutMs) {
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    while (System.nanoTime() < deadline) {
      for (VeniceControllerWrapper controller : parentControllers) {
        if (controller.isRunning() && controller.isMasterController(clusterName)) {
          return controller;
        }
      }
      Utils.sleep(Time.MS_PER_SECOND);
    }
    throw new VeniceException("Master controller does not exist, cluster=" + clusterName);
  }
}
