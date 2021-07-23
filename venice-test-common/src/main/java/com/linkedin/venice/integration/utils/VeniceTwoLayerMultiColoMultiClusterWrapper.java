package com.linkedin.venice.integration.utils;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.samza.VeniceSystemFactory;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


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
      int numberOfRouters, int replicationFactor, Optional<VeniceProperties> parentControllerProperties,
      Optional<VeniceProperties> serverProperties) {
    return generateService(numberOfColos, numberOfClustersInEachColo, numberOfParentControllers, numberOfControllers,
        numberOfServers, numberOfRouters, replicationFactor, parentControllerProperties, Optional.empty(),
        serverProperties, false, MirrorMakerWrapper.DEFAULT_TOPIC_WHITELIST, false, Optional.empty());
  }

  static ServiceProvider<VeniceTwoLayerMultiColoMultiClusterWrapper> generateService(int numberOfColos,
      int numberOfClustersInEachColo, int numberOfParentControllers, int numberOfControllers, int numberOfServers,
      int numberOfRouters, int replicationFactor, Optional<VeniceProperties> parentControllerProperties,
      Optional<Properties> childControllerProperties, Optional<VeniceProperties> serverProperties, boolean multiD2, String whitelistConfigForKMM,
      boolean forkServer, Optional<Integer> parentKafkaPort) {

    final List<VeniceControllerWrapper> parentControllers = new ArrayList<>(numberOfParentControllers);
    final List<VeniceMultiClusterWrapper> multiClusters = new ArrayList<>(numberOfColos);
    final List<MirrorMakerWrapper> mirrorMakers = new ArrayList<>(numberOfColos);

    ZkServerWrapper zkServer = null;
    KafkaBrokerWrapper parentKafka = null;
    try {
      zkServer = ServiceFactory.getZkServer();
      parentKafka = parentKafkaPort.isPresent() ? ServiceFactory.getKafkaBroker(zkServer, parentKafkaPort.get())
          : ServiceFactory.getKafkaBroker(zkServer);

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
        String coloName = "dc-" + i;
        VeniceMultiClusterWrapper multiClusterWrapper =
            ServiceFactory.getVeniceMultiClusterWrapper(coloName, numberOfClustersInEachColo, numberOfControllers, numberOfServers,
                numberOfRouters, replicationFactor, false, true, multiD2, childControllerProperties, serverProperties, forkServer);
        multiClusters.add(multiClusterWrapper);
      }

      VeniceControllerWrapper[] childControllers =
          multiClusters.stream().map(cluster -> cluster.getRandomController()).toArray(VeniceControllerWrapper[]::new);

      // Setup D2 for parent controller
      D2TestUtils.setupD2Config(parentKafka.getZkAddress(), false, D2TestUtils.CONTROLLER_CLUSTER_NAME, VeniceSystemFactory.VENICE_PARENT_D2_SERVICE, false);
      // Create parentControllers for multi-cluster
      for (int i = 0; i < numberOfParentControllers; i++) {
        // random controller from each multicluster, in reality this should include all controllers, not just one
        VeniceControllerWrapper parentController = ServiceFactory.getVeniceParentController(
            clusterNames, parentKafka.getZkAddress(), parentKafka, childControllers, clusterToD2, false,
            replicationFactor, parentControllerProperties.orElseGet(() -> new VeniceProperties()), Optional.empty());
        parentControllers.add(parentController);
      }

      // Create MirrorMakers
      for (VeniceMultiClusterWrapper multiCluster : multiClusters) {
        MirrorMakerWrapper mirrorMakerWrapper = ServiceFactory.getKafkaMirrorMaker(parentKafka, multiCluster.getKafkaBrokerWrapper(), whitelistConfigForKMM);
        mirrorMakers.add(mirrorMakerWrapper);
      }

      final ZkServerWrapper finalZkServer = zkServer;
      final KafkaBrokerWrapper finalParentKafka = parentKafka;
      return (serviceName) -> new VeniceTwoLayerMultiColoMultiClusterWrapper(
          null, finalZkServer, finalParentKafka, multiClusters, parentControllers, mirrorMakers);
    } catch (Exception e) {
      mirrorMakers.forEach(IOUtils::closeQuietly);
      parentControllers.forEach(IOUtils::closeQuietly);
      multiClusters.forEach(IOUtils::closeQuietly);
      IOUtils.closeQuietly(parentKafka);
      IOUtils.closeQuietly(zkServer);
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
    mirrorMakers.forEach(IOUtils::closeQuietly);
    parentControllers.forEach(IOUtils::closeQuietly);
    clusters.forEach(IOUtils::closeQuietly);
    IOUtils.closeQuietly(parentKafkaBrokerWrapper);
    IOUtils.closeQuietly(zkServerWrapper);
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

  public void addMirrorMakerBetween(VeniceMultiClusterWrapper srcColo, VeniceMultiClusterWrapper dstColo,
      String whitelistConfigForKMM) {
    MirrorMakerWrapper mirrorMakerWrapper =
        ServiceFactory.getKafkaMirrorMaker(srcColo.getKafkaBrokerWrapper(), dstColo.getKafkaBrokerWrapper(),
            whitelistConfigForKMM);
    mirrorMakers.add(mirrorMakerWrapper);
  }

  public void addMirrorMakerBetween(KafkaBrokerWrapper srcKafka, KafkaBrokerWrapper dstKafka,
      String whitelistConfigForKMM) {
    MirrorMakerWrapper mirrorMakerWrapper =
        ServiceFactory.getKafkaMirrorMaker(srcKafka, dstKafka, whitelistConfigForKMM);
    mirrorMakers.add(mirrorMakerWrapper);
  }
}
