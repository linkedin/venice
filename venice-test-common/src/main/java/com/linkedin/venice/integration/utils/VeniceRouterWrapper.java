package com.linkedin.venice.integration.utils;

import com.linkedin.venice.helix.HelixReadOnlyStoreRepository;
import com.linkedin.venice.helix.HelixRoutingDataRepository;
import com.linkedin.venice.router.RouterServer;
import com.linkedin.venice.router.ZkRoutersClusterManager;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import java.io.File;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.TimeUnit;


/**
 * A wrapper for the {@link VeniceRouterWrapper}.
 */
public class VeniceRouterWrapper extends ProcessWrapper {

  public static final String SERVICE_NAME = "VeniceRouter";

  private RouterServer service;
  private final int port;
  private final String clusterName;
  private final String zkAddress;
  private final boolean sslToStorageNode;

  VeniceRouterWrapper(String serviceName, File dataDirectory, RouterServer service, String clusterName, int port, String zkAddress, boolean sslToStorageNode) {
    super(serviceName, dataDirectory);
    this.service = service;
    this.port = port;
    this.clusterName = clusterName;
    this.zkAddress = zkAddress;
    this.sslToStorageNode = sslToStorageNode;
  }

  static StatefulServiceProvider<VeniceRouterWrapper> generateService(String clusterName, KafkaBrokerWrapper kafkaBrokerWrapper, boolean sslToStorageNodes) {
    // TODO: Once the ZK address used by Controller and Kafka are decoupled, change this
    String zkAddress = kafkaBrokerWrapper.getZkAddress();

    return (serviceName, port, dataDirectory) -> {
      RouterServer router = new RouterServer(port, sslPortFromPort(port), clusterName, zkAddress, new ArrayList<>(), Optional.of(SslUtils.getLocalSslFactory()), sslToStorageNodes);
      return new VeniceRouterWrapper(serviceName, dataDirectory, router, clusterName, port, zkAddress, sslToStorageNodes);
    };
  }

  @Override
  public String getHost() {
    return DEFAULT_HOST_NAME;
  }

  @Override
  public int getPort() {
    return port;
  }

  public int getSslPort() {
    return sslPortFromPort(port);
  }

  @Override
  protected void internalStart() throws Exception {
    service.start();

    TestUtils.waitForNonDeterministicCompletion(
        IntegrationTestUtils.MAX_ASYNC_START_WAIT_TIME_MS,
        TimeUnit.MILLISECONDS,
        () -> service.isStarted());
  }

  @Override
  protected void internalStop() throws Exception {
    service.stop();
  }

  @Override
  protected void newProcess()
      throws Exception {
    service = new RouterServer(port, sslPortFromPort(port), clusterName, zkAddress, new ArrayList<>(), Optional.of(SslUtils.getLocalSslFactory()), sslToStorageNode);
  }

  public HelixRoutingDataRepository getRoutingDataRepository(){
    return service.getRoutingDataRepository();
  }

  public HelixReadOnlyStoreRepository getMetaDataRepository() {
    return service.getMetadataRepository();
  }

  public ZkRoutersClusterManager getRoutersClusterManager() {
    return service.getRoutersClusterManager();
  }

  private static int sslPortFromPort(int port) {
    return port + 1;
  }
}
