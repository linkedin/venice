package com.linkedin.venice.integration.utils;

import com.linkedin.venice.helix.HelixReadOnlyStoreRepository;
import com.linkedin.venice.helix.HelixRoutingDataRepository;
import com.linkedin.venice.router.RouterServer;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import java.io.File;
import java.util.ArrayList;
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

  VeniceRouterWrapper(String serviceName, File dataDirectory, RouterServer service, String clusterName, int port, String zkAddress) {
    super(serviceName, dataDirectory);
    this.service = service;
    this.port = port;
    this.clusterName = clusterName;
    this.zkAddress = zkAddress;
  }

  static StatefulServiceProvider<VeniceRouterWrapper> generateService(String clusterName, KafkaBrokerWrapper kafkaBrokerWrapper) {
    // TODO: Once the ZK address used by Controller and Kafka are decoupled, change this
    String zkAddress = kafkaBrokerWrapper.getZkAddress();

    return (serviceName, port, dataDirectory) -> {
      RouterServer router = new RouterServer(port, sslPortFromPort(port), clusterName, zkAddress, new ArrayList<>(), SslUtils.getLocalSslFactory());
      return new VeniceRouterWrapper(serviceName, dataDirectory, router, clusterName, port, zkAddress);
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
    service = new RouterServer(port, sslPortFromPort(port), clusterName, zkAddress, new ArrayList<>(), SslUtils.getLocalSslFactory());
  }

  public HelixRoutingDataRepository getRoutingDataRepository(){
    return service.getRoutingDataRepository();
  }

  public HelixReadOnlyStoreRepository getMetaDataRepository() {
    return service.getMetadataRepository();
  }

  private static int sslPortFromPort(int port) {
    return port + 1;
  }
}
