package com.linkedin.venice.integration.utils;

import com.linkedin.venice.router.RouterServer;
import java.io.File;
import java.util.ArrayList;


/**
 * A wrapper for the {@link VeniceRouterWrapper}.
 */
public class VeniceRouterWrapper extends ProcessWrapper {

  public static final String SERVICE_NAME = "VeniceRouter";

  private final RouterServer service;
  private final int port;

  VeniceRouterWrapper(String serviceName, File dataDirectory, RouterServer service, String clusterName, int port) {
    super(serviceName, dataDirectory);
    this.service = service;
    this.port = port;
  }

  static StatefulServiceProvider<VeniceRouterWrapper> generateService(String clusterName, KafkaBrokerWrapper kafkaBrokerWrapper) {
    // TODO: Once the ZK address used by Controller and Kafka are decoupled, change this
    String zkAddress = kafkaBrokerWrapper.getZkAddress();

    return (serviceName, port, dataDirectory) -> {
      RouterServer router = new RouterServer(port, clusterName, zkAddress, new ArrayList<>());
      return new VeniceRouterWrapper(serviceName, dataDirectory, router, clusterName, port);
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

  @Override
  protected void start() throws Exception {
    service.start();
  }

  @Override
  protected void stop() throws Exception {
    service.stop();
  }

}
