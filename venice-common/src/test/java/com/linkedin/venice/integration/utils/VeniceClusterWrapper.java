package com.linkedin.venice.integration.utils;

import com.linkedin.venice.exceptions.VeniceException;

import java.io.File;

/**
 * This is the whole enchilada:
 * - {@link ZkServerWrapper}
 * - {@link KafkaBrokerWrapper}
 * - {@link VeniceControllerWrapper}
 * - {@link VeniceServerWrapper}
 */
public class VeniceClusterWrapper extends ProcessWrapper {
  public static final String SERVICE_NAME = "VeniceCluster";
  private final String clusterName;
  private final ZkServerWrapper zkServerWrapper;
  private final KafkaBrokerWrapper kafkaBrokerWrapper;
  private final VeniceControllerWrapper veniceControllerWrapper;
  private final VeniceServerWrapper veniceServerWrapper;
  private final VeniceRouterWrapper veniceRouterWrapper;

  VeniceClusterWrapper(File dataDirectory,
                       String clusterName,
                       ZkServerWrapper zkServerWrapper,
                       KafkaBrokerWrapper kafkaBrokerWrapper,
                       VeniceControllerWrapper veniceControllerWrapper,
                       VeniceServerWrapper veniceServerWrapper,
                       VeniceRouterWrapper veniceRouterWrapper) {
    super(SERVICE_NAME, dataDirectory);
    this.clusterName = clusterName;
    this.zkServerWrapper = zkServerWrapper;
    this.kafkaBrokerWrapper = kafkaBrokerWrapper;
    this.veniceControllerWrapper = veniceControllerWrapper;
    this.veniceServerWrapper = veniceServerWrapper;
    this.veniceRouterWrapper = veniceRouterWrapper;
  }

  static ServiceProvider<VeniceClusterWrapper> generateService() {
    /**
     * We get the various dependencies outside of the lambda, to avoid having a time
     * complexity of O(N^2) on the amount of retries. The calls have their own retries,
     * so we can assume they're reliable enough.
     */

    String clusterName = TestUtils.getUniqueString("venice-cluster");
    ZkServerWrapper zkServerWrapper = ServiceFactory.getZkServer();
    KafkaBrokerWrapper kafkaBrokerWrapper = ServiceFactory.getKafkaBroker(zkServerWrapper);
    VeniceControllerWrapper veniceControllerWrapper = ServiceFactory.getVeniceController(clusterName,
        kafkaBrokerWrapper);
    VeniceServerWrapper veniceServerWrapper = ServiceFactory.getVeniceServer(clusterName, kafkaBrokerWrapper);
    VeniceRouterWrapper veniceRouterWrapper = ServiceFactory.getVeniceRouter(clusterName, kafkaBrokerWrapper);

    return (serviceName, port) -> new VeniceClusterWrapper(
        null, clusterName, zkServerWrapper, kafkaBrokerWrapper, veniceControllerWrapper, veniceServerWrapper, veniceRouterWrapper);
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

  public VeniceControllerWrapper getVeniceController() {
    return veniceControllerWrapper;
  }

  public VeniceServerWrapper getVeniceServer() {
    return veniceServerWrapper;
  }

  public VeniceRouterWrapper getVeniceRouter(){
    return veniceRouterWrapper;
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
  protected void start() throws Exception {
    // Everything should already be started. So this is a no-op.
//    zkServerWrapper.start();
//    kafkaBrokerWrapper.start();
//    veniceControllerWrapper.start();
//    veniceServerWrapper.start();
  }

  @Override
  protected void stop() throws Exception {
    // Stop called in reverse order of dependency
    veniceServerWrapper.stop();
    veniceControllerWrapper.stop();
    kafkaBrokerWrapper.stop();
    zkServerWrapper.stop();
  }

  /**
   * @see {@link VeniceControllerWrapper#getNewStoreVersion(String clusterName)}
   */
  public String getNewStoreVersion() {
    String routerUrl = "http://" + getVeniceRouter().getAddress();
    return veniceControllerWrapper.getNewStoreVersion(routerUrl, clusterName);
  }
}
