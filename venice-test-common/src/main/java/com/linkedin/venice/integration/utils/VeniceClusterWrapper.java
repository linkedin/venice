package com.linkedin.venice.integration.utils;

import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;

import com.linkedin.venice.utils.TestUtils;
import java.io.File;
import java.util.ArrayList;
import java.util.List;


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
  private final List<VeniceServerWrapper> veniceServerWrappers;
  private final VeniceRouterWrapper veniceRouterWrapper;

  VeniceClusterWrapper(File dataDirectory,
                       String clusterName,
                       ZkServerWrapper zkServerWrapper,
                       KafkaBrokerWrapper kafkaBrokerWrapper,
                       VeniceControllerWrapper veniceControllerWrapper,
                       List<VeniceServerWrapper> veniceServerWrappers,
                       VeniceRouterWrapper veniceRouterWrapper) {
    super(SERVICE_NAME, dataDirectory);
    this.clusterName = clusterName;
    this.zkServerWrapper = zkServerWrapper;
    this.kafkaBrokerWrapper = kafkaBrokerWrapper;
    this.veniceControllerWrapper = veniceControllerWrapper;
    this.veniceServerWrappers = veniceServerWrappers;
    this.veniceRouterWrapper = veniceRouterWrapper;
  }

  static ServiceProvider<VeniceClusterWrapper> generateService(int numberOfServers) {
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
    List<VeniceServerWrapper> veniceServerWrappers = new ArrayList<>();
    for(int i=0;i<numberOfServers;i++){

      veniceServerWrappers.add(ServiceFactory.getVeniceServer(clusterName, kafkaBrokerWrapper, false, false));
    }
    VeniceRouterWrapper veniceRouterWrapper = ServiceFactory.getVeniceRouter(clusterName, kafkaBrokerWrapper);

    return (serviceName, port) -> new VeniceClusterWrapper(
        null, clusterName, zkServerWrapper, kafkaBrokerWrapper, veniceControllerWrapper, veniceServerWrappers, veniceRouterWrapper);
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

  public List<VeniceServerWrapper> getVeniceServers() {
    return veniceServerWrappers;
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
    for(VeniceServerWrapper veniceServerWrapper:veniceServerWrappers) {
      veniceServerWrapper.stop();
    }
    veniceControllerWrapper.stop();
    kafkaBrokerWrapper.stop();
    zkServerWrapper.stop();
  }

  /**
   * @see {@link VeniceControllerWrapper#getNewStoreVersion(String routerUrls, String clusterName)}
   */
  public VersionCreationResponse getNewStoreVersion() {
    String routerUrl = "http://" + getVeniceRouter().getAddress();
    return veniceControllerWrapper.getNewStoreVersion(routerUrl, clusterName);
  }
}
