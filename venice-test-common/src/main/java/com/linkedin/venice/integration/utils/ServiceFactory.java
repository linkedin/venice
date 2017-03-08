package com.linkedin.venice.integration.utils;

import com.linkedin.venice.exceptions.VeniceException;

import com.linkedin.venice.utils.Time;
import org.apache.log4j.Logger;

/**
 * A factory for generating Venice services and external service instances
 * used in integration tests.
 */
public class ServiceFactory {
  // CLASS-LEVEL STATE AND APIs

  private static final Logger LOGGER = Logger.getLogger(ZkServerWrapper.class);

  // Test config
  private static final int MAX_ATTEMPT = 10;
  private static final int MAX_ASYNC_WAIT_TIME_MS = 10 * Time.MS_PER_SECOND;
  private static final int DEFAULT_REPLICATION_FACTOR =1;
  private static final int DEFAULT_PARTITION_SIZE_BYTES = 100;
  private static final long DEFAULT_DELAYED_TO_REBALANCE_MS = 0; // By default, disable the delayed rebalance for testing.

  /**
   * @return an instance of {@link ZkServerWrapper}
   */
  public static ZkServerWrapper getZkServer()  {
    return getStatefulService(ZkServerWrapper.SERVICE_NAME, ZkServerWrapper.generateService());
  }

  /**
   * @return an instance of {@link KafkaBrokerWrapper}
   */
  public static KafkaBrokerWrapper getKafkaBroker() {
    /**
     * Get the ZK dependency outside of the lambda, to avoid time complexity of
     * O({@value MAX_ATTEMPT} ^2) on the amount of retries. {@link #getZkServer()}
     * has its own retries, so we can assume it's reliable enough.
     */

    return getKafkaBroker(ServiceFactory.getZkServer());
  }

  static KafkaBrokerWrapper getKafkaBroker(ZkServerWrapper zkServerWrapper) {
    return getStatefulService(KafkaBrokerWrapper.SERVICE_NAME, KafkaBrokerWrapper.generateService(zkServerWrapper));
  }

  /**
   * @return an instance of {@link com.linkedin.venice.controller.VeniceControllerService}
   */
  public static VeniceControllerWrapper getVeniceController(String clusterName, KafkaBrokerWrapper kafkaBrokerWrapper) {
    return getVeniceController(clusterName, kafkaBrokerWrapper, DEFAULT_REPLICATION_FACTOR, DEFAULT_PARTITION_SIZE_BYTES,
        DEFAULT_DELAYED_TO_REBALANCE_MS, DEFAULT_REPLICATION_FACTOR);
  }

  /**
   * @return an instance of {@link com.linkedin.venice.controller.VeniceControllerService}
   */
  public static VeniceControllerWrapper getVeniceController(String clusterName, KafkaBrokerWrapper kafkaBrokerWrapper,
      int replicaFactor, int partitionSize, long delayToRebalanceMS, int minActiveReplica) {
    return getStatefulService(VeniceControllerWrapper.SERVICE_NAME,
        VeniceControllerWrapper.generateService(clusterName, kafkaBrokerWrapper.getZkAddress(), kafkaBrokerWrapper, false, replicaFactor, partitionSize,
            delayToRebalanceMS, minActiveReplica, null));
  }

  /**
   * @return an instance of {@link com.linkedin.venice.controller.VeniceControllerService}, which will be working in parent mode.
   */
  public static VeniceControllerWrapper getVeniceParentController(String clusterName, String zkAddress, KafkaBrokerWrapper kafkaBrokerWrapper, VeniceControllerWrapper childController) {
    return getStatefulService(
        VeniceControllerWrapper.SERVICE_NAME,
        VeniceControllerWrapper.generateService(clusterName, zkAddress, kafkaBrokerWrapper, true, DEFAULT_REPLICATION_FACTOR,
            DEFAULT_PARTITION_SIZE_BYTES, DEFAULT_DELAYED_TO_REBALANCE_MS, DEFAULT_REPLICATION_FACTOR, childController));
  }

  /**
   * Deprecated, use the replacement method that accepts a boolean for whether to use ssl or not
   * @param clusterName
   * @param kafkaBrokerWrapper
   * @param enableServerWhitelist
   * @param autoJoinWhitelist
   * @return
   */
  @Deprecated
  public static VeniceServerWrapper getVeniceServer(String clusterName, KafkaBrokerWrapper kafkaBrokerWrapper,
      boolean enableServerWhitelist, boolean autoJoinWhitelist) {
    return getStatefulService(VeniceServerWrapper.SERVICE_NAME,
        VeniceServerWrapper.generateService(clusterName, kafkaBrokerWrapper, enableServerWhitelist, autoJoinWhitelist, false));
  }

  public static VeniceServerWrapper getVeniceServer(String clusterName, KafkaBrokerWrapper kafkaBrokerWrapper,
      boolean enableServerWhitelist, boolean autoJoinWhitelist, boolean ssl) {
    return getStatefulService(VeniceServerWrapper.SERVICE_NAME,
        VeniceServerWrapper.generateService(clusterName, kafkaBrokerWrapper, enableServerWhitelist, autoJoinWhitelist, ssl));
  }

  /**
   * Note: Assumes that helix and kafka are using the same zookeeper, uses the zookeeper from the kafkaBrokerWrapper
   */
  static VeniceRouterWrapper getVeniceRouter(String clusterName, KafkaBrokerWrapper kafkaBrokerWrapper){
    return getService(VeniceRouterWrapper.SERVICE_NAME,
      VeniceRouterWrapper.generateService(clusterName, kafkaBrokerWrapper));
  }

  /**
   * Initialize MockVeniceRouterWrapper without D2.
   * @return
   */
  public static MockVeniceRouterWrapper getMockVeniceRouter() {
    return getService(MockVeniceRouterWrapper.SERVICE_NAME, null);
  }

  public static MockVeniceRouterWrapper getMockVeniceRouter(String zkAddress){
    return getService(
        MockVeniceRouterWrapper.SERVICE_NAME,
        MockVeniceRouterWrapper.generateService(zkAddress));
  }

  /**
   * Initialize MockHttpServerWrapper without D2.
   * @param serviceName
   * @return
   */
  public static MockHttpServerWrapper getMockHttpServer(String serviceName) {
    return ServiceFactory.getMockHttpServer(serviceName, null);
  }

  /**
   * Initialize MockHttpServerWrapper, this function will setup a simple http server
   * If zkAddress is not null, it will create D2Server and register its URI to the D2Server.
   *
   * @param serviceName
   * @param zkAddress
   * @return
   */
  public static MockHttpServerWrapper getMockHttpServer(String serviceName, String zkAddress) {
    return getService(serviceName, MockHttpServerWrapper.generateService(zkAddress));
  }

  public static VeniceClusterWrapper getVeniceCluster() {
    // Get the cluster with 1 controller, 1 server and 1 router by default.
    return getVeniceCluster(1, 1, 1, DEFAULT_REPLICATION_FACTOR, DEFAULT_PARTITION_SIZE_BYTES);
  }

  public static VeniceClusterWrapper getVeniceCluster(int numberOfControllers, int numberOfServers, int numberOfRouter) {
    return getVeniceCluster(numberOfControllers, numberOfServers, numberOfRouter, DEFAULT_REPLICATION_FACTOR,
        DEFAULT_PARTITION_SIZE_BYTES);
  }

  public static VeniceClusterWrapper getVeniceCluster(int numberOfControllers, int numberOfServers, int numberOfRouter, int replicaFactor, int partitionSize) {
    // As we introduce bootstrap state in to venice and transition from bootstrap to online will be blocked until get
    // "end of push" message. We need more venice server for testing, because there is a limitation in helix about how
    // many uncompleted transitions one server could handle. So if we still use one server and that limitation is
    // reached, venice can not create new resource which will cause failed tests.
    // Enable to start multiple controllers and routers too, so that we could fail some of them to do the failover integration test.
    return getService(VeniceClusterWrapper.SERVICE_NAME,
        VeniceClusterWrapper.generateService(numberOfControllers, numberOfServers, numberOfRouter, replicaFactor,
            partitionSize, false, false, DEFAULT_DELAYED_TO_REBALANCE_MS, replicaFactor - 1));
  }
  // TODO instead of passing more and more parameters here, we could create a class ClusterOptions to include all of options to start a cluster. Then we only need one parameter here.
  public static VeniceClusterWrapper getVeniceCluster(int numberOfControllers, int numberOfServers, int numberOfRouter,
      int replicaFactor, int partitionSize, boolean enableWhitelist, boolean enableAutoJoinWhitelist, long delayToRebalanceMS, int minActiveReplica) {
    return getService(VeniceClusterWrapper.SERVICE_NAME,
        VeniceClusterWrapper.generateService(numberOfControllers, numberOfServers, numberOfRouter, replicaFactor,
            partitionSize, enableWhitelist, enableAutoJoinWhitelist, delayToRebalanceMS, minActiveReplica));
  }

  private static <S extends ProcessWrapper> S getStatefulService(String serviceName, StatefulServiceProvider<S> serviceProvider) {
    return getService(serviceName, serviceProvider);
  }

  private static <S extends ProcessWrapper> S getService(String serviceName, ServiceProvider<S> serviceProvider) {
    // Just some initial state. If the fabric of space-time holds up, you should never see these strings.
    Exception lastException = new VeniceException("There is no spoon.");
    String errorMessage = "If you see this message, something went horribly wrong.";

    for (int attempt = 1; attempt <= MAX_ATTEMPT; attempt++) {
      try {
        S wrapper = serviceProvider.get(serviceName, IntegrationTestUtils.getFreePort());

        // N.B.: The contract for start() is that it should block until the wrapped service is fully started.
        wrapper.start();
        return wrapper;
      } catch (Exception e) {
        lastException = e;
        errorMessage = "Got " + e.getClass().getSimpleName() + " while trying to start " + serviceName +
            ". Attempt #" + attempt + "/" + MAX_ATTEMPT + ".";
        LOGGER.info(errorMessage, e);
      }
    }

    throw new VeniceException(errorMessage + " Aborting.", lastException);
  }
}
