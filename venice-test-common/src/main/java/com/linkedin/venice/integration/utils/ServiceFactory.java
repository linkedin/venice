package com.linkedin.venice.integration.utils;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.server.AdminSparkServer;
import com.linkedin.venice.exceptions.VeniceException;

import com.linkedin.venice.utils.Time;
import io.tehuti.metrics.MetricsRepository;
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
  private static final boolean DEFAULT_SSL = false;

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

  public static BrooklinWrapper getBrooklinWrapper(KafkaBrokerWrapper kafka){
    return getService(BrooklinWrapper.SERVICE_NAME, BrooklinWrapper.generateService(kafka));
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
   * Get a running admin spark server with a passed-in {@link Admin}, good for tests that want to provide a mock admin
   * @param admin
   * @return
   */
  public static AdminSparkServer getMockAdminSparkServer(Admin admin) {
    return getService("MockAdminSparkServer", (serviceName, port) -> {
      AdminSparkServer server = new AdminSparkServer(port, admin, new MetricsRepository());
      server.start();
      return server;
    });
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
        VeniceServerWrapper.generateService(clusterName, kafkaBrokerWrapper, enableServerWhitelist, autoJoinWhitelist, DEFAULT_SSL));
  }

  public static VeniceServerWrapper getVeniceServer(String clusterName, KafkaBrokerWrapper kafkaBrokerWrapper,
      boolean enableServerWhitelist, boolean autoJoinWhitelist, boolean ssl) {
    return getStatefulService(VeniceServerWrapper.SERVICE_NAME,
        VeniceServerWrapper.generateService(clusterName, kafkaBrokerWrapper, enableServerWhitelist, autoJoinWhitelist, ssl));
  }

  /**
   * Note: Assumes that helix and kafka are using the same zookeeper, uses the zookeeper from the kafkaBrokerWrapper
   */
  static VeniceRouterWrapper getVeniceRouter(String clusterName, KafkaBrokerWrapper kafkaBrokerWrapper, boolean sslToStorageNodes){
    return getService(VeniceRouterWrapper.SERVICE_NAME,
      VeniceRouterWrapper.generateService(clusterName, kafkaBrokerWrapper, sslToStorageNodes));
  }

  public static MockVeniceRouterWrapper getMockVeniceRouter(String zkAddress, boolean sslToStorageNodes){
    return getService(
        MockVeniceRouterWrapper.SERVICE_NAME,
        MockVeniceRouterWrapper.generateService(zkAddress, sslToStorageNodes));
  }

  public static MockD2ServerWrapper getMockD2Server(String serviceName) {
    return getMockD2Server(serviceName, D2TestUtils.DEFAULT_TEST_CLUSTER_NAME, D2TestUtils.DEFAULT_TEST_SERVICE_NAME);
  }

  public static MockD2ServerWrapper getMockD2Server(String serviceName, String d2ClusterName, String d2ServiceName) {
    return getService(serviceName, MockD2ServerWrapper.generateService(d2ClusterName, d2ServiceName));
  }

  /**
   * Initialize MockHttpServerWrapper, this function will setup a simple http server
   */
  public static MockHttpServerWrapper getMockHttpServer(String serviceName) {
    return getService(serviceName, MockHttpServerWrapper.generateService());
  }

  public static VeniceClusterWrapper getVeniceCluster() {
    return getVeniceCluster(DEFAULT_SSL);
  }

  public static VeniceClusterWrapper getVeniceCluster(boolean ssl) {
    return getVeniceCluster(1, 1, 1, DEFAULT_REPLICATION_FACTOR, DEFAULT_PARTITION_SIZE_BYTES, ssl);
  }

  public static VeniceClusterWrapper getVeniceCluster(int numberOfControllers, int numberOfServers, int numberOfRouter) {
    return getVeniceCluster(numberOfControllers, numberOfServers, numberOfRouter, DEFAULT_REPLICATION_FACTOR,
        DEFAULT_PARTITION_SIZE_BYTES, DEFAULT_SSL);
  }

  @Deprecated
  public static VeniceClusterWrapper getVeniceCluster(int numberOfControllers, int numberOfServers, int numberOfRouter, int replicationFactor, int partitionSize){
    return getVeniceCluster(numberOfControllers, numberOfServers, numberOfRouter, replicationFactor, partitionSize, DEFAULT_SSL);
  }

  public static VeniceClusterWrapper getVeniceCluster(int numberOfControllers, int numberOfServers, int numberOfRouter, int replicationFactor, int partitionSize, boolean sslToStorageNodes) {
    // As we introduce bootstrap state in to venice and transition from bootstrap to online will be blocked until get
    // "end of push" message. We need more venice server for testing, because there is a limitation in helix about how
    // many uncompleted transitions one server could handle. So if we still use one server and that limitation is
    // reached, venice can not create new resource which will cause failed tests.
    // Enable to start multiple controllers and routers too, so that we could fail some of them to do the failover integration test.
    return getService(VeniceClusterWrapper.SERVICE_NAME,
        VeniceClusterWrapper.generateService(numberOfControllers, numberOfServers, numberOfRouter, replicationFactor,
            partitionSize, false, false, DEFAULT_DELAYED_TO_REBALANCE_MS, replicationFactor - 1, sslToStorageNodes));
  }

  @Deprecated
  public static VeniceClusterWrapper getVeniceCluster(int numberOfControllers, int numberOfServers, int numberOfRouter,
      int replicationFactor, int partitionSize, boolean enableWhitelist, boolean enableAutoJoinWhitelist,
      long delayToRebalanceMS, int minActiveReplica) {
    return getVeniceCluster(numberOfControllers, numberOfServers, numberOfRouter, replicationFactor, partitionSize,
        enableWhitelist, enableAutoJoinWhitelist, delayToRebalanceMS, minActiveReplica, DEFAULT_SSL);
  }
  // TODO instead of passing more and more parameters here, we could create a class ClusterOptions to include all of options to start a cluster. Then we only need one parameter here.
  // Or a builder pattern
  public static VeniceClusterWrapper getVeniceCluster(int numberOfControllers, int numberOfServers, int numberOfRouter,
      int replicaFactor, int partitionSize, boolean enableWhitelist, boolean enableAutoJoinWhitelist,
      long delayToRebalanceMS, int minActiveReplica, boolean sslToStorageNodes) {
    return getService(VeniceClusterWrapper.SERVICE_NAME,
        VeniceClusterWrapper.generateService(numberOfControllers, numberOfServers, numberOfRouter, replicaFactor,
            partitionSize, enableWhitelist, enableAutoJoinWhitelist, delayToRebalanceMS, minActiveReplica, sslToStorageNodes));
  }

  private static <S extends ProcessWrapper> S getStatefulService(String serviceName, StatefulServiceProvider<S> serviceProvider) {
    return getService(serviceName, serviceProvider);
  }

  private static <S> S getService(String serviceName, ArbitraryServiceProvider<S> serviceProvider) {
    // Just some initial state. If the fabric of space-time holds up, you should never see these strings.
    Exception lastException = new VeniceException("There is no spoon.");
    String errorMessage = "If you see this message, something went horribly wrong.";

    for (int attempt = 1; attempt <= MAX_ATTEMPT; attempt++) {
      try {
        S wrapper = serviceProvider.get(serviceName, IntegrationTestUtils.getFreePort());

        if (wrapper instanceof ProcessWrapper) {
          // N.B.: The contract for start() is that it should block until the wrapped service is fully started.
          ((ProcessWrapper) wrapper).start();
        }
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
