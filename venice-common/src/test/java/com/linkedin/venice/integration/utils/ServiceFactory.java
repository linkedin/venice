package com.linkedin.venice.integration.utils;

import com.linkedin.d2.server.factory.D2Server;
import com.linkedin.venice.exceptions.VeniceException;
import java.util.List;

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
    return getStatefulService(
        VeniceControllerWrapper.SERVICE_NAME,
        VeniceControllerWrapper.generateService(clusterName, kafkaBrokerWrapper));
  }

  public static VeniceServerWrapper getVeniceServer(String clusterName, KafkaBrokerWrapper kafkaBrokerWrapper,
      boolean enableServerWhitelist, boolean autoJoinWhitelist) {
    return getStatefulService(VeniceServerWrapper.SERVICE_NAME,
        VeniceServerWrapper.generateService(clusterName, kafkaBrokerWrapper, enableServerWhitelist, autoJoinWhitelist));
  }

  /**
   * Note: Assumes that helix and kafka are using the same zookeeper, uses the zookeeper from the kafkaBrokerWrapper
   */
  static VeniceRouterWrapper getVeniceRouter(String clusterName, KafkaBrokerWrapper kafkaBrokerWrapper){
    return getService(VeniceRouterWrapper.SERVICE_NAME,
        VeniceRouterWrapper.generateService(clusterName, kafkaBrokerWrapper));
  }

  public static MockVeniceRouterWrapper getMockVeniceRouter(List<D2Server> d2ServerList){
    return getService(
        MockVeniceRouterWrapper.SERVICE_NAME,
        MockVeniceRouterWrapper.generateService(d2ServerList));
  }

  /**
   * Initialize MockHttpServerWrapper, this function will setup a simple http server if d2ServerList is null.
   * If param: d2ServerList is null, it will create a pure http server.
   *
   * @param serviceName
   * @param d2ServerList
   * @return
   */
  public static MockHttpServerWrapper getMockHttpServer(String serviceName, List<D2Server> d2ServerList) {
    return getService(serviceName, MockHttpServerWrapper.generateService(d2ServerList));
  }

  public static VeniceClusterWrapper getVeniceCluster() {
    return getVeniceCluster(1);
  }

  public static VeniceClusterWrapper getVeniceCluster(int numberOfServers) {
    // As we introduce bootstrap state in to venice and transition from bootstrap to online will be blocked until get
    // "end of push" message. We need more venice server for testing, because there is a limitation in helix about how
    // many uncompleted transitions one server could handle. So if we still use one server and that limitation is
    // reached, venice can not create new resource which will cause failed tests.
    return getService(VeniceClusterWrapper.SERVICE_NAME, VeniceClusterWrapper.generateService(numberOfServers));
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
        LOGGER.info(errorMessage);
      }
    }

    throw new VeniceException(errorMessage + " Aborting.", lastException);
  }
}
