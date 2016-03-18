package com.linkedin.venice.integration.utils;

import com.linkedin.venice.exceptions.VeniceException;
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
  public static VeniceControllerWrapper getVeniceController() {
    /**
     * Get the Kafka dependency outside of the lambda, to avoid time complexity of
     * O({@value MAX_ATTEMPT} ^2) on the amount of retries. {@link #getKafkaBroker()}
     * has its own retries, so we can assume it's reliable enough.
     */

    return getVeniceController(TestUtils.getUniqueString("venice-cluster"), getKafkaBroker());
  }

  static VeniceControllerWrapper getVeniceController(String clusterName, KafkaBrokerWrapper kafkaBrokerWrapper) {
    return getStatefulService(
        VeniceControllerWrapper.SERVICE_NAME,
        VeniceControllerWrapper.generateService(clusterName, kafkaBrokerWrapper));
  }

  public static VeniceServerWrapper getVeniceServer() {
    /**
     * Get the Kafka dependency outside of the lambda, to avoid time complexity of
     * O({@value MAX_ATTEMPT} ^2) on the amount of retries. {@link #getKafkaBroker()}
     * has its own retries, so we can assume it's reliable enough.
     */

    return getVeniceServer(TestUtils.getUniqueString("venice-cluster"), getKafkaBroker());
  }

  static VeniceServerWrapper getVeniceServer(String clusterName, KafkaBrokerWrapper kafkaBrokerWrapper) {
    return getStatefulService(
        VeniceServerWrapper.SERVICE_NAME,
        VeniceServerWrapper.generateService(clusterName, kafkaBrokerWrapper));
  }

  public static VeniceClusterWrapper getVeniceCluster() {
    return getService(VeniceClusterWrapper.SERVICE_NAME, VeniceClusterWrapper.generateService());
  }

  public static InstanceWrapper getInstance() {
    return getService(InstanceWrapper.SERVICE_NAME, InstanceWrapper.generateService());
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
        S wrapper = serviceProvider.get(serviceName, TestUtils.getFreePort());
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
