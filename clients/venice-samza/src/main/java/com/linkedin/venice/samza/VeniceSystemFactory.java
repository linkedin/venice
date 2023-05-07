package com.linkedin.venice.samza;

import static com.linkedin.venice.ConfigKeys.LOCAL_REGION_NAME;
import static com.linkedin.venice.ConfigKeys.VALIDATE_VENICE_INTERNAL_SCHEMA_VERSION;
import static com.linkedin.venice.VeniceConstants.NATIVE_REPLICATION_DEFAULT_SOURCE_FABRIC;
import static com.linkedin.venice.VeniceConstants.SYSTEM_PROPERTY_FOR_APP_RUNNING_REGION;
import static com.linkedin.venice.producer.NearlineProducerFactory.JOB_ID;
import static com.linkedin.venice.producer.NearlineProducerFactory.VENICE_AGGREGATE;
import static com.linkedin.venice.producer.NearlineProducerFactory.VENICE_CHILD_CONTROLLER_D2_SERVICE;
import static com.linkedin.venice.producer.NearlineProducerFactory.VENICE_CHILD_D2_ZK_HOSTS;
import static com.linkedin.venice.producer.NearlineProducerFactory.VENICE_PARENT_CONTROLLER_D2_SERVICE;
import static com.linkedin.venice.producer.NearlineProducerFactory.VENICE_PUSH_TYPE;
import static com.linkedin.venice.producer.NearlineProducerFactory.VENICE_STORE;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.producer.NearlineProducerFactory;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.Serializable;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.util.SinglePartitionWithoutOffsetsSystemAdmin;


/**
 * Samza jobs talk to either parent or child controller depending on the aggregate mode config.
 * The decision of which controller should be used is made in {@link VeniceSystemFactory}.
 * The "Primary Controller" term is used to refer to whichever controller the Samza job should talk to.
 *
 * The primary controller should be:
 * 1. The parent controller when the Venice system is deployed in a multi-colo mode and either:
 *     a. {@link PushType} is {@link PushType.BATCH} or {@link PushType.STREAM_REPROCESSING}; or
 *     b. @deprecated {@link PushType} is {@link PushType.STREAM} and the job is configured to write data in AGGREGATE mode
 * 2. The child controller when either:
 *     a. The Venice system is deployed in a single-colo mode; or
 *     b. The {@link PushType} is {@link PushType.STREAM} and the job is configured to write data in NON_AGGREGATE mode
 */

public class VeniceSystemFactory implements SystemFactory, Serializable {
  private static final long serialVersionUID = 1L;
  private static final Logger LOGGER = LogManager.getLogger(VeniceSystemFactory.class);

  public static final String SYSTEMS_PREFIX = "systems.";
  public static final String DOT = ".";
  public static final String LEGACY_CHILD_D2_ZK_HOSTS_PROPERTY = "__r2d2DefaultClient__.r2d2Client.zkHosts";
  public static final String DEPLOYMENT_ID = "deployment.id";
  // Legacy D2 service name for local cluster
  public static final String LEGACY_VENICE_CHILD_CONTROLLER_D2_SERVICE = "VeniceController";
  // Legacy D2 service name for parent cluster
  public static final String LEGACY_VENICE_PARENT_CONTROLLER_D2_SERVICE = "VeniceParentController";

  @Override
  public SystemAdmin getAdmin(String systemName, Config config) {
    return new SinglePartitionWithoutOffsetsSystemAdmin();
  }

  @Override
  public SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry registry) {
    throw new SamzaException("There is no Venice Consumer");
  }

  /**
   * @deprecated Left in to maintain backward compatibility
   */
  @Deprecated
  protected SystemProducer createSystemProducer(
      String primaryControllerColoD2ZKHost,
      String primaryControllerD2Service,
      String storeName,
      Version.PushType venicePushType,
      String samzaJobId,
      String runningFabric,
      Config config,
      Optional<SSLFactory> sslFactory,
      Optional<String> partitioners) {
    return new VeniceSystemProducer(
        primaryControllerColoD2ZKHost,
        primaryControllerColoD2ZKHost,
        primaryControllerD2Service,
        storeName,
        venicePushType,
        samzaJobId,
        runningFabric,
        config.getBoolean(VALIDATE_VENICE_INTERNAL_SCHEMA_VERSION, true),
        this,
        sslFactory,
        partitioners);
  }

  /**
   * @deprecated Left in to maintain backward compatibility
    */
  @Deprecated
  protected SystemProducer createSystemProducer(
      String primaryControllerColoD2ZKHost,
      String primaryControllerD2Service,
      String storeName,
      Version.PushType venicePushType,
      String samzaJobId,
      String runningFabric,
      boolean verifyLatestProtocolPresent,
      Config config,
      Optional<SSLFactory> sslFactory,
      Optional<String> partitioners) {
    return createSystemProducer(
        primaryControllerColoD2ZKHost,
        primaryControllerColoD2ZKHost,
        primaryControllerD2Service,
        storeName,
        venicePushType,
        samzaJobId,
        runningFabric,
        verifyLatestProtocolPresent,
        config,
        sslFactory,
        partitioners);
  }

  /**
   * Construct a new instance of {@link VeniceSystemProducer}
   * @param veniceChildD2ZkHost D2 Zk Address where the components in the child colo are announcing themselves
   * @param primaryControllerColoD2ZKHost D2 Zk Address of the colo where the primary controller resides
   * @param primaryControllerD2ServiceName The service name that the primary controller uses to announce itself to D2
   * @param storeName The store to write to
   * @param pushType The {@link PushType} to use to write to the store
   * @param samzaJobId A unique id used to identify jobs that can concurrently write to the same store
   * @param runningFabric The colo where the job is running. It is used to find the best destination for the data to be written to
   * @param verifyLatestProtocolPresent Config to check whether the protocol versions used at runtime are valid in Venice backend
   * @param factory The {@link VeniceSystemFactory} object that was used to create this object
   * @param config A Config object that may be used by the factory implementation to create an overridden SystemProducer instance
   * @param sslFactory An optional {@link SSLFactory} that is used to communicate with other components using SSL
   * @param partitioners A list of comma-separated partitioners class names that are supported.
   */
  @SuppressWarnings("unused")
  protected SystemProducer createSystemProducer(
      String veniceChildD2ZkHost,
      String primaryControllerColoD2ZKHost,
      String primaryControllerD2Service,
      String storeName,
      Version.PushType venicePushType,
      String samzaJobId,
      String runningFabric,
      boolean verifyLatestProtocolPresent,
      Config config,
      Optional<SSLFactory> sslFactory,
      Optional<String> partitioners) {
    return createSystemProducer(
        veniceChildD2ZkHost,
        primaryControllerColoD2ZKHost,
        primaryControllerD2Service,
        storeName,
        venicePushType,
        samzaJobId,
        runningFabric,
        verifyLatestProtocolPresent,
        sslFactory,
        partitioners);
  }

  /**
   * Construct a new instance of {@link VeniceSystemProducer}
   * @param veniceChildD2ZkHost D2 Zk Address where the components in the child colo are announcing themselves
   * @param primaryControllerColoD2ZKHost D2 Zk Address of the colo where the primary controller resides
   * @param primaryControllerD2ServiceName The service name that the primary controller uses to announce itself to D2
   * @param storeName The store to write to
   * @param pushType The {@link PushType} to use to write to the store
   * @param samzaJobId A unique id used to identify jobs that can concurrently write to the same store
   * @param runningFabric The colo where the job is running. It is used to find the best destination for the data to be written to
   * @param verifyLatestProtocolPresent Config to check whether the protocol versions used at runtime are valid in Venice backend
   * @param factory The {@link VeniceSystemFactory} object that was used to create this object
   * @param sslFactory An optional {@link SSLFactory} that is used to communicate with other components using SSL
   * @param partitioners A list of comma-separated partitioners class names that are supported.
   */
  protected SystemProducer createSystemProducer(
      String veniceChildD2ZkHost,
      String primaryControllerColoD2ZKHost,
      String primaryControllerD2Service,
      String storeName,
      Version.PushType venicePushType,
      String samzaJobId,
      String runningFabric,
      boolean verifyLatestProtocolPresent,
      Optional<SSLFactory> sslFactory,
      Optional<String> partitioners) {
    return new VeniceSystemProducer(
        veniceChildD2ZkHost,
        primaryControllerColoD2ZKHost,
        primaryControllerD2Service,
        storeName,
        venicePushType,
        samzaJobId,
        runningFabric,
        verifyLatestProtocolPresent,
        this,
        sslFactory,
        partitioners);
  }

  /**
   * Samza table writer would directly call this function to create venice system producer instead of calling the general
   * {@link VeniceSystemFactory#getProducer(String, Config, MetricsRegistry)} function.
   */
  public SystemProducer getProducer(
      String systemName,
      String storeName,
      boolean veniceAggregate,
      String pushTypeString,
      Config config) {
    String prefix = SYSTEMS_PREFIX + systemName + DOT;

    Properties props = new Properties();
    props.putAll(config);

    for (Map.Entry<String, String> entry: config.entrySet()) {
      String key = entry.getKey();
      if (key.startsWith(prefix)) {
        String extractedKey = key.substring(prefix.length());
        props.put(extractedKey, entry.getValue());
      } else {
        props.put(key, entry.getValue());
      }
    }

    String samzaJobId = config.get(DEPLOYMENT_ID);
    props.put(JOB_ID, samzaJobId);
    props.remove(DEPLOYMENT_ID);

    String localVeniceZKHosts = config.get(VENICE_CHILD_D2_ZK_HOSTS);
    String legacyLocalVeniceZKHosts = config.get(LEGACY_CHILD_D2_ZK_HOSTS_PROPERTY);
    if (isEmpty(localVeniceZKHosts)) {
      if (isEmpty(legacyLocalVeniceZKHosts)) {
        throw new VeniceException(
            "Either " + VENICE_CHILD_D2_ZK_HOSTS + " or " + LEGACY_CHILD_D2_ZK_HOSTS_PROPERTY + " should be defined");
      }
      localVeniceZKHosts = legacyLocalVeniceZKHosts;
    }

    props.put(VENICE_CHILD_D2_ZK_HOSTS, localVeniceZKHosts);
    props.remove(LEGACY_CHILD_D2_ZK_HOSTS_PROPERTY);

    String localControllerD2Service = config.get(VENICE_CHILD_CONTROLLER_D2_SERVICE);
    if (isEmpty(localControllerD2Service)) {
      LOGGER.info(
          VENICE_CHILD_CONTROLLER_D2_SERVICE + " is not defined. Using " + LEGACY_VENICE_CHILD_CONTROLLER_D2_SERVICE);
      localControllerD2Service = LEGACY_VENICE_CHILD_CONTROLLER_D2_SERVICE;
    }
    props.put(VENICE_CHILD_CONTROLLER_D2_SERVICE, localControllerD2Service);

    String parentControllerD2Service = config.get(VENICE_PARENT_CONTROLLER_D2_SERVICE);
    if (isEmpty(parentControllerD2Service)) {
      LOGGER.info(
          VENICE_PARENT_CONTROLLER_D2_SERVICE + " is not defined. Using " + LEGACY_VENICE_PARENT_CONTROLLER_D2_SERVICE);
      parentControllerD2Service = LEGACY_VENICE_PARENT_CONTROLLER_D2_SERVICE;
    }
    props.put(VENICE_PARENT_CONTROLLER_D2_SERVICE, parentControllerD2Service);

    String runningFabric = config.get(SYSTEM_PROPERTY_FOR_APP_RUNNING_REGION);
    LOGGER.info("Running Fabric from props: {}", runningFabric);
    if (runningFabric == null) {
      runningFabric = System.getProperty(SYSTEM_PROPERTY_FOR_APP_RUNNING_REGION);
      LOGGER.info("Running Fabric from environment: {}", runningFabric);
      if (runningFabric != null) {
        runningFabric = runningFabric.toLowerCase();
      }
    }
    if (runningFabric != null && runningFabric.contains("corp")) {
      runningFabric = NATIVE_REPLICATION_DEFAULT_SOURCE_FABRIC;
    }
    if (runningFabric != null) {
      props.put(LOCAL_REGION_NAME, runningFabric);
    }

    VeniceProperties veniceProperties = new VeniceProperties(props);

    return new VeniceSystemProducer(
        storeName,
        this,
        NearlineProducerFactory.getInstance()
            .getProducer(storeName, veniceAggregate, pushTypeString, veniceProperties));
  }

  /**
   * The core function of a {@link SystemFactory}; most Samza users would specify VeniceSystemFactory in the job
   * config and Samza would invoke {@link SystemFactory#getProducer(String, Config, MetricsRegistry)} to create producers.
   */
  @Override
  public SystemProducer getProducer(String systemName, Config config, MetricsRegistry registry) {
    final String prefix = SYSTEMS_PREFIX + systemName + DOT;
    final String storeName = config.get(prefix + VENICE_STORE);
    final boolean veniceAggregate = config.getBoolean(prefix + VENICE_AGGREGATE, false);
    final String pushTypeString = config.get(prefix + VENICE_PUSH_TYPE);
    return getProducer(systemName, storeName, veniceAggregate, pushTypeString, config);
  }

  /**
   * Convenience method to hide the ugliness of casting in just one place.
   *
   * Ideally, we would change the return type of {@link #getProducer(String, Config, MetricsRegistry)} to
   * {@link VeniceSystemProducer} but since there are existing users of this API, we are being extra careful
   * not to disturb it.
   *
   * TODO: clean this up when we have the bandwidth to coordinate the refactoring with the existing users.
   */
  public VeniceSystemProducer getClosableProducer(String systemName, Config config, MetricsRegistry registry) {
    return (VeniceSystemProducer) getProducer(systemName, config, registry);
  }

  private static boolean isEmpty(String input) {
    return (input == null) || input.isEmpty() || input.equals("null");
  }
}
