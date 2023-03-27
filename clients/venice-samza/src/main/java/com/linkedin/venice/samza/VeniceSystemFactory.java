package com.linkedin.venice.samza;

import static com.linkedin.venice.CommonConfigKeys.SSL_KEYSTORE_LOCATION;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEYSTORE_TYPE;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEY_PASSWORD;
import static com.linkedin.venice.CommonConfigKeys.SSL_TRUSTSTORE_LOCATION;
import static com.linkedin.venice.CommonConfigKeys.SSL_TRUSTSTORE_PASSWORD;
import static com.linkedin.venice.ConfigKeys.VALIDATE_VENICE_INTERNAL_SCHEMA_VERSION;

import com.linkedin.venice.meta.Version;
import com.linkedin.venice.producer.NearlineProducerFactory;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
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

public class VeniceSystemFactory extends NearlineProducerFactory implements SystemFactory, Serializable {
  private static final long serialVersionUID = 1L;
  private static final Logger LOGGER = LogManager.getLogger(VeniceSystemFactory.class);

  public static final String SYSTEMS_PREFIX = "systems.";
  public static final String DOT = ".";
  public static final String DEPLOYMENT_ID = "deployment.id";

  /**
   * All the required configs to build a SSL Factory
   */
  private static final List<String> SSL_MANDATORY_CONFIGS = Arrays.asList(
      SSL_KEYSTORE_TYPE,
      SSL_KEYSTORE_LOCATION,
      SSL_KEY_PASSWORD,
      SSL_TRUSTSTORE_LOCATION,
      SSL_TRUSTSTORE_PASSWORD);

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
    String samzaJobId = config.get(DEPLOYMENT_ID);
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

    props.put(JOB_ID, samzaJobId);
    props.remove(DEPLOYMENT_ID);

    VeniceProperties veniceProperties = new VeniceProperties(props);

    return new VeniceSystemProducer(super.getProducer(storeName, veniceAggregate, pushTypeString, veniceProperties));
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
}
