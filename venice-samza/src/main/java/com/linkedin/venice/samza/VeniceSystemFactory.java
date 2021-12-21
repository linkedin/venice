package com.linkedin.venice.samza;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.log4j.Logger;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.util.SinglePartitionWithoutOffsetsSystemAdmin;

import static com.linkedin.venice.CommonConfigKeys.*;
import static com.linkedin.venice.VeniceConstants.*;


public class VeniceSystemFactory implements SystemFactory, Serializable {
  private static final Logger LOGGER = Logger.getLogger(VeniceSystemFactory.class);

  public static final String D2_ZK_HOSTS_PROPERTY = "__r2d2DefaultClient__.r2d2Client.zkHosts";

  public static final String SYSTEMS_PREFIX = "systems.";
  public static final String DOT = ".";
  public static final String DEPLOYMENT_ID = "deployment.id";

  public static final String VENICE_PUSH_TYPE = "push.type";

  /**
   * Venice store name Samza application is going to produce to.
   */
  public static final String VENICE_STORE = "store";

  /**
   * Whether to leverage Venice aggregation.
   * By default, it is 'false'.
   *
   * When the Samza application decides to leverage Venice aggregation, all the messages
   * will be produced to Venice Parent cluster, otherwise, all the messages will be produced
   * to local cluster.
   */
  public static final String VENICE_AGGREGATE = "aggregate";

  /**
   * D2 ZK hosts for Venice Parent Cluster.
   */
  public static final String VENICE_PARENT_D2_ZK_HOSTS = "venice.parent.d2.zk.hosts";

  /**
   * Specifies a list of partitioners venice supported.
   * It contains a string of concatenated partitioner class names separated by comma.
   */
  public static final String VENICE_PARTITIONERS = "venice.partitioners";

  // D2 service name for local cluster
  public static final String VENICE_LOCAL_D2_SERVICE = "VeniceController";
  // D2 service name for parent cluster
  public static final String VENICE_PARENT_D2_SERVICE = "VeniceParentController";

  /**
   * A Samza job config to check whether the protocol versions used at runtime are valid in
   * Venice backend; if not, fail fast. Default value should be true.
   *
   * Turn off the config in DEV configs so that PCL will not try to access routers.
   */
  public static final String VALIDATE_VENICE_INTERNAL_SCHEMA_VERSION = "validate.venice.internal.schema.version";

  /**
   * A global static counter to track how many factory one process would create.
   * In general, one factory is enough for one application; otherwise, if there are
   * multiple factory built in the same process, log this information for debugging purpose.
   */
  private static final AtomicInteger FACTORY_INSTANCE_NUMBER = new AtomicInteger(0);

  /**
   * Key: VeniceSystemProducer instance;
   * Value: a pair of boolean: <isActive, isStreamReprocessingJobSucceeded>
   *
   * For each SystemProducer created through this factory, keep track of its status in
   * the below Map. {@link com.linkedin.venice.pushmonitor.RouterBasedPushMonitor} will update
   * the status of the SystemProducer.
   */
  private final Map<SystemProducer, Pair<Boolean, Boolean>> systemProducerStatues;

  /**
   * All the required configs to build a SSL Factory
   */
  private static final List<String> SSL_MANDATORY_CONFIGS = Arrays.asList(
      SSL_KEYSTORE_TYPE,
      SSL_KEYSTORE_LOCATION,
      SSL_KEY_PASSWORD,
      SSL_TRUSTSTORE_LOCATION,
      SSL_TRUSTSTORE_PASSWORD
  );

  public VeniceSystemFactory() {
    systemProducerStatues = new VeniceConcurrentHashMap<>();
    int totalNumberOfFactory = FACTORY_INSTANCE_NUMBER.incrementAndGet();
    if (totalNumberOfFactory > 1) {
      LOGGER.warn("There are " + totalNumberOfFactory + " VeniceSystemProducer factory instances in one process.");
    }
  }

  @Override
  public SystemAdmin getAdmin(String systemName, Config config) {
    return new SinglePartitionWithoutOffsetsSystemAdmin();
  }

  @Override
  public SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry registry) {
    throw new SamzaException("There is no Venice Consumer");
  }

  /**
   * Keep the original createSystemProducer method to keep backward compatibility.
   * TODO: Remove this method after samza-li-venice 0.7.1 or higher is released as active version.
   */
  protected SystemProducer createSystemProducer(String veniceD2ZKHost, String veniceD2Service, String storeName,
      Version.PushType venicePushType, String samzaJobId, String runningFabric, Config config,
      Optional<SSLFactory> sslFactory, Optional<String> partitioners) {
    return new VeniceSystemProducer(veniceD2ZKHost, veniceD2Service, storeName, venicePushType, samzaJobId, runningFabric,
        config.getBoolean(VALIDATE_VENICE_INTERNAL_SCHEMA_VERSION, false), this, sslFactory, partitioners);
  }

  // Extra `Config` parameter is to ease the internal implementation
  protected SystemProducer createSystemProducer(String veniceD2ZKHost, String veniceD2Service, String storeName,
      Version.PushType venicePushType, String samzaJobId, String runningFabric, boolean verifyLatestProtocolPresent, Config config,
      Optional<SSLFactory> sslFactory, Optional<String> partitioners) {
    return new VeniceSystemProducer(veniceD2ZKHost, veniceD2Service, storeName, venicePushType, samzaJobId, runningFabric,
        verifyLatestProtocolPresent, this, sslFactory, partitioners);
  }

  /**
   * Samza table writer would directly call this function to create venice system producer instead of calling the general
   * {@link VeniceSystemFactory#getProducer(String, Config, MetricsRegistry)} function.
   */
  public SystemProducer getProducer(String systemName, String storeName, boolean veniceAggregate,
      String pushTypeString, Config config) {
    if (isEmpty(storeName)) {
      throw new SamzaException(VENICE_STORE + " should not be null for system " + systemName);
    }

    String samzaJobId = config.get(DEPLOYMENT_ID);
    String prefix = SYSTEMS_PREFIX + systemName + DOT;
    Version.PushType venicePushType;
    try {
      venicePushType = Version.PushType.valueOf(pushTypeString);
    } catch (Exception e) {
      throw new SamzaException("Cannot parse venice push type: " + pushTypeString
          + ".  Must be one of: " + Arrays.stream(Version.PushType.values())
          .map(Enum::toString)
          .collect(Collectors.joining(",")));
    }

    String veniceParentZKHosts = config.get(VENICE_PARENT_D2_ZK_HOSTS);
    if (isEmpty(veniceParentZKHosts)) {
      throw new SamzaException(VENICE_PARENT_D2_ZK_HOSTS + " should not be null, please put this property in your app-def.xml");
    }
    String localVeniceZKHosts = config.get(D2_ZK_HOSTS_PROPERTY);
    if (isEmpty(localVeniceZKHosts)) {
      throw new SamzaException(D2_ZK_HOSTS_PROPERTY + " should not be null");
    }

    // Build Ssl Factory if Controller SSL is enabled
    Optional<SSLFactory> sslFactory = Optional.empty();
    boolean controllerSslEnabled = config.getBoolean(SSL_ENABLED, true);
    if (controllerSslEnabled) {
      LOGGER.info("Controller ACL is enabled.");
      String sslFactoryClassName = config.get(SSL_FACTORY_CLASS_NAME, DEFAULT_SSL_FACTORY_CLASS_NAME);
      Properties sslProps = getSslProperties(config);
      sslFactory = Optional.of(SslUtils.getSSLFactory(sslProps, sslFactoryClassName));
    }

    Optional<String> partitioners = Optional.ofNullable(config.get(VENICE_PARTITIONERS));

    LOGGER.info("Configs for " + systemName + " producer: ");
    LOGGER.info(prefix + VENICE_STORE + ": " + storeName);
    LOGGER.info(prefix + VENICE_AGGREGATE + ": " + veniceAggregate);
    LOGGER.info(prefix + VENICE_PUSH_TYPE + ": " + venicePushType);
    LOGGER.info(VENICE_PARENT_D2_ZK_HOSTS + ": " + veniceParentZKHosts);
    LOGGER.info(D2_ZK_HOSTS_PROPERTY + ": " + localVeniceZKHosts);

    String runningFabric = config.get(SYSTEM_PROPERTY_FOR_APP_RUNNING_REGION);
    LOGGER.info("Running Fabric from config: " + runningFabric);
    if (runningFabric == null) {
      runningFabric = System.getProperty(SYSTEM_PROPERTY_FOR_APP_RUNNING_REGION);
      LOGGER.info("Running Fabric from environment: " + runningFabric);
      if (runningFabric != null) {
        runningFabric = runningFabric.toLowerCase();
      }
    }
    if (runningFabric != null && runningFabric.contains("corp")) {
      runningFabric = NATIVE_REPLICATION_DEFAULT_SOURCE_FABRIC;
    }
    LOGGER.info("Final Running Fabric: " + runningFabric);

    String veniceD2ZKHost;
    String veniceD2Service;
    if (veniceAggregate) {
      veniceD2ZKHost = veniceParentZKHosts;
      veniceD2Service = VENICE_PARENT_D2_SERVICE;
    } else {
      veniceD2ZKHost = localVeniceZKHosts;
      veniceD2Service = VENICE_LOCAL_D2_SERVICE;
    }
    LOGGER.info("Will use the following Venice D2 ZK hosts: " + veniceD2ZKHost);

    boolean verifyLatestProtocolPresent = config.getBoolean(VALIDATE_VENICE_INTERNAL_SCHEMA_VERSION, false);
    SystemProducer systemProducer = createSystemProducer(veniceD2ZKHost, veniceD2Service, storeName, venicePushType,
        samzaJobId, runningFabric, verifyLatestProtocolPresent, config, sslFactory, partitioners);
    this.systemProducerStatues.computeIfAbsent(systemProducer, k -> Pair.create(true, false));
    return systemProducer;
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
   * Get the total number of active SystemProducer.
   *
   * The SystemProducer for push type: STREAM and BATCH will always be at active state; so if there is any
   * real-time SystemProducer in the Samza task, the task will not be stopped even though all the stream reprocessing
   * jobs have completed. Besides, a Samza task can not have a mix of BATCH push type and STREAM_REPROCESSING push type;
   * otherwise, the Samza task can not be automatically stopped.
   */
  public int getNumberOfActiveSystemProducers() {
    int count = 0;
    for (Map.Entry<SystemProducer, Pair<Boolean, Boolean>> entry: systemProducerStatues.entrySet()) {
      boolean isActive = entry.getValue().getFirst();
      count += isActive ? 1 : 0;
    }
    return count;
  }

  /**
   * Check whether all the stream reprocessing jobs have succeeded; return false if any of them fail.
   */
  public boolean getOverallExecutionStatus() {
    for (Map.Entry<SystemProducer, Pair<Boolean, Boolean>> entry: systemProducerStatues.entrySet()) {
      boolean jobSucceed = entry.getValue().getSecond();
      if (!jobSucceed) {
        return false;
      }
    }
    return true;
  }

  /**
   * {@link com.linkedin.venice.pushmonitor.RouterBasedPushMonitor} will update the status of a SystemProducer with
   * push type STREAM_REPROCESSING:
   * END_OF_PUSH_RECEIVED: isActive -> false; isStreamReprocessingJobSucceeded -> true
   * COMPLETED: isActive -> false; isStreamReprocessingJobSucceeded -> true
   * ERROR: isActive -> false; isStreamReprocessingJobSucceeded -> false
   *
   * For all the other push job status, SystemProducer status will not be updated.
   */
  public void endStreamReprocessingSystemProducer(SystemProducer systemProducer, boolean jobSucceed) {
    systemProducerStatues.put(systemProducer, Pair.create(false, jobSucceed));
  }

  /**
   * Build SSL properties based on the Samza job config
   */
  private Properties getSslProperties(Config samzaConfig) {
    // Make sure all mandatory configs exist
    SSL_MANDATORY_CONFIGS.forEach(requiredConfig -> {
      if (!samzaConfig.containsKey(requiredConfig)) {
        throw new VeniceException("Missing a mandatory SSL config: " + requiredConfig);
      }
    });

    Properties sslProperties = new Properties();
    sslProperties.setProperty(SSL_ENABLED, "true");
    sslProperties.setProperty(SSL_KEYSTORE_TYPE, samzaConfig.get(SSL_KEYSTORE_TYPE));
    sslProperties.setProperty(SSL_KEYSTORE_LOCATION, samzaConfig.get(SSL_KEYSTORE_LOCATION));
    sslProperties.setProperty(SSL_KEYSTORE_PASSWORD, samzaConfig.get(SSL_KEY_PASSWORD));
    sslProperties.setProperty(SSL_TRUSTSTORE_LOCATION, samzaConfig.get(SSL_TRUSTSTORE_LOCATION));
    sslProperties.setProperty(SSL_TRUSTSTORE_PASSWORD, samzaConfig.get(SSL_TRUSTSTORE_PASSWORD));
    return sslProperties;
  }

  private static boolean isEmpty(String input) {
    return (null == input) || input.isEmpty() || input.equals("null");
  }
}
