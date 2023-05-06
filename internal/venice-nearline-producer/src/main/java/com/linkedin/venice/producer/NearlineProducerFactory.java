package com.linkedin.venice.producer;

import static com.linkedin.venice.CommonConfigKeys.SSL_ENABLED;
import static com.linkedin.venice.CommonConfigKeys.SSL_FACTORY_CLASS_NAME;
import static com.linkedin.venice.ConfigKeys.LOCAL_REGION_NAME;
import static com.linkedin.venice.ConfigKeys.VALIDATE_VENICE_INTERNAL_SCHEMA_VERSION;
import static com.linkedin.venice.ConfigKeys.VENICE_PARTITIONERS;
import static com.linkedin.venice.VeniceConstants.DEFAULT_SSL_FACTORY_CLASS_NAME;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Nearline writers talk to either parent or child controller depending on the aggregate mode config.
 * The decision of which controller should be used is made in {@link NearlineProducerFactory}.
 * The "Primary Controller" term is used to refer to whichever controller the nearline job should talk to.
 *
 * The primary controller should be:
 * 1. The parent controller when the Venice system is deployed in a multi-colo mode and either:
 *     a. {@link PushType} is {@link PushType.BATCH} or {@link PushType.STREAM_REPROCESSING}; or
 *     b. @deprecated {@link PushType} is {@link PushType.STREAM} and the job is configured to write data in AGGREGATE mode
 * 2. The child controller when either:
 *     a. The Venice system is deployed in a single-colo mode; or
 *     b. The {@link PushType} is {@link PushType.STREAM} and the job is configured to write data in NON_AGGREGATE mode
 */

public class NearlineProducerFactory {
  private static final Logger LOGGER = LogManager.getLogger(NearlineProducerFactory.class);
  public static final String JOB_ID = "job.id";
  public static final String VENICE_PUSH_TYPE = "push.type";

  private static final NearlineProducerFactory INSTANCE = new NearlineProducerFactory();

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
   * D2 ZK hosts for Venice Child Cluster.
   */
  public static final String VENICE_CHILD_D2_ZK_HOSTS = "venice.child.d2.zk.hosts";

  /**
   * D2 ZK hosts for Venice Parent Cluster.
   */
  public static final String VENICE_PARENT_D2_ZK_HOSTS = "venice.parent.d2.zk.hosts";

  // D2 service name for local cluster
  public static final String VENICE_CHILD_CONTROLLER_D2_SERVICE = "venice.child.controller.d2.service";
  // D2 service name for parent cluster
  public static final String VENICE_PARENT_CONTROLLER_D2_SERVICE = "venice.parent.controller.d2.service";

  /**
   * Key: NearlineProducer instance;
   * Value: a pair of boolean: <isActive, isStreamReprocessingJobSucceeded>
   *
   * For each NearlineProducer created through this factory, keep track of its status in
   * the below Map. {@link com.linkedin.venice.pushmonitor.RouterBasedPushMonitor} will update
   * the status of the NearlineProducer.
   */
  private final Map<NearlineProducer, JobState> nearlineProducerStates = new VeniceConcurrentHashMap<>();

  /**
   * Construct a new instance of {@link NearlineProducer}
   * @param veniceChildD2ZkHost D2 Zk Address where the components in the child colo are announcing themselves
   * @param primaryControllerColoD2ZKHost D2 Zk Address of the colo where the primary controller resides
   * @param primaryControllerD2ServiceName The service name that the primary controller uses to announce itself to D2
   * @param storeName The store to write to
   * @param pushType The {@link PushType} to use to write to the store
   * @param jobId A unique id used to identify jobs that can concurrently write to the same store
   * @param runningRegion The colo where the job is running. It is used to find the best destination for the data to be written to
   * @param verifyLatestProtocolPresent Config to check whether the protocol versions used at runtime are valid in Venice backend
   * @param factory The {@link NearlineProducerFactory} object that was used to create this object
   * @param config A Config object that may be used by the factory implementation to create an overridden NearlineProducer instance
   * @param sslFactory An optional {@link SSLFactory} that is used to communicate with other components using SSL
   * @param partitioners A list of comma-separated partitioners class names that are supported.
   */
  @SuppressWarnings("unused")
  protected NearlineProducer createNearlineProducer(
      String veniceChildD2ZkHost,
      String primaryControllerColoD2ZKHost,
      String primaryControllerD2Service,
      String storeName,
      Version.PushType venicePushType,
      String jobId,
      String runningRegion,
      boolean verifyLatestProtocolPresent,
      VeniceProperties properties,
      SSLFactory sslFactory,
      String partitioners) {
    return createNearlineProducer(
        veniceChildD2ZkHost,
        primaryControllerColoD2ZKHost,
        primaryControllerD2Service,
        storeName,
        venicePushType,
        jobId,
        runningRegion,
        verifyLatestProtocolPresent,
        sslFactory,
        partitioners);
  }

  /**
   * Construct a new instance of {@link NearlineProducer}
   * @param veniceChildD2ZkHost D2 Zk Address where the components in the child colo are announcing themselves
   * @param primaryControllerColoD2ZKHost D2 Zk Address of the colo where the primary controller resides
   * @param primaryControllerD2ServiceName The service name that the primary controller uses to announce itself to D2
   * @param storeName The store to write to
   * @param pushType The {@link PushType} to use to write to the store
   * @param jobId A unique id used to identify jobs that can concurrently write to the same store
   * @param runningRegion The colo where the job is running. It is used to find the best destination for the data to be written to
   * @param verifyLatestProtocolPresent Config to check whether the protocol versions used at runtime are valid in Venice backend
   * @param factory The {@link NearlineProducerFactory} object that was used to create this object
   * @param sslFactory An optional {@link SSLFactory} that is used to communicate with other components using SSL
   * @param partitioners A list of comma-separated partitioners class names that are supported.
   */
  protected NearlineProducer createNearlineProducer(
      String veniceChildD2ZkHost,
      String primaryControllerColoD2ZKHost,
      String primaryControllerD2Service,
      String storeName,
      Version.PushType venicePushType,
      String jobId,
      String runningRegion,
      boolean verifyLatestProtocolPresent,
      SSLFactory sslFactory,
      String partitioners) {
    return new NearlineProducer(
        veniceChildD2ZkHost,
        primaryControllerColoD2ZKHost,
        primaryControllerD2Service,
        storeName,
        venicePushType,
        jobId,
        runningRegion,
        verifyLatestProtocolPresent,
        sslFactory,
        partitioners);
  }

  /**
   * Samza table writer would directly call this function to create venice system producer instead of calling the general
   * {@link NearlineProducerFactory#getProducer(String, Config, MetricsRegistry)} function.
   */
  public NearlineProducer getProducer(
      String storeName,
      boolean veniceAggregate,
      String pushTypeString,
      VeniceProperties props) {
    if (isEmpty(storeName)) {
      throw new VeniceException(VENICE_STORE + " should not be null");
    }

    String jobId = props.getString(JOB_ID);
    Version.PushType venicePushType;
    try {
      venicePushType = Version.PushType.valueOf(pushTypeString);
    } catch (Exception e) {
      throw new VeniceException(
          "Cannot parse venice push type: " + pushTypeString + ".  Must be one of: "
              + Arrays.stream(Version.PushType.values()).map(Enum::toString).collect(Collectors.joining(",")));
    }

    String veniceParentZKHosts = props.getString(VENICE_PARENT_D2_ZK_HOSTS);
    String localVeniceZKHosts = props.getString(VENICE_CHILD_D2_ZK_HOSTS);
    String localControllerD2Service = props.getString(VENICE_CHILD_CONTROLLER_D2_SERVICE);
    String parentControllerD2Service = props.getString(VENICE_PARENT_CONTROLLER_D2_SERVICE);

    // Build Ssl Factory if Controller SSL is enabled
    SSLFactory sslFactory = null;
    boolean controllerSslEnabled = props.getBoolean(SSL_ENABLED, true);
    if (controllerSslEnabled) {
      LOGGER.info("Controller ACL is enabled.");
      String sslFactoryClassName = props.getString(SSL_FACTORY_CLASS_NAME, DEFAULT_SSL_FACTORY_CLASS_NAME);
      sslFactory = SslUtils.getSSLFactory(props.getPropertiesCopy(), sslFactoryClassName);
    }

    String partitioners = props.getString(VENICE_PARTITIONERS, (String) null);
    String runningRegion = props.getString(LOCAL_REGION_NAME, (String) null);

    LOGGER.info("Configs for producer: ");
    LOGGER.info("{}: {}", VENICE_STORE, storeName);
    LOGGER.info("{}: {}", VENICE_AGGREGATE, veniceAggregate);
    LOGGER.info("{}: {}", VENICE_PUSH_TYPE, venicePushType);
    LOGGER.info("{}: {}", VENICE_PARENT_D2_ZK_HOSTS, veniceParentZKHosts);
    LOGGER.info("{}: {}", VENICE_CHILD_D2_ZK_HOSTS, localVeniceZKHosts);
    LOGGER.info("{}: {}", VENICE_PARENT_CONTROLLER_D2_SERVICE, parentControllerD2Service);
    LOGGER.info("{}: {}", VENICE_CHILD_CONTROLLER_D2_SERVICE, localControllerD2Service);
    LOGGER.info("{}: {}", LOCAL_REGION_NAME, runningRegion);

    String primaryControllerColoD2ZKHost;
    String primaryControllerD2Service;
    if (veniceAggregate) {
      primaryControllerColoD2ZKHost = veniceParentZKHosts;
      primaryControllerD2Service = parentControllerD2Service;
    } else {
      primaryControllerColoD2ZKHost = localVeniceZKHosts;
      primaryControllerD2Service = localControllerD2Service;
    }
    LOGGER.info(
        "Will use the following primary controller D2 ZK hosts: {} and D2 Service: {}",
        primaryControllerColoD2ZKHost,
        primaryControllerD2Service);

    boolean verifyLatestProtocolPresent = props.getBoolean(VALIDATE_VENICE_INTERNAL_SCHEMA_VERSION, true);
    NearlineProducer systemProducer = createNearlineProducer(
        localVeniceZKHosts,
        primaryControllerColoD2ZKHost,
        primaryControllerD2Service,
        storeName,
        venicePushType,
        jobId,
        runningRegion,
        verifyLatestProtocolPresent,
        props, // Although we don't use this config in our default implementation, overridden implementations might
        // need this
        sslFactory,
        partitioners);
    this.nearlineProducerStates.computeIfAbsent(systemProducer, k -> new JobState(true, false));
    return systemProducer;
  }

  /**
   * The core function of a {@link NearlineProducerFactory}; most Nearline users would specify NearlineProducerFactory in the job
   * config and Samza would invoke {@link NearlineProducerFactory#getProducer(Config, MetricsRegistry)} to create producers.
   */
  public NearlineProducer getProducer(VeniceProperties props, MetricsRepository repository) {
    final String storeName = props.getString(VENICE_STORE);
    final boolean veniceAggregate = props.getBoolean(VENICE_AGGREGATE, false);
    final String pushTypeString = props.getString(VENICE_PUSH_TYPE);
    return getProducer(storeName, veniceAggregate, pushTypeString, props);
  }

  /**
   * Get the total number of active NearlineProducer.
   *
   * The NearlineProducer for push type: STREAM and BATCH will always be at active state; so if there is any
   * real-time NearlineProducer in the Samza task, the task will not be stopped even though all the stream reprocessing
   * jobs have completed. Besides, a Samza task can not have a mix of BATCH push type and STREAM_REPROCESSING push type;
   * otherwise, the Samza task can not be automatically stopped.
   */
  public int getNumberOfActiveNearlineProducers() {
    int count = 0;
    for (Map.Entry<NearlineProducer, JobState> entry: nearlineProducerStates.entrySet()) {
      boolean isActive = entry.getValue().isActive();
      count += isActive ? 1 : 0;
    }
    return count;
  }

  /**
   * Check whether all the stream reprocessing jobs have succeeded; return false if any of them fail.
   */
  public boolean getOverallExecutionStatus() {
    for (Map.Entry<NearlineProducer, JobState> entry: nearlineProducerStates.entrySet()) {
      boolean jobSucceed = entry.getValue().isSucceeded();
      if (!jobSucceed) {
        return false;
      }
    }
    return true;
  }

  /**
   * {@link com.linkedin.venice.pushmonitor.RouterBasedPushMonitor} will update the status of a NearlineProducer with
   * push type STREAM_REPROCESSING:
   * END_OF_PUSH_RECEIVED: isActive -> false; isStreamReprocessingJobSucceeded -> true
   * COMPLETED: isActive -> false; isStreamReprocessingJobSucceeded -> true
   * ERROR: isActive -> false; isStreamReprocessingJobSucceeded -> false
   *
   * For all the other push job status, NearlineProducer status will not be updated.
   */
  public void endStreamReprocessingNearlineProducer(NearlineProducer systemProducer, boolean jobSucceed) {
    nearlineProducerStates.put(systemProducer, new JobState(false, jobSucceed));
  }

  private static boolean isEmpty(String input) {
    return (input == null) || input.isEmpty() || input.equals("null");
  }

  public static NearlineProducerFactory getInstance() {
    return INSTANCE;
  }
}
