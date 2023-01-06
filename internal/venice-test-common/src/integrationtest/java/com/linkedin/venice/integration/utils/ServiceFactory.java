package com.linkedin.venice.integration.utils;

import static com.linkedin.venice.ConfigKeys.CLIENT_USE_SYSTEM_STORE_REPOSITORY;
import static com.linkedin.venice.ConfigKeys.D2_ZK_HOSTS_ADDRESS;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_MAX_ATTEMPT;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_REPLICATION_FACTOR;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_WAIT_TIME_FOR_CLUSTER_START_S;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.davinci.client.AvroGenericDaVinciClient;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.server.AdminSparkServer;
import com.linkedin.venice.controllerapi.ControllerRoute;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.ExceptionUtils;
import com.linkedin.venice.utils.MockTime;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.ReflectUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A factory for generating Venice services and external service instances
 * used in integration tests.
 */
public class ServiceFactory {
  private static final Logger LOGGER = LogManager.getLogger(ServiceFactory.class);
  private static final String ULIMIT;
  private static final String VM_ARGS;
  /**
   * Calling {@link System#exit(int)} System.exit in tests is unacceptable. The Spark server lib, in particular, calls it.
   */
  static {
    TestUtils.preventSystemExit();

    String ulimitOutput;
    try {
      String[] cmd = { "/bin/bash", "-c", "ulimit -a" };
      Process proc = Runtime.getRuntime().exec(cmd);
      try (InputStream stderr = proc.getInputStream();
          InputStreamReader isr = new InputStreamReader(stderr);
          BufferedReader br = new BufferedReader(isr)) {
        String line;
        ulimitOutput = "";
        while ((line = br.readLine()) != null) {
          ulimitOutput += line + "\n";
        }
      } finally {
        proc.destroyForcibly();
      }
    } catch (IOException e) {
      ulimitOutput = "N/A";
      LOGGER.error("Could not run ulimit.");
    }
    ULIMIT = ulimitOutput;

    RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
    List<String> args = runtimeMxBean.getInputArguments();
    VM_ARGS = args.stream().collect(Collectors.joining(", "));
  }

  private static int maxAttempt = DEFAULT_MAX_ATTEMPT;

  public static void withMaxAttempt(int maxAttempt, Runnable action) {
    try {
      ServiceFactory.maxAttempt = maxAttempt;
      action.run();
    } finally {
      ServiceFactory.maxAttempt = DEFAULT_MAX_ATTEMPT;
    }
  }

  /**
   * @return an instance of {@link ZkServerWrapper}
   */
  public static ZkServerWrapper getZkServer() {
    return getStatefulService(ZkServerWrapper.SERVICE_NAME, ZkServerWrapper.generateService());
  }

  /**
   * @return an instance of {@link KafkaBrokerWrapper}
   */
  public static KafkaBrokerWrapper getKafkaBroker() {
    /**
     * Get the ZK dependency outside of the lambda, to avoid time complexity of
     * O({@value maxAttempt} ^2) on the amount of retries. {@link #getZkServer()}
     * has its own retries, so we can assume it's reliable enough.
     */

    return getKafkaBroker(ServiceFactory.getZkServer());
  }

  public static KafkaBrokerWrapper getKafkaBroker(ZkServerWrapper zkServerWrapper) {
    return getKafkaBroker(zkServerWrapper, Optional.empty());
  }

  public static KafkaBrokerWrapper getKafkaBroker(ZkServerWrapper zkServerWrapper, Optional<MockTime> mockTime) {
    return getStatefulService(
        KafkaBrokerWrapper.SERVICE_NAME,
        KafkaBrokerWrapper.generateService(zkServerWrapper, mockTime));
  }

  // to get parent controller, add child controllers to controllerCreateOptions
  public static VeniceControllerWrapper getVeniceController(VeniceControllerCreateOptions controllerCreateOptions) {
    final String serviceName;
    if (controllerCreateOptions.isParent()) {
      serviceName = VeniceControllerWrapper.PARENT_D2_SERVICE_NAME;
    } else {
      serviceName = VeniceControllerWrapper.D2_SERVICE_NAME;
    }
    return getStatefulService(serviceName, VeniceControllerWrapper.generateService(controllerCreateOptions));
  }

  public static AdminSparkServer getMockAdminSparkServer(
      Admin admin,
      String cluster,
      List<ControllerRoute> bannedRoutes) {
    return getService("MockAdminSparkServer", (serviceName) -> {
      Set<String> clusters = new HashSet<>();
      clusters.add(cluster);
      AdminSparkServer server = new AdminSparkServer(
          Utils.getFreePort(),
          admin,
          new MetricsRepository(),
          clusters,
          false,
          Optional.empty(),
          false,
          Optional.empty(),
          bannedRoutes,
          null,
          false);
      server.start();
      return server;
    });
  }

  public static VeniceServerWrapper getVeniceServer(
      String clusterName,
      KafkaBrokerWrapper kafkaBrokerWrapper,
      String zkAddress,
      Properties featureProperties,
      Properties configProperties) {
    return getVeniceServer(
        clusterName,
        kafkaBrokerWrapper,
        zkAddress,
        featureProperties,
        configProperties,
        false,
        "",
        Collections.emptyMap());
  }

  public static VeniceServerWrapper getVeniceServer(
      String clusterName,
      KafkaBrokerWrapper kafkaBrokerWrapper,
      String zkAddress,
      Properties featureProperties,
      Properties configProperties,
      boolean forkServer,
      String serverName,
      Map<String, Map<String, String>> kafkaClusterMap) {
    // Set ZK host needed for D2 client creation ingestion isolation ingestion.
    configProperties.setProperty(D2_ZK_HOSTS_ADDRESS, zkAddress);
    return getStatefulService(
        VeniceServerWrapper.SERVICE_NAME,
        VeniceServerWrapper.generateService(
            clusterName,
            kafkaBrokerWrapper,
            featureProperties,
            configProperties,
            forkServer,
            serverName,
            kafkaClusterMap));
  }

  static VeniceRouterWrapper getVeniceRouter(
      String clusterName,
      ZkServerWrapper zkServerWrapper,
      KafkaBrokerWrapper kafkaBrokerWrapper,
      boolean sslToStorageNodes,
      Map<String, String> clusterToD2,
      Properties extraProperties) {
    return getService(
        VeniceRouterWrapper.SERVICE_NAME,
        VeniceRouterWrapper.generateService(
            clusterName,
            zkServerWrapper,
            kafkaBrokerWrapper,
            sslToStorageNodes,
            clusterToD2,
            extraProperties));
  }

  public static MockVeniceRouterWrapper getMockVeniceRouter(
      String zkAddress,
      boolean sslToStorageNodes,
      Properties extraConfigs) {
    return getService(
        MockVeniceRouterWrapper.SERVICE_NAME,
        MockVeniceRouterWrapper.generateService(zkAddress, sslToStorageNodes, extraConfigs));
  }

  public static MockD2ServerWrapper getMockD2Server(String serviceName, String d2ServiceName) {
    return getService(serviceName, MockD2ServerWrapper.generateService(d2ServiceName));
  }

  /**
   * Initialize MockHttpServerWrapper, this function will setup a simple http server
   */
  public static MockHttpServerWrapper getMockHttpServer(String serviceName) {
    return getService(serviceName, MockHttpServerWrapper.generateService());
  }

  /**
   * Start up a testing Venice cluster in another process.
   *
   * The reason to call this method instead of other {@link #getVeniceCluster()} methods is
   * when one wants to maximize its testing environment isolation.
   * Example usage: {@literal com.linkedin.venice.benchmark.IngestionBenchmarkWithTwoProcesses}
   *
   * @param clusterInfoFilePath works as IPC to pass back the needed information to the caller process
   */
  public static void startVeniceClusterInAnotherProcess(String clusterInfoFilePath) {
    startVeniceClusterInAnotherProcess(clusterInfoFilePath, DEFAULT_WAIT_TIME_FOR_CLUSTER_START_S);
  }

  /**
   * Start up a testing Venice cluster in another process.
   *
   * The reason to call this method instead of other {@link #getVeniceCluster()} methods is
   * when one wants to maximize its testing environment isolation.
   * Example usage: {@literal com.linkedin.venice.benchmark.IngestionBenchmarkWithTwoProcesses}
   *
   * @param clusterInfoFilePath works as IPC to pass back the needed information to the caller process
   * @param waitTimeInSeconds gives some wait time to make sure all the services have been started in the other process.
   *                          The default wait time is an empirical value based on observations. If we have more
   *                          components to start in the future, this value needs to be increased.
   */
  public static void startVeniceClusterInAnotherProcess(String clusterInfoFilePath, int waitTimeInSeconds) {
    try {
      VeniceClusterWrapper.generateServiceInAnotherProcess(clusterInfoFilePath, waitTimeInSeconds);
    } catch (IOException | InterruptedException e) {
      throw new VeniceException("Start Venice cluster in another process has failed", e);
    }
  }

  public static void stopVeniceClusterInAnotherProcess() {
    VeniceClusterWrapper.stopServiceInAnotherProcess();
  }

  public static VeniceClusterWrapper getVeniceCluster(VeniceClusterCreateOptions options) {
    return getService(VeniceClusterWrapper.SERVICE_NAME, VeniceClusterWrapper.generateService(options));
  }

  /**
   * @deprecated
   * <p> Use {@link ServiceFactory#getVeniceCluster(VeniceClusterCreateOptions)} instead.
   */
  @Deprecated
  public static VeniceClusterWrapper getVeniceCluster() {
    VeniceClusterCreateOptions options =
        new VeniceClusterCreateOptions.Builder().numberOfControllers(1).numberOfServers(1).numberOfRouters(1).build();
    return getVeniceCluster(options);
  }

  @Deprecated
  public static VeniceClusterWrapper getVeniceCluster(
      int numberOfControllers,
      int numberOfServers,
      int numberOfRouters) {
    VeniceClusterCreateOptions options =
        new VeniceClusterCreateOptions.Builder().numberOfControllers(numberOfControllers)
            .numberOfServers(numberOfServers)
            .numberOfRouters(numberOfRouters)
            .build();
    return getVeniceCluster(options);
  }

  @Deprecated
  public static VeniceClusterWrapper getVeniceCluster(
      int numberOfControllers,
      int numberOfServers,
      int numberOfRouters,
      int replicationFactor) {
    VeniceClusterCreateOptions options =
        new VeniceClusterCreateOptions.Builder().numberOfControllers(numberOfControllers)
            .numberOfServers(numberOfServers)
            .numberOfRouters(numberOfRouters)
            .replicationFactor(replicationFactor)
            .minActiveReplica(replicationFactor - 1)
            .build();
    return getVeniceCluster(options);
  }

  @Deprecated
  public static VeniceClusterWrapper getVeniceCluster(
      int numberOfControllers,
      int numberOfServers,
      int numberOfRouters,
      int replicationFactor,
      int partitionSize,
      boolean sslToStorageNodes,
      boolean sslToKafka,
      Properties extraProperties) {
    VeniceClusterCreateOptions options =
        new VeniceClusterCreateOptions.Builder().numberOfControllers(numberOfControllers)
            .numberOfServers(numberOfServers)
            .numberOfRouters(numberOfRouters)
            .replicationFactor(replicationFactor)
            .partitionSize(partitionSize)
            .minActiveReplica(replicationFactor - 1)
            .sslToStorageNodes(sslToStorageNodes)
            .sslToKafka(sslToKafka)
            .extraProperties(extraProperties)
            .build();
    return getVeniceCluster(options);
  }

  @Deprecated
  public static VeniceClusterWrapper getVeniceCluster(
      int numberOfControllers,
      int numberOfServers,
      int numberOfRouters,
      int replicationFactor,
      int partitionSize,
      boolean sslToStorageNodes,
      boolean sslToKafka) {
    VeniceClusterCreateOptions options =
        new VeniceClusterCreateOptions.Builder().numberOfControllers(numberOfControllers)
            .numberOfServers(numberOfServers)
            .numberOfRouters(numberOfRouters)
            .replicationFactor(replicationFactor)
            .partitionSize(partitionSize)
            .minActiveReplica(replicationFactor - 1)
            .sslToStorageNodes(sslToStorageNodes)
            .sslToKafka(sslToKafka)
            .build();
    return getVeniceCluster(options);
  }

  public static VeniceMultiClusterWrapper getVeniceMultiClusterWrapper(VeniceMultiClusterCreateOptions options) {
    return getService(VeniceMultiClusterWrapper.SERVICE_NAME, VeniceMultiClusterWrapper.generateService(options));
  }

  public static VeniceTwoLayerMultiColoMultiClusterWrapper getVeniceTwoLayerMultiColoMultiClusterWrapper(
      int numberOfColos,
      int numberOfClustersInEachColo,
      int numberOfParentControllers,
      int numberOfControllers,
      int numberOfServers,
      int numberOfRouters) {
    return getService(
        VeniceTwoLayerMultiColoMultiClusterWrapper.SERVICE_NAME,
        VeniceTwoLayerMultiColoMultiClusterWrapper.generateService(
            numberOfColos,
            numberOfClustersInEachColo,
            numberOfParentControllers,
            numberOfControllers,
            numberOfServers,
            numberOfRouters,
            DEFAULT_REPLICATION_FACTOR,
            Optional.empty(),
            Optional.empty()));
  }

  public static VeniceTwoLayerMultiColoMultiClusterWrapper getVeniceTwoLayerMultiColoMultiClusterWrapper(
      int numberOfColos,
      int numberOfClustersInEachColo,
      int numberOfParentControllers,
      int numberOfControllers,
      int numberOfServers,
      int numberOfRouters,
      int replicationFactor,
      Optional<VeniceProperties> parentControllerProps,
      Optional<Properties> childControllerProperties,
      Optional<VeniceProperties> serverProps) {
    return getService(
        VeniceTwoLayerMultiColoMultiClusterWrapper.SERVICE_NAME,
        VeniceTwoLayerMultiColoMultiClusterWrapper.generateService(
            numberOfColos,
            numberOfClustersInEachColo,
            numberOfParentControllers,
            numberOfControllers,
            numberOfServers,
            numberOfRouters,
            replicationFactor,
            parentControllerProps,
            childControllerProperties,
            serverProps,
            false));
  }

  public static VeniceTwoLayerMultiColoMultiClusterWrapper getVeniceTwoLayerMultiColoMultiClusterWrapper(
      int numberOfColos,
      int numberOfClustersInEachColo,
      int numberOfParentControllers,
      int numberOfControllers,
      int numberOfServers,
      int numberOfRouters,
      int replicationFactor,
      Optional<VeniceProperties> parentControllerProps,
      Optional<Properties> childControllerProperties,
      Optional<VeniceProperties> serverProps,
      boolean forkServer) {
    return getService(
        VeniceTwoLayerMultiColoMultiClusterWrapper.SERVICE_NAME,
        VeniceTwoLayerMultiColoMultiClusterWrapper.generateService(
            numberOfColos,
            numberOfClustersInEachColo,
            numberOfParentControllers,
            numberOfControllers,
            numberOfServers,
            numberOfRouters,
            replicationFactor,
            parentControllerProps,
            childControllerProperties,
            serverProps,
            forkServer));
  }

  public static HelixAsAServiceWrapper getHelixController(String zkAddress) {
    return getService(HelixAsAServiceWrapper.SERVICE_NAME, HelixAsAServiceWrapper.generateService(zkAddress));
  }

  private static <S extends ProcessWrapper> S getStatefulService(
      String serviceName,
      StatefulServiceProvider<S> serviceProvider) {
    return getService(serviceName, serviceProvider);
  }

  private static <S extends Closeable> S getService(String serviceName, ServiceProvider<S> serviceProvider) {
    // Just some initial state. If the fabric of space-time holds up, you should never see these strings.
    Exception lastException = new VeniceException("There is no spoon.");
    String errorMessage = "If you see this message, something went horribly wrong.";

    for (int attempt = 1; attempt <= maxAttempt; attempt++) {
      S wrapper = null;
      try {
        wrapper = serviceProvider.get(serviceName);

        if (wrapper instanceof ProcessWrapper) {
          LOGGER.info("Starting ProcessWrapper: {}", serviceName);

          // N.B.: The contract for start() is that it should block until the wrapped service is fully started.
          ProcessWrapper processWrapper = (ProcessWrapper) wrapper;
          processWrapper.start();

          LOGGER.info("Started ProcessWrapper: {}", serviceName);
        }
        return wrapper;
      } catch (NoSuchMethodError e) {
        LOGGER.error(
            "Got a {} while trying to start {}. Will print the jar containing the bad class and then bubble up.",
            e.getClass().getSimpleName(),
            serviceName);
        ReflectUtils.printJarContainingBadClass(e);
        Utils.closeQuietlyWithErrorLogged(wrapper);
        throw e;
      } catch (LinkageError e) {
        LOGGER.error(
            "Got a {} while trying to start {}. Will print the classpath and then bubble up.",
            e.getClass().getSimpleName(),
            serviceName);
        ReflectUtils.printClasspath();
        Utils.closeQuietlyWithErrorLogged(wrapper);
        throw e;
      } catch (InterruptedException e) {
        // This should mean that TestNG has timed out the test. We'll try to sneak a fast one and still close
        // the process asynchronously, so it doesn't leak.
        final S finalWrapper = wrapper;
        CompletableFuture.runAsync(() -> Utils.closeQuietlyWithErrorLogged(finalWrapper));
        throw new VeniceException("Interrupted!", e);
      } catch (Exception e) {
        Utils.closeQuietlyWithErrorLogged(wrapper);
        if (ExceptionUtils.recursiveMessageContains(e, "Too many open files")) {
          throw new VeniceException("Too many open files!\nVM args: " + VM_ARGS + "\n$ ulimit -a\n" + ULIMIT, e);
        }
        lastException = e;
        errorMessage = new StringBuilder("Got ").append(e.getClass().getSimpleName())
            .append(" while trying to start ")
            .append(serviceName)
            .append(". Attempt #")
            .append(attempt)
            .append("/")
            .append(maxAttempt)
            .append(".")
            .toString();
        LOGGER.warn(errorMessage, e);
        // We don't throw for other exception types, since we want to retry.
      }
    }

    throw new VeniceException(errorMessage + " Aborting.", lastException);
  }

  public static <K, V> DaVinciClient<K, V> getGenericAvroDaVinciClient(String storeName, VeniceClusterWrapper cluster) {
    return getGenericAvroDaVinciClient(storeName, cluster, Utils.getTempDataDirectory().getAbsolutePath());
  }

  public static <K, V> DaVinciClient<K, V> getGenericAvroDaVinciClient(
      String storeName,
      VeniceClusterWrapper cluster,
      String dataBasePath) {
    return getGenericAvroDaVinciClient(storeName, cluster, dataBasePath, new DaVinciConfig());
  }

  public static <K, V> DaVinciClient<K, V> getGenericAvroDaVinciClient(
      String storeName,
      VeniceClusterWrapper cluster,
      String dataBasePath,
      DaVinciConfig daVinciConfig) {
    VeniceProperties backendConfig = DaVinciTestContext.getDaVinciPropertyBuilder(cluster.getZk().getAddress())
        .put(DATA_BASE_PATH, dataBasePath)
        .build();
    return getGenericAvroDaVinciClient(storeName, cluster, daVinciConfig, backendConfig);
  }

  public static <K, V> DaVinciClient<K, V> getGenericAvroDaVinciClient(
      String storeName,
      VeniceClusterWrapper cluster,
      DaVinciConfig daVinciConfig,
      VeniceProperties backendConfig) {
    return getGenericAvroDaVinciClient(storeName, cluster.getZk().getAddress(), daVinciConfig, backendConfig);
  }

  public static <K, V> DaVinciClient<K, V> getGenericAvroDaVinciClient(
      String storeName,
      String zkAddress,
      DaVinciConfig daVinciConfig,
      VeniceProperties backendConfig) {
    ClientConfig clientConfig = ClientConfig.defaultGenericClientConfig(storeName)
        .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
        .setVeniceURL(zkAddress);
    PropertyBuilder daVinciPropertyBuilder = DaVinciTestContext.getDaVinciPropertyBuilder(zkAddress);
    backendConfig.getPropertiesCopy().forEach((key, value) -> {
      daVinciPropertyBuilder.put(key.toString(), value);
    });

    DaVinciClient<K, V> client =
        new AvroGenericDaVinciClient<>(daVinciConfig, clientConfig, daVinciPropertyBuilder.build(), Optional.empty());
    client.start();
    return client;
  }

  public static <K, V> DaVinciClient<K, V> getGenericAvroDaVinciClientWithoutMetaSystemStoreRepo(
      String storeName,
      String zkAddress,
      String dataBasePath) {
    Properties extraBackendConfig = new Properties();
    extraBackendConfig.setProperty(DATA_BASE_PATH, dataBasePath);
    extraBackendConfig.setProperty(CLIENT_USE_SYSTEM_STORE_REPOSITORY, String.valueOf(false));
    return getGenericAvroDaVinciClient(
        storeName,
        zkAddress,
        new DaVinciConfig(),
        new VeniceProperties(extraBackendConfig));
  }

  public static <K, V> DaVinciClient<K, V> getGenericAvroDaVinciClientWithRetries(
      String storeName,
      String zkAddress,
      DaVinciConfig daVinciConfig,
      Map<String, Object> extraBackendProperties) {
    return DaVinciTestContext
        .getGenericAvroDaVinciClientWithRetries(storeName, zkAddress, daVinciConfig, extraBackendProperties);
  }

  public static <K, V> DaVinciTestContext<K, V> getGenericAvroDaVinciFactoryAndClientWithRetries(
      D2Client d2Client,
      MetricsRepository metricsRepository,
      Optional<Set<String>> managedClients,
      String zkAddress,
      String storeName,
      DaVinciConfig daVinciConfig,
      Map<String, Object> extraBackendProperties) {
    return DaVinciTestContext.getGenericAvroDaVinciFactoryAndClientWithRetries(
        d2Client,
        metricsRepository,
        managedClients,
        zkAddress,
        storeName,
        daVinciConfig,
        extraBackendProperties);
  }
}
