package com.linkedin.venice.integration.utils;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_OPTIONS_USE_DIRECT_READS;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.ADMIN_PORT;
import static com.linkedin.venice.ConfigKeys.CLUSTER_DISCOVERY_D2_SERVICE;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.ENABLE_SERVER_ALLOW_LIST;
import static com.linkedin.venice.ConfigKeys.KAFKA_READ_CYCLE_DELAY_MS;
import static com.linkedin.venice.ConfigKeys.KAFKA_SECURITY_PROTOCOL;
import static com.linkedin.venice.ConfigKeys.LISTENER_PORT;
import static com.linkedin.venice.ConfigKeys.MAX_ONLINE_OFFLINE_STATE_TRANSITION_THREAD_NUMBER;
import static com.linkedin.venice.ConfigKeys.PARTICIPANT_MESSAGE_CONSUMPTION_DELAY_MS;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.SERVER_DISK_FULL_THRESHOLD;
import static com.linkedin.venice.ConfigKeys.SERVER_HTTP2_INBOUND_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_INGESTION_ISOLATION_APPLICATION_PORT;
import static com.linkedin.venice.ConfigKeys.SERVER_INGESTION_ISOLATION_SERVICE_PORT;
import static com.linkedin.venice.ConfigKeys.SERVER_NETTY_GRACEFUL_SHUTDOWN_PERIOD_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_PARTITION_GRACEFUL_DROP_DELAY_IN_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_REST_SERVICE_STORAGE_THREAD_NUM;
import static com.linkedin.venice.ConfigKeys.SERVER_SSL_HANDSHAKE_THREAD_POOL_SIZE;
import static com.linkedin.venice.ConfigKeys.SYSTEM_SCHEMA_CLUSTER_NAME;
import static com.linkedin.venice.meta.PersistenceType.ROCKS_DB;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.AllowlistAccessor;
import com.linkedin.venice.helix.ZkAllowlistAccessor;
import com.linkedin.venice.pubsub.api.PubSubClientsFactory;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.server.VeniceServer;
import com.linkedin.venice.server.VeniceServerContext;
import com.linkedin.venice.servicediscovery.ServiceDiscoveryAnnouncer;
import com.linkedin.venice.tehuti.MetricsAware;
import com.linkedin.venice.utils.ForkedJavaProcess;
import com.linkedin.venice.utils.KafkaSSLUtils;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A wrapper for the {@link com.linkedin.venice.server.VeniceServer}.
 */
public class VeniceServerWrapper extends ProcessWrapper implements MetricsAware {
  private static final Logger LOGGER = LogManager.getLogger(VeniceServerWrapper.class);
  public static final String SERVICE_NAME = "VeniceServer";

  /**
   *  Possible config options which are not included in {@link com.linkedin.venice.ConfigKeys}.
    */
  public static final String SERVER_ENABLE_SERVER_ALLOW_LIST = "server_enable_allow_list";
  public static final String SERVER_IS_AUTO_JOIN = "server_is_auto_join";
  public static final String SERVER_ENABLE_SSL = "server_enable_ssl";
  public static final String SERVER_SSL_TO_KAFKA = "server_ssl_to_kafka";
  public static final String CLIENT_CONFIG_FOR_CONSUMER = "client_config_for_consumer";

  private TestVeniceServer veniceServer;
  private final VeniceProperties serverProps;
  private final VeniceConfigLoader config;
  private final ClientConfig consumerClientConfig;
  private final SSLFactory sslFactory;
  private final File dataDirectory;
  private final PubSubClientsFactory pubSubClientsFactory;

  /**
   * Following member fields are only needed when server runs in forked mode. We need to save these information and
   * pass as command line args when forking the server, so that the TestVeniceServer could be instantiated with
   * proper configurations.
   */
  private boolean forkServer = false;
  private String regionName = "";
  private String clusterName;
  private int listenPort;
  private String serverConfigPath;
  private boolean ssl;
  private boolean enableServerAllowlist;
  private boolean isAutoJoin;
  private String veniceUrl;
  private String d2ServiceName;
  private String serverName;
  private Process serverProcess;

  VeniceServerWrapper(
      String serviceName,
      File dataDirectory,
      TestVeniceServer veniceServer,
      VeniceProperties serverProps,
      VeniceConfigLoader config,
      ClientConfig consumerClientConfig,
      SSLFactory sslFactory,
      String regionName,
      PubSubClientsFactory pubSubClientsFactory) {
    super(serviceName, dataDirectory);
    this.dataDirectory = dataDirectory;
    this.veniceServer = veniceServer;
    this.serverProps = serverProps;
    this.config = config;
    this.consumerClientConfig = consumerClientConfig;
    this.sslFactory = sslFactory;
    this.regionName = regionName;
    this.pubSubClientsFactory = pubSubClientsFactory;
  }

  VeniceServerWrapper(
      String serviceName,
      File dataDirectory,
      TestVeniceServer veniceServer,
      VeniceProperties serverProps,
      VeniceConfigLoader config,
      ClientConfig consumerClientConfig,
      SSLFactory sslFactory,
      boolean forkServer,
      String clusterName,
      int listenPort,
      String serverConfigPath,
      boolean ssl,
      boolean enableServerAllowlist,
      boolean isAutoJoin,
      String serverName,
      String regionName,
      PubSubClientsFactory pubSubClientsFactory) {
    this(
        serviceName,
        dataDirectory,
        veniceServer,
        serverProps,
        config,
        consumerClientConfig,
        sslFactory,
        regionName,
        pubSubClientsFactory);
    this.forkServer = forkServer;
    this.clusterName = clusterName;
    this.listenPort = listenPort;
    this.serverConfigPath = serverConfigPath;
    this.ssl = ssl;
    this.enableServerAllowlist = enableServerAllowlist;
    this.isAutoJoin = isAutoJoin;
    if (consumerClientConfig != null) {
      this.veniceUrl = consumerClientConfig.getVeniceURL();
      this.d2ServiceName = consumerClientConfig.getD2ServiceName();
    }
    this.serverName = serverName;
    this.regionName = regionName;
  }

  static StatefulServiceProvider<VeniceServerWrapper> generateService(
      String regionName,
      String clusterName,
      String zkAddress,
      PubSubBrokerWrapper pubSubBrokerWrapper,
      Properties featureProperties,
      Properties configProperties,
      boolean forkServer,
      String serverName,
      Map<String, Map<String, String>> kafkaClusterMap,
      String serverD2ServiceName) {
    return (serviceName, dataDirectory) -> {
      boolean enableServerAllowlist =
          Boolean.parseBoolean(featureProperties.getProperty(SERVER_ENABLE_SERVER_ALLOW_LIST, "false"));
      boolean sslToKafka = Boolean.parseBoolean(featureProperties.getProperty(SERVER_SSL_TO_KAFKA, "false"));
      boolean ssl = Boolean.parseBoolean(featureProperties.getProperty(SERVER_ENABLE_SSL, "false"));
      boolean isAutoJoin = Boolean.parseBoolean(featureProperties.getProperty(SERVER_IS_AUTO_JOIN, "false"));
      ClientConfig consumerClientConfig = (ClientConfig) featureProperties.get(CLIENT_CONFIG_FOR_CONSUMER);

      /** Create config directory under {@link dataDirectory} */
      File configDirectory = new File(dataDirectory.getAbsolutePath(), "config");
      FileUtils.forceMkdir(configDirectory);

      // Generate cluster.properties in config directory
      VeniceProperties clusterProps =
          IntegrationTestUtils.getClusterProps(clusterName, zkAddress, pubSubBrokerWrapper, sslToKafka);
      File clusterConfigFile = new File(configDirectory, VeniceConfigLoader.CLUSTER_PROPERTIES_FILE);
      clusterProps.storeFlattened(clusterConfigFile);

      // Generate server.properties in config directory
      int listenPort = TestUtils.getFreePort();
      int ingestionIsolationApplicationPort = TestUtils.getFreePort();
      int ingestionIsolationServicePort = TestUtils.getFreePort();
      PropertyBuilder serverPropsBuilder = new PropertyBuilder().put(LISTENER_PORT, listenPort)
          .put(ADMIN_PORT, TestUtils.getFreePort())
          .put(DATA_BASE_PATH, dataDirectory.getAbsolutePath())
          .put(ENABLE_SERVER_ALLOW_LIST, enableServerAllowlist)
          .put(SERVER_REST_SERVICE_STORAGE_THREAD_NUM, 4)
          .put(MAX_ONLINE_OFFLINE_STATE_TRANSITION_THREAD_NUMBER, 100)
          .put(SERVER_NETTY_GRACEFUL_SHUTDOWN_PERIOD_SECONDS, 0)
          .put(PERSISTENCE_TYPE, ROCKS_DB)
          .put(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, true)
          .put(ROCKSDB_OPTIONS_USE_DIRECT_READS, false) // Required by PlainTable format
          .put(SERVER_PARTITION_GRACEFUL_DROP_DELAY_IN_SECONDS, 0)
          .put(PARTICIPANT_MESSAGE_CONSUMPTION_DELAY_MS, 1000)
          .put(KAFKA_READ_CYCLE_DELAY_MS, 50)
          .put(SERVER_DISK_FULL_THRESHOLD, 0.99) // Minimum free space is required in tests
          .put(SYSTEM_SCHEMA_CLUSTER_NAME, clusterName)
          .put(SERVER_INGESTION_ISOLATION_APPLICATION_PORT, ingestionIsolationApplicationPort)
          .put(SERVER_INGESTION_ISOLATION_SERVICE_PORT, ingestionIsolationServicePort)
          .put(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(1L))
          .put(CLUSTER_DISCOVERY_D2_SERVICE, VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
          .put(SERVER_SSL_HANDSHAKE_THREAD_POOL_SIZE, 10)
          .put(configProperties);
      if (sslToKafka) {
        serverPropsBuilder.put(KAFKA_SECURITY_PROTOCOL, SecurityProtocol.SSL.name);
        serverPropsBuilder.put(KafkaSSLUtils.getLocalCommonKafkaSSLConfig());
      }

      VeniceProperties serverProps = serverPropsBuilder.build();

      File serverConfigFile = new File(configDirectory, VeniceConfigLoader.SERVER_PROPERTIES_FILE);
      serverProps.storeFlattened(serverConfigFile);

      boolean https = serverProps.getBoolean(SERVER_HTTP2_INBOUND_ENABLED, false);
      String httpURI = "http://localhost:" + listenPort;
      String httpsURI = "https://localhost:" + listenPort;
      String d2ClusterName = D2TestUtils.setupD2Config(zkAddress, https, serverD2ServiceName);
      List<ServiceDiscoveryAnnouncer> d2Servers =
          new ArrayList<>(D2TestUtils.getD2Servers(zkAddress, d2ClusterName, httpURI, httpsURI));

      // generate the kafka cluster map in config directory
      VeniceConfigLoader.storeKafkaClusterMap(configDirectory, kafkaClusterMap);

      if (!forkServer) {
        VeniceConfigLoader veniceConfigLoader =
            VeniceConfigLoader.loadFromConfigDirectory(configDirectory.getAbsolutePath());

        if (enableServerAllowlist && isAutoJoin) {
          joinClusterAllowlist(
              veniceConfigLoader.getVeniceClusterConfig().getZookeeperAddress(),
              clusterName,
              listenPort);
        }

        SSLFactory sslFactory = ssl ? SslUtils.getVeniceLocalSslFactory() : null;

        VeniceServerContext.Builder serverContextBuilder =
            new VeniceServerContext.Builder().setVeniceConfigLoader(veniceConfigLoader)
                .setMetricsRepository(new MetricsRepository())
                .setSslFactory(sslFactory)
                .setClientConfigForConsumer(consumerClientConfig)
                .setServiceDiscoveryAnnouncers(d2Servers)
                .setPubSubClientsFactory(pubSubBrokerWrapper.getPubSubClientsFactory());

        TestVeniceServer server = new TestVeniceServer(serverContextBuilder.build());
        return new VeniceServerWrapper(
            serviceName,
            dataDirectory,
            server,
            serverProps,
            veniceConfigLoader,
            consumerClientConfig,
            sslFactory,
            regionName,
            pubSubBrokerWrapper.getPubSubClientsFactory());
      } else {
        return new VeniceServerWrapper(
            serviceName,
            dataDirectory,
            null,
            serverProps,
            null,
            consumerClientConfig,
            null,
            true,
            clusterName,
            listenPort,
            configDirectory.getAbsolutePath(),
            ssl,
            enableServerAllowlist,
            isAutoJoin,
            serverName,
            regionName,
            pubSubBrokerWrapper.getPubSubClientsFactory());
      }
    };
  }

  private static void joinClusterAllowlist(String zkAddress, String clusterName, int port) throws IOException {
    try (AllowlistAccessor accessor = new ZkAllowlistAccessor(zkAddress)) {
      accessor.addInstanceToAllowList(clusterName, Utils.getHelixNodeIdentifier(Utils.getHostName(), port));
    }
  }

  public File getDataDirectory() {
    return dataDirectory;
  }

  @Override
  public String getHost() {
    return DEFAULT_HOST_NAME;
  }

  /**
   * @return the value of the {@value com.linkedin.venice.ConfigKeys#LISTENER_PORT} config
   */
  @Override
  public int getPort() {
    return serverProps.getInt(LISTENER_PORT);
  }

  /**
   * @return the value of the {@value com.linkedin.venice.ConfigKeys#ADMIN_PORT} config
   */
  public int getAdminPort() {
    return serverProps.getInt(ADMIN_PORT);
  }

  @Override
  protected void internalStart() throws Exception {
    if (!forkServer) {
      veniceServer.start();

      TestUtils.waitForNonDeterministicCompletion(
          IntegrationTestUtils.MAX_ASYNC_START_WAIT_TIME_MS,
          TimeUnit.MILLISECONDS,
          () -> veniceServer.isStarted());
    } else {
      List<String> cmdList = new ArrayList<>();
      cmdList.addAll(
          Arrays.asList(
              "--clusterName",
              clusterName,
              "--listenPort",
              String.valueOf(listenPort),
              "--serverConfigPath",
              serverConfigPath));
      if (ssl) {
        cmdList.add("--ssl");
      }
      if (enableServerAllowlist) {
        cmdList.add("--enableServerAllowlist");
      }
      if (isAutoJoin) {
        cmdList.add("--isAutoJoin");
      }
      if (consumerClientConfig != null) {
        cmdList.add("--veniceUrl");
        cmdList.add(veniceUrl);
        cmdList.add("--d2ServiceName");
        cmdList.add(d2ServiceName);
      }
      serverProcess = ForkedJavaProcess.exec(
          VeniceServerWrapper.class,
          cmdList,
          Arrays.asList("-Xms64m", "-Xmx128m"),
          true,
          Optional.of(getComponentTagForLogging()));
      LOGGER.info("VeniceServer {} is started!", serverName);
    }
  }

  @Override
  protected void internalStop() throws Exception {
    if (!forkServer) {
      veniceServer.shutdown();
    } else {
      serverProcess.destroy();
    }
  }

  @Override
  protected void newProcess() throws Exception {
    if (forkServer) {
      return; // nothing to be done in forked mode.
    }
    this.veniceServer = new TestVeniceServer(
        new VeniceServerContext.Builder().setVeniceConfigLoader(config)
            .setMetricsRepository(new MetricsRepository())
            .setSslFactory(sslFactory)
            .setClientConfigForConsumer(consumerClientConfig)
            .setPubSubClientsFactory(pubSubClientsFactory)
            .build());
  }

  public TestVeniceServer getVeniceServer() {
    if (!forkServer) {
      return veniceServer;
    } else {
      throw new VeniceException("getVeniceServer is not supported in forked Mode");
    }
  }

  @Override
  public MetricsRepository getMetricsRepository() {
    if (!forkServer) {
      return veniceServer.getMetricsRepository();
    } else {
      throw new VeniceException("getMetricsRepository is not supported in forked Mode");
    }
  }

  @Override
  public String getComponentTagForLogging() {
    return new StringBuilder(getComponentTagPrefix(regionName)).append(super.getComponentTagForLogging()).toString();
  }

  public static void main(String args[]) throws Exception {
    // parse the inputs
    LOGGER.info("VeniceServer args: {}", Arrays.toString(args));
    Options options = new Options();
    options.addOption(new Option("cn", "clusterName", true, "cluster name"));
    options.addOption(new Option("lp", "listenPort", true, "listening port for server"));
    options.addOption(new Option("scp", "serverConfigPath", true, "path to server config file"));
    options.addOption(new Option("ss", "ssl", false, "is secured"));
    options.addOption(new Option("esa", "enableServerAllowlist", false, "allow listing enabled for the server"));
    options.addOption(new Option("iaj", "isAutoJoin", false, "automatically join the venice cluster"));
    options.addOption(new Option("vu", "veniceUrl", true, "ZK url for venice d2 service"));
    options.addOption(new Option("dsn", "d2ServiceName", true, "d2 service name"));
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);

    String clusterName = cmd.getOptionValue("cn");
    int listenPort = Integer.parseInt(cmd.getOptionValue("lp"));
    boolean ssl = false;
    String serverConfigPath = cmd.getOptionValue("scp");
    if (cmd.hasOption("ss")) {
      ssl = true;
    }
    boolean enableServerAllowlist = false;
    if (cmd.hasOption("esw") || cmd.hasOption("esa")) {
      enableServerAllowlist = true;
    }
    boolean isAutoJoin = false;
    if (cmd.hasOption("iaj")) {
      isAutoJoin = true;
    }
    ClientConfig consumerClientConfig = null;
    if (cmd.hasOption("vu") && cmd.hasOption("dsn")) {
      String veniceUrl = cmd.getOptionValue("vu");
      String d2ServiceName = cmd.getOptionValue("dsn");
      consumerClientConfig = new ClientConfig().setVeniceURL(veniceUrl)
          .setD2ServiceName(d2ServiceName)
          .setSslFactory(SslUtils.getVeniceLocalSslFactory());
    }

    VeniceConfigLoader veniceConfigLoader = VeniceConfigLoader.loadFromConfigDirectory(serverConfigPath);
    if (enableServerAllowlist && isAutoJoin) {
      joinClusterAllowlist(veniceConfigLoader.getVeniceClusterConfig().getZookeeperAddress(), clusterName, listenPort);
    }

    VeniceServerContext serverContext = new VeniceServerContext.Builder().setVeniceConfigLoader(veniceConfigLoader)
        .setMetricsRepository(new MetricsRepository())
        .setSslFactory(ssl ? SslUtils.getVeniceLocalSslFactory() : null)
        .setClientConfigForConsumer(consumerClientConfig)
        .setPubSubClientsFactory(ServiceFactory.getPubSubClientsFactory())
        .build();
    TestVeniceServer server = new TestVeniceServer(serverContext);

    if (!server.isStarted()) {
      server.start();
    }
    TestUtils.waitForNonDeterministicCompletion(
        IntegrationTestUtils.MAX_ASYNC_START_WAIT_TIME_MS,
        TimeUnit.MILLISECONDS,
        () -> server.isStarted());

    addShutdownHook(server);
  }

  private static void addShutdownHook(VeniceServer server) {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      LOGGER.info("shutting down server");
      server.shutdown();
    }));

    try {
      Thread.currentThread().join();
    } catch (InterruptedException e) {
      LOGGER.error("Unable to join thread in shutdown hook. ", e);
    }
  }
}
