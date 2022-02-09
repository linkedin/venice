package com.linkedin.venice.integration.utils;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.ddsstorage.linetty4.ssl.SslEngineComponentFactoryImpl;
import com.linkedin.security.ssl.access.control.SSLEngineComponentFactory;
import com.linkedin.security.ssl.access.control.SSLEngineComponentFactoryImpl;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.WhitelistAccessor;
import com.linkedin.venice.helix.ZkWhitelistAccessor;
import com.linkedin.venice.server.VeniceServer;
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

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.*;
import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.meta.PersistenceType.*;


/**
 * A wrapper for the {@link com.linkedin.venice.server.VeniceServer}.
 */
public class VeniceServerWrapper extends ProcessWrapper implements MetricsAware {
  private static final Logger logger = LogManager.getLogger(VeniceServerWrapper.class);
  public static final String SERVICE_NAME = "VeniceServer";

  /**
   *  Possible config options which are not included in {@link com.linkedin.venice.ConfigKeys}.
    */
  public static final String SERVER_ENABLE_SERVER_WHITE_LIST = "server_enable_white_list";
  public static final String SERVER_IS_AUTO_JOIN = "server_is_auto_join";
  public static final String SERVER_ENABLE_SSL = "server_enable_ssl";
  public static final String SERVER_SSL_TO_KAFKA = "server_ssl_to_kafka";
  public static final String CLIENT_CONFIG_FOR_CONSUMER = "client_config_for_consumer";

  private TestVeniceServer veniceServer;
  private final VeniceProperties serverProps;
  private final VeniceConfigLoader config;
  private final Optional<ClientConfig> consumerClientConfig;
  private final Optional<SSLEngineComponentFactory> sslFactory;
  private final File dataDirectory;

  /**
   * Following member fields are only needed when server runs in forked mode. We need to save these information and
   * pass as command line args when forking the server, so that the TestVeniceServer could be instantiated with
   * proper configurations.
   */
  private boolean forkServer = false;
  private String clusterName;
  private int listenPort;
  private String serverConfigPath;
  private boolean ssl;
  private boolean enableServerWhitelist;
  private boolean isAutoJoin;
  private boolean consumerClientConfigPresent;
  private String veniceUrl;
  private String d2ServiceName;
  private String serverName;
  private Process serverProcess;

  VeniceServerWrapper(String serviceName, File dataDirectory, TestVeniceServer veniceServer,
      VeniceProperties serverProps, VeniceConfigLoader config, Optional<ClientConfig> consumerClientConfig,
      Optional<SSLEngineComponentFactory> sslFactory) {
    super(serviceName, dataDirectory);
    this.dataDirectory = dataDirectory;
    this.veniceServer = veniceServer;
    this.serverProps = serverProps;
    this.config = config;
    this.consumerClientConfig = consumerClientConfig;
    this.sslFactory = sslFactory;
  }

  VeniceServerWrapper(String serviceName, File dataDirectory, TestVeniceServer veniceServer,
      VeniceProperties serverProps, VeniceConfigLoader config, Optional<ClientConfig> consumerClientConfig,
      Optional<SSLEngineComponentFactory> sslFactory, boolean forkServer, String clusterName, int listenPort,
      String serverConfigPath, boolean ssl, boolean enableServerWhitelist, boolean isAutoJoin, String serverName) {
    this(serviceName, dataDirectory, veniceServer, serverProps, config, consumerClientConfig, sslFactory);
    this.forkServer = forkServer;
    this.clusterName = clusterName;
    this.listenPort = listenPort;
    this.serverConfigPath = serverConfigPath;
    this.ssl = ssl;
    this.enableServerWhitelist = enableServerWhitelist;
    this.isAutoJoin = isAutoJoin;
    this.consumerClientConfigPresent = consumerClientConfig.isPresent();
    if (this.consumerClientConfigPresent) {
      this.veniceUrl = consumerClientConfig.get().getVeniceURL();
      this.d2ServiceName = consumerClientConfig.get().getD2ServiceName();
    }
    this.serverName = serverName;
  }

  static StatefulServiceProvider<VeniceServerWrapper> generateService(String clusterName,
      KafkaBrokerWrapper kafkaBrokerWrapper,
      Properties featureProperties,
      Properties configProperties,
      boolean forkServer,
      String serverName,
      Optional<Map<String, Map<String, String>>> kafkaClusterMap) {
    return (serviceName, dataDirectory) -> {
      boolean enableServerWhitelist = Boolean.parseBoolean(featureProperties.getProperty(SERVER_ENABLE_SERVER_WHITE_LIST, "false"));
      boolean sslToKafka = Boolean.parseBoolean(featureProperties.getProperty(SERVER_SSL_TO_KAFKA, "false"));
      boolean isKafkaOpenSSLEnabled = Boolean.parseBoolean(featureProperties.getProperty(SERVER_ENABLE_KAFKA_OPENSSL, "false"));
      boolean ssl = Boolean.parseBoolean(featureProperties.getProperty(SERVER_ENABLE_SSL, "false"));
      boolean isAutoJoin = Boolean.parseBoolean(featureProperties.getProperty(SERVER_IS_AUTO_JOIN, "false"));
      Optional<ClientConfig> consumerClientConfig = Optional.ofNullable(
          (ClientConfig) featureProperties.get(CLIENT_CONFIG_FOR_CONSUMER));

      /** Create config directory under {@link dataDirectory} */
      File configDirectory = new File(dataDirectory.getAbsolutePath(), "config");
      FileUtils.forceMkdir(configDirectory);

      // Generate cluster.properties in config directory
      VeniceProperties clusterProps = IntegrationTestUtils.getClusterProps(clusterName, dataDirectory,
          kafkaBrokerWrapper.getZkAddress(), kafkaBrokerWrapper, sslToKafka);
      File clusterConfigFile = new File(configDirectory, VeniceConfigLoader.CLUSTER_PROPERTIES_FILE);
      clusterProps.storeFlattened(clusterConfigFile);

      // Generate server.properties in config directory
      int listenPort = Utils.getFreePort();
      int ingestionIsolationApplicationPort = Utils.getFreePort();
      int ingestionIsolationServicePort = Utils.getFreePort();
      PropertyBuilder serverPropsBuilder = new PropertyBuilder()
          .put(LISTENER_PORT, listenPort)
          .put(ADMIN_PORT, Utils.getFreePort())
          .put(DATA_BASE_PATH, dataDirectory.getAbsolutePath())
          .put(ENABLE_SERVER_WHITE_LIST, enableServerWhitelist)
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
          .put(configProperties);
      if (sslToKafka) {
        serverPropsBuilder.put(KAFKA_SECURITY_PROTOCOL, SecurityProtocol.SSL.name);
        serverPropsBuilder.put(KafkaSSLUtils.getLocalCommonKafkaSSLConfig());
        if (isKafkaOpenSSLEnabled) {
          serverPropsBuilder.put(SERVER_ENABLE_KAFKA_OPENSSL, true);
        }
      }

      VeniceProperties serverProps = serverPropsBuilder.build();

      File serverConfigFile = new File(configDirectory, VeniceConfigLoader.SERVER_PROPERTIES_FILE);
      serverProps.storeFlattened(serverConfigFile);

      //generate the kafka cluster map in config directory
      VeniceConfigLoader.storeKafkaClusterMap(configDirectory, kafkaClusterMap);

      if (!forkServer) {
        VeniceConfigLoader veniceConfigLoader = VeniceConfigLoader.loadFromConfigDirectory(configDirectory.getAbsolutePath());

        if (enableServerWhitelist && isAutoJoin) {
          joinClusterWhitelist(veniceConfigLoader.getVeniceClusterConfig().getZookeeperAddress(), clusterName,
              listenPort);
        }

        Optional<SSLEngineComponentFactory> sslFactory = Optional.empty();
        if (ssl) {
          sslFactory = Optional.of(H2SSLUtils.getLocalHttp2SslFactory());
        }

        TestVeniceServer server = new TestVeniceServer(veniceConfigLoader, new MetricsRepository(), sslFactory, Optional.empty(),
            consumerClientConfig);
        return new VeniceServerWrapper(serviceName, dataDirectory, server, serverProps, veniceConfigLoader,
            consumerClientConfig, sslFactory);
      } else {
        VeniceServerWrapper veniceServerWrapper = new VeniceServerWrapper(serviceName, dataDirectory, null, serverProps, null,
            consumerClientConfig, null, true, clusterName, listenPort, configDirectory.getAbsolutePath(), ssl, enableServerWhitelist,
            isAutoJoin, serverName);
        return veniceServerWrapper;
      }
    };
  }

  private static void joinClusterWhitelist(String zkAddress, String clusterName, int port)
      throws IOException {
    try (WhitelistAccessor accessor = new ZkWhitelistAccessor(zkAddress)) {
      accessor.addInstanceToWhiteList(clusterName, Utils.getHelixNodeIdentifier(port));
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

      TestUtils.waitForNonDeterministicCompletion(IntegrationTestUtils.MAX_ASYNC_START_WAIT_TIME_MS, TimeUnit.MILLISECONDS,
          () -> veniceServer.isStarted());
    } else {
      List<String> cmdList = new ArrayList<>();
      cmdList.addAll(Arrays.asList(
          "--clusterName", clusterName,
          "--listenPort", String.valueOf(listenPort),
          "--serverConfigPath", serverConfigPath));
      if (ssl) {
        cmdList.add("--ssl");
      }
      if (enableServerWhitelist) {
        cmdList.add("--enableServerWhitelist");
      }
      if (isAutoJoin) {
        cmdList.add("--isAutoJoin");
      }
      if (consumerClientConfigPresent) {
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
          Optional.of(serverName));
      logger.info("VeniceServer " + serverName + " is started!");
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
    if (!forkServer) {
      this.veniceServer =
          new TestVeniceServer(config, new MetricsRepository(), sslFactory, Optional.empty(), consumerClientConfig);
    } else {
      //nothing to be done in forked mode.
    }
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

  public static void main(String args[]) throws Exception {
    //parse the inputs
    logger.info("VeniceServer args: " + Arrays.toString(args));
    Options options = new Options();
    options.addOption(new Option("cn", "clusterName", true, "cluster name"));
    options.addOption(new Option("lp", "listenPort", true, "listening port for server"));
    options.addOption(new Option("scp", "serverConfigPath", true, "path to server config file"));
    options.addOption(new Option("ss", "ssl", false, "is secured"));
    options.addOption(new Option("esw", "enableServerWhitelist", false, "white listing enabled for the server"));
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
    boolean enableServerWhitelist = false;
    if (cmd.hasOption("esw")) {
      enableServerWhitelist = true;
    }
    boolean isAutoJoin = false;
    if (cmd.hasOption("iaj")) {
      isAutoJoin = true;
    }
    Optional<ClientConfig> consumerClientConfig = Optional.empty();
    if (cmd.hasOption("vu") && cmd.hasOption("dsn"))  {
      String veniceUrl = cmd.getOptionValue("vu");
      String d2ServiceName = cmd.getOptionValue("dsn");
      consumerClientConfig = Optional.of(new ClientConfig().setVeniceURL(veniceUrl)
          .setD2ServiceName(D2TestUtils.getD2ServiceName(d2ServiceName, clusterName))
          .setSslEngineComponentFactory(SslUtils.getLocalSslFactory()));
    }

    VeniceConfigLoader veniceConfigLoader = VeniceConfigLoader.loadFromConfigDirectory(serverConfigPath);
    if (enableServerWhitelist && isAutoJoin) {
      joinClusterWhitelist(veniceConfigLoader.getVeniceClusterConfig().getZookeeperAddress(), clusterName,
          listenPort);
    }

    Optional<SSLEngineComponentFactory> sslFactory = Optional.empty();
    if (ssl){
      sslFactory = Optional.of(SslUtils.getLocalSslFactory());
    }

    TestVeniceServer server = new TestVeniceServer(veniceConfigLoader, new MetricsRepository(), sslFactory,
        Optional.empty(), consumerClientConfig);

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
        logger.info("shutting down server");
        server.shutdown();
    }));

    try {
      Thread.currentThread().join();
    } catch (InterruptedException e) {
      logger.error("Unable to join thread in shutdown hook. ", e);
    }
  }
}
