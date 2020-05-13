package com.linkedin.venice.integration.utils;

import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.meta.PersistenceType.*;
import static com.linkedin.venice.store.rocksdb.RocksDBServerConfig.*;

import com.linkedin.security.ssl.access.control.SSLEngineComponentFactory;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.helix.WhitelistAccessor;
import com.linkedin.venice.helix.ZkWhitelistAccessor;
import com.linkedin.venice.server.VeniceConfigLoader;
import com.linkedin.venice.tehuti.MetricsAware;
import com.linkedin.venice.utils.KafkaSSLUtils;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;

import io.tehuti.metrics.MetricsRepository;
import java.util.Optional;
import java.util.Properties;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.protocol.SecurityProtocol;


/**
 * A wrapper for the {@link com.linkedin.venice.server.VeniceServer}.
 */
public class VeniceServerWrapper extends ProcessWrapper implements MetricsAware {
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

  static StatefulServiceProvider<VeniceServerWrapper> generateService(String clusterName,
      KafkaBrokerWrapper kafkaBrokerWrapper,
      Properties featureProperties,
      Properties configProperties) {
    return (serviceName, port, dataDirectory) -> {
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
      int listenPort = IntegrationTestUtils.getFreePort();
      PropertyBuilder serverPropsBuilder = new PropertyBuilder()
          .put(LISTENER_PORT, listenPort)
          .put(ADMIN_PORT, IntegrationTestUtils.getFreePort())
          .put(DATA_BASE_PATH, dataDirectory.getAbsolutePath())
          .put(ENABLE_SERVER_WHITE_LIST, enableServerWhitelist)
          .put(SERVER_REST_SERVICE_STORAGE_THREAD_NUM, 4)
          .put(MAX_ONLINE_OFFLINE_STATE_TRANSITION_THREAD_NUMBER, 100)
          .put(SERVER_NETTY_GRACEFUL_SHUTDOWN_PERIOD_SECONDS, 0)
          .put(PERSISTENCE_TYPE, ROCKS_DB)
          .put(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, true)
          .put(SERVER_ENABLE_ROCKSDB_METADATA, true)
          .put(ROCKSDB_OPTIONS_USE_DIRECT_READS, false) // Required by PlainTable format
          .put(SERVER_PARTITION_GRACEFUL_DROP_DELAY_IN_SECONDS, 0)
          .put(PARTICIPANT_MESSAGE_CONSUMPTION_DELAY_MS, 1000)
          .put(KAFKA_READ_CYCLE_DELAY_MS, 50)
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

      VeniceConfigLoader veniceConfigLoader = VeniceConfigLoader.loadFromConfigDirectory(
              configDirectory.getAbsolutePath());

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
      return new VeniceServerWrapper(serviceName, dataDirectory, server, serverProps, veniceConfigLoader,
          consumerClientConfig, sslFactory);
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
    veniceServer.start();

    TestUtils.waitForNonDeterministicCompletion(
        IntegrationTestUtils.MAX_ASYNC_START_WAIT_TIME_MS,
        TimeUnit.MILLISECONDS,
        () -> veniceServer.isStarted());
  }

  @Override
  protected void internalStop() throws Exception {
    veniceServer.shutdown();
  }

  @Override
  protected void newProcess() throws Exception {
    this.veniceServer = new TestVeniceServer(config, new MetricsRepository(), sslFactory, Optional.empty(),
        consumerClientConfig);
  }

  public TestVeniceServer getVeniceServer() {
    return veniceServer;
  }

  @Override
  public MetricsRepository getMetricsRepository() {
    return veniceServer.getMetricsRepository();
  }
}
