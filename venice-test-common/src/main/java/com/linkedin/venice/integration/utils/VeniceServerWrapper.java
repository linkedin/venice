package com.linkedin.venice.integration.utils;

import static com.linkedin.venice.ConfigKeys.*;

import com.linkedin.security.ssl.access.control.SSLEngineComponentFactory;
import com.linkedin.venice.helix.WhitelistAccessor;
import com.linkedin.venice.helix.ZkWhitelistAccessor;
import com.linkedin.venice.server.VeniceConfigLoader;
import com.linkedin.venice.server.VeniceServer;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;

import io.tehuti.metrics.MetricsRepository;
import java.util.Optional;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 * A wrapper for the {@link com.linkedin.venice.server.VeniceServer}.
 */
public class VeniceServerWrapper extends ProcessWrapper {
  public static final String SERVICE_NAME = "VeniceServer";

  private VeniceServer veniceServer;
  private final VeniceProperties serverProps;
  private final VeniceConfigLoader config;

  VeniceServerWrapper(String serviceName, File dataDirectory, VeniceServer veniceServer, VeniceProperties serverProps, VeniceConfigLoader config) {
    super(serviceName, dataDirectory);
    this.veniceServer = veniceServer;
    this.serverProps = serverProps;
    this.config = config;
  }

  static StatefulServiceProvider<VeniceServerWrapper> generateService(String clusterName, KafkaBrokerWrapper kafkaBrokerWrapper, boolean enableServerWhitelist, boolean isAutoJoin, boolean ssl) {
    return (serviceName, port, dataDirectory) -> {
      /** Create config directory under {@link dataDirectory} */
      File configDirectory = new File(dataDirectory.getAbsolutePath(), "config");
      FileUtils.forceMkdir(configDirectory);

      // Generate cluster.properties in config directory
      VeniceProperties clusterProps = IntegrationTestUtils.getClusterProps(clusterName, dataDirectory, kafkaBrokerWrapper.getZkAddress(), kafkaBrokerWrapper);
      File clusterConfigFile = new File(configDirectory, VeniceConfigLoader.CLUSTER_PROPERTIES_FILE);
      clusterProps.storeFlattened(clusterConfigFile);

      // Generate server.properties in config directory
      int listenPort = IntegrationTestUtils.getFreePort();
      VeniceProperties serverProps = new PropertyBuilder()
      .put(LISTENER_PORT, listenPort)
      .put(ADMIN_PORT, IntegrationTestUtils.getFreePort())
      .put(DATA_BASE_PATH, dataDirectory.getAbsolutePath())
      .put(ENABLE_SERVER_WHITE_LIST, enableServerWhitelist).build();

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

      VeniceServer server = new VeniceServer(veniceConfigLoader, new MetricsRepository(), sslFactory);
      return new VeniceServerWrapper(serviceName, dataDirectory, server, serverProps, veniceConfigLoader);
    };
  }

  private static void joinClusterWhitelist(String zkAddress, String clusterName, int port)
      throws IOException {
    try (WhitelistAccessor accessor = new ZkWhitelistAccessor(zkAddress)) {
      accessor.addInstanceToWhiteList(clusterName, Utils.getHelixNodeIdentifier(port));
    }
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
  protected void newProcess()
      throws Exception {
    this.veniceServer = new VeniceServer(config);
  }

  public VeniceServer getVeniceServer(){
    return veniceServer;
  }
}
