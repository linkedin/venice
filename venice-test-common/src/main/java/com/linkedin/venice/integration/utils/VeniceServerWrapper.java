package com.linkedin.venice.integration.utils;

import static com.linkedin.venice.ConfigKeys.*;

import com.linkedin.venice.helix.WhitelistAccessor;
import com.linkedin.venice.helix.ZkWhitelistAccessor;
import com.linkedin.venice.server.VeniceConfigLoader;
import com.linkedin.venice.server.VeniceServer;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 * A wrapper for the {@link com.linkedin.venice.server.VeniceServer}.
 */
public class VeniceServerWrapper extends ProcessWrapper {
  public static final String SERVICE_NAME = "VeniceServer";

  private final VeniceServer veniceServer;
  private final VeniceProperties serverProps;

  VeniceServerWrapper(String serviceName, File dataDirectory, VeniceServer veniceServer, VeniceProperties serverProps) {
    super(serviceName, dataDirectory);
    this.veniceServer = veniceServer;
    this.serverProps = serverProps;
  }

  static StatefulServiceProvider<VeniceServerWrapper> generateService(String clusterName, KafkaBrokerWrapper kafkaBrokerWrapper, boolean enableServerWhitelist, boolean isAutoJoin) {
    return (serviceName, port, dataDirectory) -> {
      /** Create config directory under {@link dataDirectory} */
      File configDirectory = new File(dataDirectory.getAbsolutePath(), "config");
      FileUtils.forceMkdir(configDirectory);

      // Generate cluster.properties in config directory
      VeniceProperties clusterProps = IntegrationTestUtils.getClusterProps(clusterName, dataDirectory, kafkaBrokerWrapper);
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

      if (isAutoJoin) {
        joinClusterWhitelist(veniceConfigLoader.getVeniceClusterConfig().getZookeeperAddress(), clusterName,
            listenPort);
      }
      VeniceServer server = new VeniceServer(veniceConfigLoader);
      return new VeniceServerWrapper(serviceName, dataDirectory, server, serverProps);
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
  public void start() throws Exception {
    veniceServer.start();

    TestUtils.waitForNonDeterministicCompletion(
        IntegrationTestUtils.MAX_ASYNC_START_WAIT_TIME_MS,
        TimeUnit.MILLISECONDS,
        () -> veniceServer.isStarted());
  }

  @Override
  public void stop() throws Exception {
    veniceServer.shutdown();
  }

  public VeniceServer getVeniceServer(){
    return veniceServer;
  }
}
