package com.linkedin.venice.integration.utils;

import static com.linkedin.venice.ConfigKeys.*;
import com.linkedin.venice.server.VeniceConfigLoader;
import com.linkedin.venice.server.VeniceServer;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.VeniceProperties;
import org.apache.commons.io.FileUtils;

import java.io.File;

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

  static StatefulServiceProvider<VeniceServerWrapper> generateService(String clusterName, KafkaBrokerWrapper kafkaBrokerWrapper) {
    return (serviceName, port, dataDirectory) -> {
      /** Create config directory under {@link dataDirectory} */
      File configDirectory = new File(dataDirectory.getAbsolutePath(), "config");
      FileUtils.forceMkdir(configDirectory);

      // Generate cluster.properties in config directory
      VeniceProperties clusterProps = TestUtils.getClusterProps(clusterName, dataDirectory, kafkaBrokerWrapper);
      File clusterConfigFile = new File(configDirectory, VeniceConfigLoader.CLUSTER_PROPERTIES_FILE);
      clusterProps.storeFlattened(clusterConfigFile);

      // Generate server.properties in config directory
      VeniceProperties serverProps = new PropertyBuilder()
      .put(NODE_ID, 0)
      .put(LISTENER_PORT, TestUtils.getFreePort())
      .put(ADMIN_PORT, TestUtils.getFreePort())
      .put(DATA_BASE_PATH, dataDirectory.getAbsolutePath()).build();

      File serverConfigFile = new File(configDirectory, VeniceConfigLoader.SERVER_PROPERTIES_FILE);
      serverProps.storeFlattened(serverConfigFile);

      VeniceConfigLoader veniceConfigLoader = VeniceConfigLoader.loadFromConfigDirectory(
              configDirectory.getAbsolutePath());

      VeniceServer server = new VeniceServer(veniceConfigLoader);
      return new VeniceServerWrapper(serviceName, dataDirectory, server, serverProps);
    };
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
  }

  @Override
  public void stop() throws Exception {
    veniceServer.shutdown();
  }
}
