package com.linkedin.venice.integration.utils;

import static com.linkedin.venice.ConfigKeys.*;
import com.linkedin.venice.server.VeniceConfigService;
import com.linkedin.venice.server.VeniceServer;
import com.linkedin.venice.utils.Props;
import org.apache.commons.io.FileUtils;

import java.io.File;

/**
 * A wrapper for the {@link com.linkedin.venice.server.VeniceServer}.
 */
public class VeniceServerWrapper extends ProcessWrapper {
  public static final String SERVICE_NAME = "VeniceServer";

  private final VeniceServer veniceServer;
  private final Props serverProps;

  VeniceServerWrapper(String serviceName, File dataDirectory, VeniceServer veniceServer, Props serverProps) {
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
      Props clusterProps = TestUtils.getClusterProps(clusterName, dataDirectory, kafkaBrokerWrapper);
      File clusterConfigFile = new File(configDirectory, VeniceConfigService.VENICE_CLUSTER_PROPERTIES_FILE);
      clusterProps.storeFlattened(clusterConfigFile);

      // Generate server.properties in config directory
      Props serverProps = new Props()
          .with(NODE_ID, 0)
          .with(LISTENER_PORT, TestUtils.getFreePort())
          .with(ADMIN_PORT, TestUtils.getFreePort())
          .with(DATA_BASE_PATH, dataDirectory.getAbsolutePath());
      File serverConfigFile = new File(configDirectory, VeniceConfigService.VENICE_SERVER_PROPERTIES_FILE);
      serverProps.storeFlattened(serverConfigFile);

      // Generate empty STORES directory underneath config directory
      File storesDirectory = new File(configDirectory, VeniceConfigService.STORE_CONFIGS_DIR_NAME);
      FileUtils.forceMkdir(storesDirectory);
      storesDirectory.mkdir();

      VeniceConfigService veniceConfigService = new VeniceConfigService(configDirectory.getAbsolutePath());
      VeniceServer server = new VeniceServer(veniceConfigService);
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
