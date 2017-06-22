package com.linkedin.venice.router;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.List;
import org.apache.log4j.Logger;

import static com.linkedin.venice.ConfigKeys.*;


/**
 * Configuration for Venice Router.
 */
public class VeniceRouterConfig {
  private static final Logger logger = Logger.getLogger(VeniceRouterConfig.class);

  private String clusterName;
  private String zkConnection;
  private int port;
  private int sslPort;
  private int clientTimeoutMs;
  private int heartbeatTimeoutMs;
  private boolean sslToStorageNodes;
  private long maxReadCapacityCu;

  public VeniceRouterConfig(VeniceProperties props) {
    try {
      checkProperties(props);
      logger.info("Loaded configuration");
    } catch (Exception e) {
      String errorMessage = "Can not load properties.";
      logger.error(errorMessage);
      throw new VeniceException(errorMessage, e);
    }
  }

  private void checkProperties(VeniceProperties props) {
    clusterName = props.getString(CLUSTER_NAME);
    port = props.getInt(LISTENER_PORT);
    sslPort = props.getInt(LISTENER_SSL_PORT);
    zkConnection = props.getString(ZOOKEEPER_ADDRESS);
    clientTimeoutMs = props.getInt(CLIENT_TIMEOUT, 10000); //10s
    heartbeatTimeoutMs = props.getInt(HEARTBEAT_TIMEOUT, 1000); //1s
    sslToStorageNodes = props.getBoolean(SSL_TO_STORAGE_NODES, false); // disable ssl on path to stroage node by default.
    maxReadCapacityCu = props.getLong(MAX_READ_CAPCITY, 100000); //100000 CU
  }

  public String getClusterName() {
    return clusterName;
  }

  public String getZkConnection() {
    return zkConnection;
  }

  public int getPort() {
    return port;
  }

  public int getSslPort() {
    return sslPort;
  }

  public int getClientTimeoutMs() {
    return clientTimeoutMs;
  }

  public int getHeartbeatTimeoutMs() {
    return heartbeatTimeoutMs;
  }

  public boolean isSslToStorageNodes() {
    return sslToStorageNodes;
  }

  public long getMaxReadCapacityCu() {
    return maxReadCapacityCu;
  }
}
