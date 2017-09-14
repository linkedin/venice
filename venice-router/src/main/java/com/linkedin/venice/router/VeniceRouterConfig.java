package com.linkedin.venice.router;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
  private int longTailRetryThresholdMs;
  private int maxKeyCountInMultiGetReq;
  private int connectionLimit;
  private int httpClientPoolSize;
  private int maxOutgoingConnPerRoute;
  private int maxOutgoingConn;
  private Map<String, String> clusterToD2Map;

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
    longTailRetryThresholdMs = props.getInt(ROUTER_LONG_TAIL_RETRY_THRESHOLD_MS, 30); //30 ms
    maxKeyCountInMultiGetReq = props.getInt(ROUTER_MAX_KEY_COUNT_IN_MULTIGET_REQ, 500);
    connectionLimit = props.getInt(ROUTER_CONNECTION_LIMIT, 10000);
    httpClientPoolSize = props.getInt(ROUTER_HTTP_CLIENT_POOL_SIZE, 12);
    maxOutgoingConnPerRoute = props.getInt(ROUTER_MAX_OUTGOING_CONNECTION_PER_ROUTE, 120);
    maxOutgoingConn = props.getInt(ROUTER_MAX_OUTGOING_CONNECTION, 1200);
    clusterToD2Map = props.getMap(CLUSTER_TO_D2);
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

  public int getLongTailRetryThresholdMs() {
    return longTailRetryThresholdMs;
  }

  public int getMaxKeyCountInMultiGetReq() {
    return maxKeyCountInMultiGetReq;
  }

  public Map<String, String> getClusterToD2Map() {
    return clusterToD2Map;
  }

  public int getConnectionLimit() {
    return connectionLimit;
  }

  public int getHttpClientPoolSize() {
    return httpClientPoolSize;
  }

  public int getMaxOutgoingConnPerRoute() {
    return maxOutgoingConnPerRoute;
  }

  public int getMaxOutgoingConn() {
    return maxOutgoingConn;
  }
}
