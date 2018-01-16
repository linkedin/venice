package com.linkedin.venice.router;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.router.cache.CacheEviction;
import com.linkedin.venice.router.cache.CacheType;
import com.linkedin.venice.utils.VeniceProperties;
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
  private int longTailRetryForSingleGetThresholdMs;
  private int maxKeyCountInMultiGetReq;
  private int connectionLimit;
  private int httpClientPoolSize;
  private int maxOutgoingConnPerRoute;
  private int maxOutgoingConn;
  private Map<String, String> clusterToD2Map;
  private boolean stickyRoutingEnabledForSingleGet;
  private boolean stickyRoutingEnabledForMultiGet;
  private double perStorageNodeReadQuotaBuffer;
  private int refreshAttemptsForZkReconnect;
  private long refreshIntervalForZkReconnectInMs;
  private boolean cacheEnabled;
  private long cacheSizeBytes;
  private int cacheConcurrency;
  private CacheType cacheType;
  private CacheEviction cacheEviction;
  private int cacheHashTableSize;
  private double cacheHitRequestThrottleWeight;
  private int routerNettyGracefulShutdownPeriodSeconds;
  private boolean enforceSecureOnly;

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
    longTailRetryForSingleGetThresholdMs = props.getInt(ROUTER_LONG_TAIL_RETRY_FOR_SINGLE_GET_THRESHOLD_MS, 15); //15 ms
    maxKeyCountInMultiGetReq = props.getInt(ROUTER_MAX_KEY_COUNT_IN_MULTIGET_REQ, 500);
    connectionLimit = props.getInt(ROUTER_CONNECTION_LIMIT, 10000);
    httpClientPoolSize = props.getInt(ROUTER_HTTP_CLIENT_POOL_SIZE, 12);
    maxOutgoingConnPerRoute = props.getInt(ROUTER_MAX_OUTGOING_CONNECTION_PER_ROUTE, 120);
    maxOutgoingConn = props.getInt(ROUTER_MAX_OUTGOING_CONNECTION, 1200);
    clusterToD2Map = props.getMap(CLUSTER_TO_D2);
    stickyRoutingEnabledForSingleGet = props.getBoolean(ROUTER_ENABLE_STICKY_ROUTING_FOR_SINGLE_GET, true);
    stickyRoutingEnabledForMultiGet = props.getBoolean(ROUTER_ENABLE_STICKY_ROUTING_FOR_MULTI_GET, true);
    perStorageNodeReadQuotaBuffer = props.getDouble(ROUTER_PER_STORAGE_NODE_READ_QUOTA_BUFFER, 1.0);
    refreshAttemptsForZkReconnect = props.getInt(REFRESH_ATTEMPTS_FOR_ZK_RECONNECT, 3);
    refreshIntervalForZkReconnectInMs =
        props.getLong(REFRESH_INTERVAL_FOR_ZK_RECONNECT_MS, java.util.concurrent.TimeUnit.SECONDS.toMillis(10));
    cacheEnabled = props.getBoolean(ROUTER_CACHE_ENABLED, false);
    cacheSizeBytes = props.getSizeInBytes(ROUTER_CACHE_SIZE_IN_BYTES, 500 * 1024 * 1024l); // 500MB
    cacheConcurrency = props.getInt(ROUTER_CACHE_CONCURRENCY, 16);
    cacheType = CacheType.valueOf(props.getString(ROUTER_CACHE_TYPE, CacheType.OFF_HEAP_CACHE.name()));
    cacheEviction = CacheEviction.valueOf(props.getString(ROUTER_CACHE_EVICTION, CacheEviction.W_TINY_LFU.name()));
    cacheHashTableSize = props.getInt(ROUTER_CACHE_HASH_TABLE_SIZE, 1024 * 1024); // 1M
    /**
     * Make the default value for the throttle weight of cache hit request to be 1, which is same as the regular request.
     * The reason behind this:
     * 1. Easy to reason w/o cache on both customer and Venice side;
     * 2. Hot key problem is still being alleviated since cache hit request is only being counted when calculating
     * store-level quota, not storage-node level quota, which means the hot keys routing to the same router at most
     * could use (total_quota / total_router_number);
     *
     * If it is not working well, we could adjust this config later on.
     */
    cacheHitRequestThrottleWeight = props.getDouble(ROUTER_CACHE_HIT_REQUEST_THROTTLE_WEIGHT, 1);
    routerNettyGracefulShutdownPeriodSeconds = props.getInt(ROUTER_NETTY_GRACEFUL_SHUTDOWN_PERIOD_SECONDS, 30); //30s
    enforceSecureOnly = props.getBoolean(ENFORCE_SECURE_ROUTER, false);
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

  public boolean isStickyRoutingEnabledForSingleGet() {
    return stickyRoutingEnabledForSingleGet;
  }

  public boolean isStickyRoutingEnabledForMultiGet() {
    return stickyRoutingEnabledForMultiGet;
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

  public int getLongTailRetryForSingleGetThresholdMs() {
    return longTailRetryForSingleGetThresholdMs;
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

  public double getPerStorageNodeReadQuotaBuffer() {
    return perStorageNodeReadQuotaBuffer;
  }

  public long getRefreshIntervalForZkReconnectInMs() {
    return refreshIntervalForZkReconnectInMs;
  }

  public int getRefreshAttemptsForZkReconnect() {
    return refreshAttemptsForZkReconnect;
  }

  public boolean isCacheEnabled() {
    return cacheEnabled;
  }

  public long getCacheSizeBytes() {
    return cacheSizeBytes;
  }

  public int getCacheConcurrency() {
    return cacheConcurrency;
  }

  public double getCacheHitRequestThrottleWeight() {
    return cacheHitRequestThrottleWeight;
  }

  public int getRouterNettyGracefulShutdownPeriodSeconds() {
    return routerNettyGracefulShutdownPeriodSeconds;
  }

  public boolean isEnforcingSecureOnly() {
    return enforceSecureOnly;
  }

  public CacheType getCacheType() {
    return cacheType;
  }

  public CacheEviction getCacheEviction() {
    return cacheEviction;
  }

  public int getCacheHashTableSize() {
    return cacheHashTableSize;
  }
}
