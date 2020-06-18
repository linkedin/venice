package com.linkedin.venice.router;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.router.cache.CacheEviction;
import com.linkedin.venice.router.cache.CacheType;
import com.linkedin.venice.router.httpclient.StorageNodeClientType;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
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
  private double heartbeatTimeoutMs;
  private long heartbeatCycleMs;
  private boolean sslToStorageNodes;
  private long maxReadCapacityCu;
  private int longTailRetryForSingleGetThresholdMs;
  private TreeMap<Integer, Integer> longTailRetryForBatchGetThresholdMs;
  private boolean smartLongTailRetryEnabled;
  private int smartLongTailRetryAbortThresholdMs;
  private int longTailRetryMaxRouteForMultiKeyReq;
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
  private long cacheTTLmillis;
  private int routerNettyGracefulShutdownPeriodSeconds;
  private boolean enforceSecureOnly;
  private boolean dnsCacheEnabled;
  private String hostPatternForDnsCache;
  private long dnsCacheRefreshIntervalInMs;
  private boolean greedyMultiGet;
  private long singleGetTardyLatencyThresholdMs;
  private long multiGetTardyLatencyThresholdMs;
  private long computeTardyLatencyThresholdMs;
  private boolean readThrottlingEnabled;
  private long maxPendingRequest;
  private StorageNodeClientType storageNodeClientType;
  private int nettyClientEventLoopThreads;
  private long nettyClientChannelPoolAcquireTimeoutMs;
  private int nettyClientChannelPoolMinConnections;
  private int nettyClientChannelPoolMaxConnections;
  private int nettyClientChannelPoolMaxPendingAcquires;
  private long nettyClientChannelPoolHealthCheckIntervalMs;
  private int nettyClientMaxAggregatedObjectLength;
  private boolean decompressOnClient;
  private boolean streamingEnabled;
  private boolean computeFastAvroEnabled;
  private int socketTimeout;
  private int connectionTimeout;
  private boolean statefulRouterHealthCheckEnabled;
  private int routerUnhealthyPendingConnThresholdPerRoute;
  private int routerPendingConnResumeThresholdPerRoute;
  private boolean perNodeClientAllocationEnabled;
  private int perNodeClientThreadCount;
  private boolean keyValueProfilingEnabled;
  private long leakedFutureCleanupPollIntervalMs;
  private long leakedFutureCleanupThresholdMs;
  private String kafkaZkAddress;
  private String kafkaBootstrapServers;
  private boolean idleConnectionToServerCleanupEnabled;
  private long idleConnectionToServerCleanupThresholdMins;
  private long fullPendingQueueServerOORMs;
  private boolean httpasyncclientConnectionWarmingEnabled;
  private long httpasyncclientConnectionWarmingSleepIntervalMs;
  private int dictionaryRetrievalTimeMs;
  private int routerDictionaryProcessingThreads;
  private int httpasyncclientConnectionWarmingLowWaterMark;
  private int httpasyncclientConnectionWarmingExecutorThreadNum;
  private long httpasyncclientConnectionWarmingNewInstanceDelayJoinMs;
  private boolean asyncStartEnabled;
  private boolean earlyThrottleEnabled;
  private long routerQuotaCheckWindow;
  private long maxRouterReadCapacityCu;
  private boolean helixCustomizedViewEnabled;
  private int ioThreadCountInPoolMode;
  private boolean leastLoadedHostSelectionEnabled;


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
    kafkaZkAddress = props.getString(KAFKA_ZK_ADDRESS);
    kafkaBootstrapServers = props.getString(KAFKA_BOOTSTRAP_SERVERS);
    clientTimeoutMs = props.getInt(CLIENT_TIMEOUT, 10000); //10s
    heartbeatTimeoutMs = props.getDouble(HEARTBEAT_TIMEOUT, TimeUnit.MINUTES.toMillis(1)); // 1 minute
    heartbeatCycleMs = props.getLong(HEARTBEAT_CYCLE, TimeUnit.SECONDS.toMillis(5)); // 5 seconds
    sslToStorageNodes = props.getBoolean(SSL_TO_STORAGE_NODES, false); // disable ssl on path to stroage node by default.
    maxReadCapacityCu = props.getLong(MAX_READ_CAPCITY, 100000); //100000 CU
    longTailRetryForSingleGetThresholdMs = props.getInt(ROUTER_LONG_TAIL_RETRY_FOR_SINGLE_GET_THRESHOLD_MS, 15); //15 ms
    longTailRetryForBatchGetThresholdMs = parseRetryThresholdForBatchGet(
        props.getString(ROUTER_LONG_TAIL_RETRY_FOR_BATCH_GET_THRESHOLD_MS, "1-5:15,6-20:30,21-150:50,151-500:100,501-:500"));
    // disable smart long tail retry by default
    smartLongTailRetryEnabled = props.getBoolean(ROUTER_SMART_LONG_TAIL_RETRY_ENABLED, false);
    smartLongTailRetryAbortThresholdMs = props.getInt(ROUTER_SMART_LONG_TAIL_RETRY_ABORT_THRESHOLD_MS, 100);
    // Default: -1 means this feature is not enabled.
    longTailRetryMaxRouteForMultiKeyReq = props.getInt(ROUTER_LONG_TAIL_RETRY_MAX_ROUTE_FOR_MULTI_KEYS_REQ, -1);
    maxKeyCountInMultiGetReq = props.getInt(ROUTER_MAX_KEY_COUNT_IN_MULTIGET_REQ, 500);
    connectionLimit = props.getInt(ROUTER_CONNECTION_LIMIT, 10000);
    httpClientPoolSize = props.getInt(ROUTER_HTTP_CLIENT_POOL_SIZE, 12);
    maxOutgoingConnPerRoute = props.getInt(ROUTER_MAX_OUTGOING_CONNECTION_PER_ROUTE, 120);
    maxOutgoingConn = props.getInt(ROUTER_MAX_OUTGOING_CONNECTION, 1200);
    clusterToD2Map = props.getMap(CLUSTER_TO_D2);
    stickyRoutingEnabledForSingleGet = props.getBoolean(ROUTER_ENABLE_STICKY_ROUTING_FOR_SINGLE_GET, true);
    stickyRoutingEnabledForMultiGet = props.getBoolean(ROUTER_ENABLE_STICKY_ROUTING_FOR_MULTI_GET, true);
    greedyMultiGet = props.getBoolean(ROUTER_GREEDY_MULTIGET, true); //TODO, if testing shows it isn't desirable, change this default
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
    cacheTTLmillis = props.getLong(ROUTER_CACHE_TTL_MILLIS, 0); // Any non-positive number will disable the cache TTL
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

    // This only needs to be enabled in some DC, where slow DNS lookup happens.
    dnsCacheEnabled = props.getBoolean(ROUTER_DNS_CACHE_ENABLED, false);
    hostPatternForDnsCache = props.getString(ROUTE_DNS_CACHE_HOST_PATTERN, ".*prod.linkedin.com");
    dnsCacheRefreshIntervalInMs = props.getLong(ROUTER_DNS_CACHE_REFRESH_INTERVAL_MS, TimeUnit.MINUTES.toMillis(3)); // 3 mins

    singleGetTardyLatencyThresholdMs = props.getLong(ROUTER_SINGLEGET_TARDY_LATENCY_MS, TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS));
    multiGetTardyLatencyThresholdMs = props.getLong(ROUTER_MULTIGET_TARDY_LATENCY_MS, TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS));
    computeTardyLatencyThresholdMs = props.getLong(ROUTER_COMPUTE_TARDY_LATENCY_MS, TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS));

    readThrottlingEnabled = props.getBoolean(ROUTER_ENABLE_READ_THROTTLING, true);
    maxPendingRequest = props.getLong(ROUTER_MAX_PENDING_REQUEST, 2500l * 12l);

    storageNodeClientType = StorageNodeClientType.valueOf(props.getString(ROUTER_STORAGE_NODE_CLIENT_TYPE, StorageNodeClientType.APACHE_HTTP_ASYNC_CLIENT.name())); // Use ApacheHttpAsynClient by default
    // TODO: what is the best setting? 5*NUMBER_OF_CORES?
    nettyClientEventLoopThreads = props.getInt(ROUTER_NETTY_CLIENT_EVENT_LOOP_THREADS, 24); // 24 threads by default
    nettyClientChannelPoolAcquireTimeoutMs = props.getLong(ROUTER_NETTY_CLIENT_CHANNEL_POOL_ACQUIRE_TIMEOUT_MS, 10); // 10ms by default
    nettyClientChannelPoolMinConnections = props.getInt(ROUTER_NETTY_CLIENT_CHANNEL_POOL_MIN_CONNECTIONS, 160); // 160 connections by default
    nettyClientChannelPoolMaxConnections = props.getInt(ROUTER_NETTY_CLIENT_CHANNEL_POOL_MAX_CONNECTIONS, 165); // 165 connections by default
    /**
     * ignore this config by default; only rely on the ROUTER_MAX_PENDING_REQUEST config for pending request throttling
      */
    nettyClientChannelPoolMaxPendingAcquires = props.getInt(ROUTER_NETTY_CLIENT_CHANNEL_POOL_MAX_PENDING_ACQUIRES, Integer.MAX_VALUE);
    /**
     * A channel will be closed if a request fails while using this channel, so there is no need to do frequent health check unless the system doesn't receive much traffic
     */
    nettyClientChannelPoolHealthCheckIntervalMs =
        props.getLong(ROUTER_NETTY_CLIENT_CHANNEL_POOL_HEALTH_CHECK_INTERVAL_MS, TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES));
    nettyClientMaxAggregatedObjectLength = props.getInt(ROUTER_NETTY_CLIENT_MAX_AGGREGATED_OBJECT_LENGTH, 1024 * 1024 * 20); // 20MB by default; change it according to the max response size
    decompressOnClient = props.getBoolean(ROUTER_CLIENT_DECOMPRESSION_ENABLED, true);
    streamingEnabled = props.getBoolean(ROUTER_STREAMING_ENABLED, false);
    computeFastAvroEnabled = props.getBoolean(ROUTER_COMPUTE_FAST_AVRO_ENABLED, false);

    socketTimeout = props.getInt(ROUTER_SOCKET_TIMEOUT, 5000); // 5s
    connectionTimeout = props.getInt(ROUTER_CONNECTION_TIMEOUT, 5000); // 5s

    statefulRouterHealthCheckEnabled = props.getBoolean(ROUTER_STATEFUL_HEALTHCHECK_ENABLED, false);
    routerUnhealthyPendingConnThresholdPerRoute = props.getInt(ROUTER_UNHEALTHY_PENDING_CONNECTION_THRESHOLD_PER_ROUTE, 500);
    routerPendingConnResumeThresholdPerRoute = props.getInt(ROUTER_PENDING_CONNECTION_RESUME_THRESHOLD_PER_ROUTE, 15);


    perNodeClientAllocationEnabled = props.getBoolean(ROUTER_PER_NODE_CLIENT_ENABLED, false);
    perNodeClientThreadCount = props.getInt(ROUTER_PER_NODE_CLIENT_THREAD_COUNT, 2);

    keyValueProfilingEnabled = props.getBoolean(KEY_VALUE_PROFILING_ENABLED, false);

    leakedFutureCleanupPollIntervalMs = props.getLong(ROUTER_LEAKED_FUTURE_CLEANUP_POLL_INTERVAL_MS, TimeUnit.MINUTES.toMillis(1));
    leakedFutureCleanupThresholdMs = props.getLong(ROUTER_LEAKED_FUTURE_CLEANUP_THRESHOLD_MS, TimeUnit.MINUTES.toMillis(1));

    idleConnectionToServerCleanupEnabled = props.getBoolean(ROUTER_IDLE_CONNECTION_TO_SERVER_CLEANUP_ENABLED, true);
    idleConnectionToServerCleanupThresholdMins = props.getLong(ROUTER_IDLE_CONNECTION_TO_SERVER_CLEANUP_THRESHOLD_MINS, TimeUnit.HOURS.toMinutes(3));

    fullPendingQueueServerOORMs = props.getLong(ROUTER_FULL_PENDING_QUEUE_SERVER_OOR_MS, TimeUnit.SECONDS.toMillis(0)); // No OOR
    httpasyncclientConnectionWarmingEnabled = props.getBoolean(ROUTER_HTTPASYNCCLIENT_CONNECTION_WARMING_ENABLED, false);
    httpasyncclientConnectionWarmingSleepIntervalMs = props.getLong(ROUTER_HTTPASYNCCLIENT_CONNECTION_WARMING_SLEEP_INTERVAL_MS, 100); // 100ms
    dictionaryRetrievalTimeMs = (int)props.getLong(ROUTER_DICTIONARY_RETRIEVAL_TIME_MS, TimeUnit.SECONDS.toMillis(30)); // 30 seconds
    routerDictionaryProcessingThreads = props.getInt(ROUTER_DICTIONARY_PROCESSING_THREADS, 2);
    httpasyncclientConnectionWarmingLowWaterMark = props.getInt(ROUTER_HTTPASYNCCLIENT_CONNECTION_WARMING_LOW_WATER_MARK, 60);
    httpasyncclientConnectionWarmingExecutorThreadNum = props.getInt(ROUTER_HTTPASYNCCLIENT_CONNECTION_WARMING_EXECUTOR_THREAD_NUM, 6); // 6 threads
    httpasyncclientConnectionWarmingNewInstanceDelayJoinMs = props.getLong(ROUTER_HTTPASYNCCLIENT_CONNECTION_WARMING_NEW_INSTANCE_DELAY_JOIN_MS, TimeUnit.MINUTES.toMillis(2)); // 2 mins
    asyncStartEnabled = props.getBoolean(ROUTER_ASYNC_START_ENABLED, false);

    maxRouterReadCapacityCu = props.getLong(ROUTER_MAX_READ_CAPACITY, 6000);
    routerQuotaCheckWindow = props.getLong(ROUTER_QUOTA_CHECK_WINDOW, 30000);
    earlyThrottleEnabled = props.getBoolean(ROUTER_EARLY_THROTTLE_ENABLED, false);
    helixCustomizedViewEnabled = props.getBoolean(HELIX_CUSTOMIZED_VIEW_ENABLED, false);
    ioThreadCountInPoolMode = props.getInt(ROUTER_HTTPASYNCCLIENT_CLIENT_POOL_THREAD_COUNT, Runtime.getRuntime().availableProcessors());
    leastLoadedHostSelectionEnabled = props.getBoolean(ROUTER_LEAST_LOADED_HOST_ENABLED, false);
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

  public boolean isGreedyMultiGet() {
    return greedyMultiGet;
  }

  public double getHeartbeatTimeoutMs() {
    return heartbeatTimeoutMs;
  }

  public long getHeartbeatCycleMs() {
    return heartbeatCycleMs;
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

  public TreeMap<Integer, Integer> getLongTailRetryForBatchGetThresholdMs() {
    return longTailRetryForBatchGetThresholdMs;
  }

  public long getCacheTTLmillis() {
    return cacheTTLmillis;
  }

  public boolean isDnsCacheEnabled() {
    return dnsCacheEnabled;
  }

  public String getHostPatternForDnsCache() {
    return hostPatternForDnsCache;
  }

  public long getDnsCacheRefreshIntervalInMs() {
    return dnsCacheRefreshIntervalInMs;
  }

  public long getSingleGetTardyLatencyThresholdMs() {
    return singleGetTardyLatencyThresholdMs;
  }

  public long getMultiGetTardyLatencyThresholdMs() {
    return multiGetTardyLatencyThresholdMs;
  }

  public long getComputeTardyLatencyThresholdMs() {
    return computeTardyLatencyThresholdMs;
  }

  public boolean isReadThrottlingEnabled() {
    return readThrottlingEnabled;
  }

  public long getMaxPendingRequest() {
    return maxPendingRequest;
  }

  public boolean isSmartLongTailRetryEnabled() {
    return smartLongTailRetryEnabled;
  }

  public int getSmartLongTailRetryAbortThresholdMs() {
    return smartLongTailRetryAbortThresholdMs;
  }

  public int getLongTailRetryMaxRouteForMultiKeyReq() {
    return longTailRetryMaxRouteForMultiKeyReq;
  }

  public StorageNodeClientType getStorageNodeClientType() {
    return storageNodeClientType;
  }

  public int getNettyClientEventLoopThreads() {
    return nettyClientEventLoopThreads;
  }

  public long getNettyClientChannelPoolAcquireTimeoutMs() {
    return nettyClientChannelPoolAcquireTimeoutMs;
  }

  public int getNettyClientChannelPoolMinConnections() {
    return nettyClientChannelPoolMinConnections;
  }

  public int getNettyClientChannelPoolMaxConnections() {
    return nettyClientChannelPoolMaxConnections;
  }

  public int getNettyClientChannelPoolMaxPendingAcquires() {
    return nettyClientChannelPoolMaxPendingAcquires;
  }

  public long getNettyClientChannelPoolHealthCheckIntervalMs() {
    return nettyClientChannelPoolHealthCheckIntervalMs;
  }

  public int getNettyClientMaxAggregatedObjectLength() {
    return nettyClientMaxAggregatedObjectLength;
  }

  public boolean isDecompressOnClient() {
    return decompressOnClient;
  }

  public boolean isStreamingEnabled() {
    return streamingEnabled;
  }

  public boolean isComputeFastAvroEnabled() {
    return computeFastAvroEnabled;
  }

  public int getSocketTimeout()  {
    return socketTimeout;
  }

  public int getConnectionTimeout() {
    return connectionTimeout;
  }

  public boolean isStatefulRouterHealthCheckEnabled() {
    return statefulRouterHealthCheckEnabled;
  }

  public int getRouterUnhealthyPendingConnThresholdPerRoute() {
    return routerUnhealthyPendingConnThresholdPerRoute;
  }

  public int getRouterPendingConnResumeThresholdPerRoute() {
    return routerPendingConnResumeThresholdPerRoute;
  }

  public boolean isPerNodeClientAllocationEnabled() {
    return perNodeClientAllocationEnabled;
  }

  public int getPerNodeClientThreadCount() {
    return perNodeClientThreadCount;
  }

  public boolean isKeyValueProfilingEnabled() {
    return keyValueProfilingEnabled;
  }

  public long getLeakedFutureCleanupPollIntervalMs() {
    return leakedFutureCleanupPollIntervalMs;
  }

  public long getLeakedFutureCleanupThresholdMs() {
    return leakedFutureCleanupThresholdMs;
  }

  public String getKafkaZkAddress() {
    return kafkaZkAddress;
  }

  public String getKafkaBootstrapServers() {
    return kafkaBootstrapServers;
  }

  public boolean isIdleConnectionToServerCleanupEnabled() {
    return idleConnectionToServerCleanupEnabled;
  }

  public long getIdleConnectionToServerCleanupThresholdMins() {
    return idleConnectionToServerCleanupThresholdMins;
  }

  public long getFullPendingQueueServerOORMs() {
    return fullPendingQueueServerOORMs;
  }

  public boolean isHttpasyncclientConnectionWarmingEnabled() {
    return httpasyncclientConnectionWarmingEnabled;
  }

  public long getHttpasyncclientConnectionWarmingSleepIntervalMs() {
    return httpasyncclientConnectionWarmingSleepIntervalMs;
  }

  public int getDictionaryRetrievalTimeMs() {
    return dictionaryRetrievalTimeMs;
  }

  public int getRouterDictionaryProcessingThreads() {
    return routerDictionaryProcessingThreads;
  }
  public int getHttpasyncclientConnectionWarmingLowWaterMark() {
    return httpasyncclientConnectionWarmingLowWaterMark;
  }

  public int getHttpasyncclientConnectionWarmingExecutorThreadNum() {
    return httpasyncclientConnectionWarmingExecutorThreadNum;
  }

  public long getHttpasyncclientConnectionWarmingNewInstanceDelayJoinMs() {
    return httpasyncclientConnectionWarmingNewInstanceDelayJoinMs;
  }

  public boolean isAsyncStartEnabled() {
    return asyncStartEnabled;
  }

  public boolean isLeastLoadedHostSelectionEnabled() {
    return leastLoadedHostSelectionEnabled;
  }

  public long getMaxRouterReadCapacityCu() {
    return maxRouterReadCapacityCu;
  }

  public long getRouterQuotaCheckWindow() {
    return routerQuotaCheckWindow;
  }

  public boolean isEarlyThrottleEnabled() {
    return earlyThrottleEnabled;
  }

  public boolean isHelixCustomizedViewEnabled() { return helixCustomizedViewEnabled; }

  public int getIoThreadCountInPoolMode() {
    return ioThreadCountInPoolMode;
  }

  /**
   * The expected config format is like the following:
   * "1-10:20,11-50:50,51-200:80,201-:1000"
   *
   * @param retryThresholdStr
   * @return
   */
  public static TreeMap<Integer, Integer> parseRetryThresholdForBatchGet(String retryThresholdStr) {
    final String retryThresholdListSeparator = ",\\s*";
    final String retryThresholdSeparator = ":\\s*";
    final String keyRangeSeparator = "-\\s*";
    String[] retryThresholds = retryThresholdStr.split(retryThresholdListSeparator);
    List<String> retryThresholdList = Arrays.asList(retryThresholds);
    // Sort by the lower bound of the key ranges.
    retryThresholdList.sort((range1, range2) -> {
      // Find the lower bound of the key ranges
      String keyRange1[] = range1.split(keyRangeSeparator);
      String keyRange2[] = range2.split(keyRangeSeparator);
      if (keyRange1.length != 2) {
        throw new VeniceException("Invalid single retry threshold config: " + range1 +
            ", which contains two parts separated by '" + keyRangeSeparator + "'");
      }
      if (keyRange2.length != 2) {
        throw new VeniceException("Invalid single retry threshold config: " + range2 +
            ", which should contain two parts separated by '" + keyRangeSeparator + "'");
      }
      return Integer.parseInt(keyRange1[0]) - Integer.parseInt(keyRange2[0]);
    });
    TreeMap<Integer, Integer> retryThresholdMap = new TreeMap<>();
    // Check whether the key ranges are continuous, and store the mapping if everything is good
    int previousUpperBound = 0;
    final int MAX_KEY_COUNT = Integer.MAX_VALUE;
    for (String singleRetryThreshold : retryThresholdList) {
      // parse the range and retry threshold
      String[] singleRetryThresholdParts = singleRetryThreshold.split(retryThresholdSeparator);
      if (singleRetryThresholdParts.length != 2) {
        throw new VeniceException("Invalid single retry threshold config: " + singleRetryThreshold + ", which"
            + " should contain two parts separated by '" + retryThresholdSeparator + "'");
      }
      Integer threshold = Integer.parseInt(singleRetryThresholdParts[1]);
      String[] keyCountRange = singleRetryThresholdParts[0].split(keyRangeSeparator);
      int upperBoundKeyCount = MAX_KEY_COUNT;
      if (keyCountRange.length > 2) {
        throw new VeniceException("Invalid single retry threshold config: " + singleRetryThreshold + ", which"
            + " should contain only lower bound and upper bound of key count range");
      }
      int lowerBoundKeyCount = Integer.parseInt(keyCountRange[0]);
      if (keyCountRange.length == 2) {
        upperBoundKeyCount = keyCountRange[1].isEmpty() ? MAX_KEY_COUNT : Integer.parseInt(keyCountRange[1]);
      }
      if (lowerBoundKeyCount < 0 || upperBoundKeyCount < 0 || lowerBoundKeyCount > upperBoundKeyCount) {
        throw new VeniceException("Invalid single retry threshold config: " + singleRetryThreshold);
      }
      if (lowerBoundKeyCount != previousUpperBound + 1) {
        throw new VeniceException("Current retry threshold config: " + retryThresholdStr +
            " is not continuous according to key count range");
      }
      retryThresholdMap.put(lowerBoundKeyCount, threshold);
      previousUpperBound = upperBoundKeyCount;
    }
    if (!retryThresholdMap.containsKey(1)) {
      throw new VeniceException("Retry threshold for batch-get: " + retryThresholdStr + " should be setup starting from 1");
    }
    if (previousUpperBound != MAX_KEY_COUNT) {
      throw new VeniceException(" Retry threshold for batch-get: " + retryThresholdStr + " doesn't cover unlimited key count");
    }

    return retryThresholdMap;
  }
}
