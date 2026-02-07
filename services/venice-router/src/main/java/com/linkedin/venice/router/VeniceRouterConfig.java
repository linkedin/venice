package com.linkedin.venice.router;

import static com.linkedin.venice.ConfigKeys.ACL_IN_MEMORY_CACHE_TTL_MS;
import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.CLUSTER_TO_D2;
import static com.linkedin.venice.ConfigKeys.CLUSTER_TO_SERVER_D2;
import static com.linkedin.venice.ConfigKeys.ENFORCE_SECURE_ROUTER;
import static com.linkedin.venice.ConfigKeys.HEARTBEAT_CYCLE;
import static com.linkedin.venice.ConfigKeys.HEARTBEAT_TIMEOUT;
import static com.linkedin.venice.ConfigKeys.HELIX_HYBRID_STORE_QUOTA_ENABLED;
import static com.linkedin.venice.ConfigKeys.IDENTITY_PARSER_CLASS;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.KAFKA_OVER_SSL;
import static com.linkedin.venice.ConfigKeys.KEY_VALUE_PROFILING_ENABLED;
import static com.linkedin.venice.ConfigKeys.LISTENER_HOSTNAME;
import static com.linkedin.venice.ConfigKeys.LISTENER_PORT;
import static com.linkedin.venice.ConfigKeys.LISTENER_SSL_PORT;
import static com.linkedin.venice.ConfigKeys.MAX_READ_CAPACITY;
import static com.linkedin.venice.ConfigKeys.NAME_REPOSITORY_MAX_ENTRY_COUNT;
import static com.linkedin.venice.ConfigKeys.REFRESH_ATTEMPTS_FOR_ZK_RECONNECT;
import static com.linkedin.venice.ConfigKeys.REFRESH_INTERVAL_FOR_ZK_RECONNECT_MS;
import static com.linkedin.venice.ConfigKeys.ROUTER_ASYNC_START_ENABLED;
import static com.linkedin.venice.ConfigKeys.ROUTER_CLIENT_DECOMPRESSION_ENABLED;
import static com.linkedin.venice.ConfigKeys.ROUTER_CLIENT_IP_SPOOFING_CHECK_ENABLED;
import static com.linkedin.venice.ConfigKeys.ROUTER_CLIENT_RESOLUTION_RETRY_ATTEMPTS;
import static com.linkedin.venice.ConfigKeys.ROUTER_CLIENT_RESOLUTION_RETRY_BACKOFF_MS;
import static com.linkedin.venice.ConfigKeys.ROUTER_CLIENT_SSL_HANDSHAKE_QUEUE_CAPACITY;
import static com.linkedin.venice.ConfigKeys.ROUTER_COMPUTE_TARDY_LATENCY_MS;
import static com.linkedin.venice.ConfigKeys.ROUTER_CONNECTION_HANDLE_MODE;
import static com.linkedin.venice.ConfigKeys.ROUTER_CONNECTION_LIMIT;
import static com.linkedin.venice.ConfigKeys.ROUTER_CONNECTION_TIMEOUT;
import static com.linkedin.venice.ConfigKeys.ROUTER_DICTIONARY_PROCESSING_THREADS;
import static com.linkedin.venice.ConfigKeys.ROUTER_DICTIONARY_RETRIEVAL_TIME_MS;
import static com.linkedin.venice.ConfigKeys.ROUTER_DNS_CACHE_ENABLED;
import static com.linkedin.venice.ConfigKeys.ROUTER_DNS_CACHE_REFRESH_INTERVAL_MS;
import static com.linkedin.venice.ConfigKeys.ROUTER_EARLY_THROTTLE_ENABLED;
import static com.linkedin.venice.ConfigKeys.ROUTER_ENABLE_READ_THROTTLING;
import static com.linkedin.venice.ConfigKeys.ROUTER_FULL_PENDING_QUEUE_SERVER_OOR_MS;
import static com.linkedin.venice.ConfigKeys.ROUTER_HEART_BEAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.ROUTER_HELIX_ASSISTED_ROUTING_GROUP_SELECTION_STRATEGY;
import static com.linkedin.venice.ConfigKeys.ROUTER_HTTP2_HEADER_TABLE_SIZE;
import static com.linkedin.venice.ConfigKeys.ROUTER_HTTP2_INBOUND_ENABLED;
import static com.linkedin.venice.ConfigKeys.ROUTER_HTTP2_INITIAL_WINDOW_SIZE;
import static com.linkedin.venice.ConfigKeys.ROUTER_HTTP2_MAX_CONCURRENT_STREAMS;
import static com.linkedin.venice.ConfigKeys.ROUTER_HTTP2_MAX_FRAME_SIZE;
import static com.linkedin.venice.ConfigKeys.ROUTER_HTTP2_MAX_HEADER_LIST_SIZE;
import static com.linkedin.venice.ConfigKeys.ROUTER_HTTPASYNCCLIENT_CLIENT_POOL_THREAD_COUNT;
import static com.linkedin.venice.ConfigKeys.ROUTER_HTTPASYNCCLIENT_CONNECTION_WARMING_ENABLED;
import static com.linkedin.venice.ConfigKeys.ROUTER_HTTPASYNCCLIENT_CONNECTION_WARMING_EXECUTOR_THREAD_NUM;
import static com.linkedin.venice.ConfigKeys.ROUTER_HTTPASYNCCLIENT_CONNECTION_WARMING_LOW_WATER_MARK;
import static com.linkedin.venice.ConfigKeys.ROUTER_HTTPASYNCCLIENT_CONNECTION_WARMING_NEW_INSTANCE_DELAY_JOIN_MS;
import static com.linkedin.venice.ConfigKeys.ROUTER_HTTPASYNCCLIENT_CONNECTION_WARMING_SLEEP_INTERVAL_MS;
import static com.linkedin.venice.ConfigKeys.ROUTER_HTTPAYSNCCLIENT_CONNECTION_WARMING_SOCKET_TIMEOUT_MS;
import static com.linkedin.venice.ConfigKeys.ROUTER_HTTP_CLIENT5_POOL_SIZE;
import static com.linkedin.venice.ConfigKeys.ROUTER_HTTP_CLIENT5_SKIP_CIPHER_CHECK_ENABLED;
import static com.linkedin.venice.ConfigKeys.ROUTER_HTTP_CLIENT5_TOTAL_IO_THREAD_COUNT;
import static com.linkedin.venice.ConfigKeys.ROUTER_HTTP_CLIENT_OPENSSL_ENABLED;
import static com.linkedin.venice.ConfigKeys.ROUTER_HTTP_CLIENT_POOL_SIZE;
import static com.linkedin.venice.ConfigKeys.ROUTER_IDLE_CONNECTION_TO_SERVER_CLEANUP_ENABLED;
import static com.linkedin.venice.ConfigKeys.ROUTER_IDLE_CONNECTION_TO_SERVER_CLEANUP_THRESHOLD_MINS;
import static com.linkedin.venice.ConfigKeys.ROUTER_IO_WORKER_COUNT;
import static com.linkedin.venice.ConfigKeys.ROUTER_LATENCY_BASED_ROUTING_ENABLED;
import static com.linkedin.venice.ConfigKeys.ROUTER_LEAKED_FUTURE_CLEANUP_POLL_INTERVAL_MS;
import static com.linkedin.venice.ConfigKeys.ROUTER_LEAKED_FUTURE_CLEANUP_THRESHOLD_MS;
import static com.linkedin.venice.ConfigKeys.ROUTER_LONG_TAIL_RETRY_BUDGET_ENFORCEMENT_WINDOW_MS;
import static com.linkedin.venice.ConfigKeys.ROUTER_LONG_TAIL_RETRY_FOR_BATCH_GET_THRESHOLD_MS;
import static com.linkedin.venice.ConfigKeys.ROUTER_LONG_TAIL_RETRY_FOR_SINGLE_GET_THRESHOLD_MS;
import static com.linkedin.venice.ConfigKeys.ROUTER_LONG_TAIL_RETRY_MAX_ROUTE_FOR_MULTI_KEYS_REQ;
import static com.linkedin.venice.ConfigKeys.ROUTER_MAX_CONCURRENT_SSL_HANDSHAKES;
import static com.linkedin.venice.ConfigKeys.ROUTER_MAX_KEY_COUNT_IN_MULTIGET_REQ;
import static com.linkedin.venice.ConfigKeys.ROUTER_MAX_OUTGOING_CONNECTION;
import static com.linkedin.venice.ConfigKeys.ROUTER_MAX_OUTGOING_CONNECTION_PER_ROUTE;
import static com.linkedin.venice.ConfigKeys.ROUTER_MAX_PENDING_REQUEST;
import static com.linkedin.venice.ConfigKeys.ROUTER_MAX_READ_CAPACITY;
import static com.linkedin.venice.ConfigKeys.ROUTER_META_STORE_SHADOW_READ_ENABLED;
import static com.linkedin.venice.ConfigKeys.ROUTER_MULTIGET_TARDY_LATENCY_MS;
import static com.linkedin.venice.ConfigKeys.ROUTER_MULTI_KEY_LONG_TAIL_RETRY_BUDGET_PERCENT_DECIMAL;
import static com.linkedin.venice.ConfigKeys.ROUTER_MULTI_KEY_ROUTING_STRATEGY;
import static com.linkedin.venice.ConfigKeys.ROUTER_NETTY_GRACEFUL_SHUTDOWN_PERIOD_SECONDS;
import static com.linkedin.venice.ConfigKeys.ROUTER_PARALLEL_ROUTING_CHUNK_SIZE;
import static com.linkedin.venice.ConfigKeys.ROUTER_PARALLEL_ROUTING_THREAD_POOL_SIZE;
import static com.linkedin.venice.ConfigKeys.ROUTER_PENDING_CONNECTION_RESUME_THRESHOLD_PER_ROUTE;
import static com.linkedin.venice.ConfigKeys.ROUTER_PER_NODE_CLIENT_ENABLED;
import static com.linkedin.venice.ConfigKeys.ROUTER_PER_NODE_CLIENT_THREAD_COUNT;
import static com.linkedin.venice.ConfigKeys.ROUTER_PER_STORE_ROUTER_QUOTA_BUFFER;
import static com.linkedin.venice.ConfigKeys.ROUTER_QUOTA_CHECK_WINDOW;
import static com.linkedin.venice.ConfigKeys.ROUTER_READ_QUOTA_THROTTLING_LEASE_TIMEOUT_MS;
import static com.linkedin.venice.ConfigKeys.ROUTER_RESOLVE_QUEUE_CAPACITY;
import static com.linkedin.venice.ConfigKeys.ROUTER_RESOLVE_THREADS;
import static com.linkedin.venice.ConfigKeys.ROUTER_RESPONSE_AGGREGATION_QUEUE_CAPACITY;
import static com.linkedin.venice.ConfigKeys.ROUTER_RESPONSE_AGGREGATION_THREAD_POOL_SIZE;
import static com.linkedin.venice.ConfigKeys.ROUTER_RETRY_MANAGER_CORE_POOL_SIZE;
import static com.linkedin.venice.ConfigKeys.ROUTER_ROUTING_COMPUTATION_MODE;
import static com.linkedin.venice.ConfigKeys.ROUTER_SINGLEGET_TARDY_LATENCY_MS;
import static com.linkedin.venice.ConfigKeys.ROUTER_SINGLE_KEY_LONG_TAIL_RETRY_BUDGET_PERCENT_DECIMAL;
import static com.linkedin.venice.ConfigKeys.ROUTER_SLOW_SCATTER_REQUEST_THRESHOLD_MS;
import static com.linkedin.venice.ConfigKeys.ROUTER_SMART_LONG_TAIL_RETRY_ABORT_THRESHOLD_MS;
import static com.linkedin.venice.ConfigKeys.ROUTER_SMART_LONG_TAIL_RETRY_ENABLED;
import static com.linkedin.venice.ConfigKeys.ROUTER_SOCKET_TIMEOUT;
import static com.linkedin.venice.ConfigKeys.ROUTER_STATEFUL_HEALTHCHECK_ENABLED;
import static com.linkedin.venice.ConfigKeys.ROUTER_STORAGE_NODE_CLIENT_TYPE;
import static com.linkedin.venice.ConfigKeys.ROUTER_UNHEALTHY_PENDING_CONNECTION_THRESHOLD_PER_ROUTE;
import static com.linkedin.venice.ConfigKeys.ROUTE_DNS_CACHE_HOST_PATTERN;
import static com.linkedin.venice.ConfigKeys.SSL_TO_KAFKA_LEGACY;
import static com.linkedin.venice.ConfigKeys.SSL_TO_STORAGE_NODES;
import static com.linkedin.venice.ConfigKeys.SYSTEM_SCHEMA_CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.UNREGISTER_METRIC_FOR_DELETED_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;
import static com.linkedin.venice.router.api.VeniceMultiKeyRoutingStrategy.LEAST_LOADED_ROUTING;
import static com.linkedin.venice.router.api.routing.helix.HelixGroupSelectionStrategyEnum.LEAST_LOADED;

import com.linkedin.alpini.netty4.handlers.ConnectionHandleMode;
import com.linkedin.venice.acl.VeniceComponent;
import com.linkedin.venice.authorization.DefaultIdentityParser;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.NameRepository;
import com.linkedin.venice.router.api.RoutingComputationMode;
import com.linkedin.venice.router.api.VeniceMultiKeyRoutingStrategy;
import com.linkedin.venice.router.api.routing.helix.HelixGroupSelectionStrategyEnum;
import com.linkedin.venice.router.httpclient.StorageNodeClientType;
import com.linkedin.venice.utils.BatchGetConfigUtils;
import com.linkedin.venice.utils.LogContext;
import com.linkedin.venice.utils.RegionUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Configuration for Venice Router.
 */
public class VeniceRouterConfig implements RouterRetryConfig {
  private static final Logger LOGGER = LogManager.getLogger(VeniceRouterConfig.class);

  // IMMUTABLE CONFIGS
  private final String regionName;
  private final String clusterName;
  private final String zkConnection;
  private final int port;
  private final String hostname;
  private final int sslPort;
  private final double heartbeatTimeoutMs;
  private final long heartbeatCycleMs;
  private final boolean sslToStorageNodes;
  private final long maxReadCapacityCu;
  private final int longTailRetryForSingleGetThresholdMs;
  private final TreeMap<Integer, Integer> longTailRetryForBatchGetThresholdMs;
  private final boolean smartLongTailRetryEnabled;
  private final int smartLongTailRetryAbortThresholdMs;
  private final int longTailRetryMaxRouteForMultiKeyReq;
  private final int maxKeyCountInMultiGetReq;
  private final int connectionLimit;
  private final ConnectionHandleMode connectionHandleMode;
  private final int httpClientPoolSize;
  private final int maxOutgoingConnPerRoute;
  private final int maxOutgoingConn;
  private final Map<String, String> clusterToD2Map;
  private final Map<String, String> clusterToServerD2Map;
  private final int refreshAttemptsForZkReconnect;
  private final long refreshIntervalForZkReconnectInMs;
  private final int routerNettyGracefulShutdownPeriodSeconds;
  private final int routerInFlightMetricWindowSeconds;
  private final boolean enforceSecureOnly;
  private final boolean dnsCacheEnabled;
  private final String hostPatternForDnsCache;
  private final long dnsCacheRefreshIntervalInMs;
  private final long singleGetTardyLatencyThresholdMs;
  private final long multiGetTardyLatencyThresholdMs;
  private final long computeTardyLatencyThresholdMs;
  private final long slowScatterRequestThresholdMs;
  private final long maxPendingRequest;
  private final StorageNodeClientType storageNodeClientType;
  private final boolean decompressOnClient;
  private final int socketTimeout;
  private final int connectionTimeout;
  private final boolean statefulRouterHealthCheckEnabled;
  private final boolean latencyBasedRoutingEnabled;
  private final int routerUnhealthyPendingConnThresholdPerRoute;
  private final int routerPendingConnResumeThresholdPerRoute;
  private final boolean perNodeClientAllocationEnabled;
  private final int perNodeClientThreadCount;
  private final boolean keyValueProfilingEnabled;
  private final long leakedFutureCleanupPollIntervalMs;
  private final long leakedFutureCleanupThresholdMs;
  private final String kafkaBootstrapServers;
  private final boolean sslToKafka;
  private final boolean idleConnectionToServerCleanupEnabled;
  private final long idleConnectionToServerCleanupThresholdMins;
  private final long fullPendingQueueServerOORMs;
  private final boolean httpasyncclientConnectionWarmingEnabled;
  private final long httpasyncclientConnectionWarmingSleepIntervalMs;
  private final int dictionaryRetrievalTimeMs;
  private final int routerDictionaryProcessingThreads;
  private final int httpasyncclientConnectionWarmingLowWaterMark;
  private final int httpasyncclientConnectionWarmingExecutorThreadNum;
  private final long httpasyncclientConnectionWarmingNewInstanceDelayJoinMs;
  private final int httpasyncclientConnectionWarmingSocketTimeoutMs;
  private final boolean asyncStartEnabled;
  private final long routerQuotaCheckWindow;
  private final long maxRouterReadCapacityCu;
  private final boolean helixHybridStoreQuotaEnabled;
  private final int ioThreadCountInPoolMode;
  private final VeniceMultiKeyRoutingStrategy multiKeyRoutingStrategy;
  private final HelixGroupSelectionStrategyEnum helixGroupSelectionStrategy;
  private final String systemSchemaClusterName;
  private final int maxConcurrentSslHandshakes;
  private final int resolveThreads;
  private final int resolveQueueCapacity;
  private final int clientResolutionRetryAttempts;
  private final long clientResolutionRetryBackoffMs;
  private final int clientSslHandshakeQueueCapacity;
  private final boolean clientIPSpoofingCheckEnabled;
  private final long readQuotaThrottlingLeaseTimeoutMs;
  private final boolean routerHeartBeatEnabled;
  private final int httpClient5PoolSize;
  private final int httpClient5TotalIOThreadCount;
  private final boolean httpClient5SkipCipherCheck;
  private final boolean http2InboundEnabled;
  private final int http2MaxConcurrentStreams;
  private final int http2MaxFrameSize;
  private final int http2InitialWindowSize;
  private final int http2HeaderTableSize;
  private final int http2MaxHeaderListSize;
  private final boolean metaStoreShadowReadEnabled;
  private final boolean unregisterMetricForDeletedStoreEnabled;
  private final int routerIOWorkerCount;
  private final double perStoreRouterQuotaBuffer;
  private final boolean httpClientOpensslEnabled;
  private final String identityParserClassName;
  private final double singleKeyLongTailRetryBudgetPercentDecimal;
  private final double multiKeyLongTailRetryBudgetPercentDecimal;
  private final long longTailRetryBudgetEnforcementWindowInMs;
  private final int retryManagerCorePoolSize;
  private final int nameRepoMaxEntryCount;
  private final int aclInMemoryCacheTTLMs;
  private final LogContext logContext;
  private final RoutingComputationMode routingComputationMode;
  private final int parallelRoutingThreadCount;
  private final int parallelRoutingChunkSize;
  private final int responseAggregationThreadPoolSize;
  private final int responseAggregationQueueCapacity;

  // MUTABLE CONFIGS

  private boolean readThrottlingEnabled;
  private boolean earlyThrottleEnabled;

  public VeniceRouterConfig(VeniceProperties props) {
    try {
      regionName = RegionUtils.getLocalRegionName(props, false);
      clusterName = props.getString(CLUSTER_NAME);
      port = props.getInt(LISTENER_PORT);
      hostname = props.getString(LISTENER_HOSTNAME, () -> Utils.getHostName());
      logContext = new LogContext.Builder().setComponentName(VeniceComponent.ROUTER.name())
          .setRegionName(regionName)
          .setInstanceName(Utils.getHelixNodeIdentifier(hostname, port))
          .build();
      sslPort = props.getInt(LISTENER_SSL_PORT);
      zkConnection = props.getString(ZOOKEEPER_ADDRESS);
      kafkaBootstrapServers = props.getString(KAFKA_BOOTSTRAP_SERVERS);
      sslToKafka = props.getBooleanWithAlternative(KAFKA_OVER_SSL, SSL_TO_KAFKA_LEGACY, false);
      heartbeatTimeoutMs = props.getDouble(HEARTBEAT_TIMEOUT, TimeUnit.MINUTES.toMillis(1));
      heartbeatCycleMs = props.getLong(HEARTBEAT_CYCLE, TimeUnit.SECONDS.toMillis(5));
      sslToStorageNodes = props.getBoolean(SSL_TO_STORAGE_NODES, false);
      maxReadCapacityCu = props.getLong(MAX_READ_CAPACITY, 100000);
      longTailRetryForSingleGetThresholdMs = props.getInt(ROUTER_LONG_TAIL_RETRY_FOR_SINGLE_GET_THRESHOLD_MS, 15);
      longTailRetryForBatchGetThresholdMs = BatchGetConfigUtils.parseRetryThresholdForBatchGet(
          props.getString(
              ROUTER_LONG_TAIL_RETRY_FOR_BATCH_GET_THRESHOLD_MS,
              "1-5:15,6-20:30,21-150:50,151-500:100,501-:500"));
      // Enable smart long tail retry by default
      smartLongTailRetryEnabled = props.getBoolean(ROUTER_SMART_LONG_TAIL_RETRY_ENABLED, true);
      smartLongTailRetryAbortThresholdMs = props.getInt(ROUTER_SMART_LONG_TAIL_RETRY_ABORT_THRESHOLD_MS, 100);
      // Default: -1 means this feature is not enabled.
      longTailRetryMaxRouteForMultiKeyReq = props.getInt(ROUTER_LONG_TAIL_RETRY_MAX_ROUTE_FOR_MULTI_KEYS_REQ, 2);
      maxKeyCountInMultiGetReq = props.getInt(ROUTER_MAX_KEY_COUNT_IN_MULTIGET_REQ, 500);
      connectionLimit = props.getInt(ROUTER_CONNECTION_LIMIT, 10000);
      // When connection limit is breached, fail fast to client request by default.
      connectionHandleMode = ConnectionHandleMode.valueOf(
          props
              .getString(ROUTER_CONNECTION_HANDLE_MODE, ConnectionHandleMode.FAIL_FAST_WHEN_LIMIT_EXCEEDED.toString()));
      httpClientPoolSize = props.getInt(ROUTER_HTTP_CLIENT_POOL_SIZE, 12);
      maxOutgoingConnPerRoute = props.getInt(ROUTER_MAX_OUTGOING_CONNECTION_PER_ROUTE, 120);
      maxOutgoingConn = props.getInt(ROUTER_MAX_OUTGOING_CONNECTION, 1200);
      clusterToD2Map = props.getMap(CLUSTER_TO_D2);
      clusterToServerD2Map = props.getMap(CLUSTER_TO_SERVER_D2, Collections.emptyMap());
      refreshAttemptsForZkReconnect = props.getInt(REFRESH_ATTEMPTS_FOR_ZK_RECONNECT, 9);
      refreshIntervalForZkReconnectInMs =
          props.getLong(REFRESH_INTERVAL_FOR_ZK_RECONNECT_MS, java.util.concurrent.TimeUnit.SECONDS.toMillis(10));
      routerNettyGracefulShutdownPeriodSeconds = props.getInt(ROUTER_NETTY_GRACEFUL_SHUTDOWN_PERIOD_SECONDS, 10);
      routerInFlightMetricWindowSeconds = props.getInt(ROUTER_NETTY_GRACEFUL_SHUTDOWN_PERIOD_SECONDS, 5);
      enforceSecureOnly = props.getBoolean(ENFORCE_SECURE_ROUTER, false);

      // This only needs to be enabled in some DC, where slow DNS lookup happens.
      dnsCacheEnabled = props.getBoolean(ROUTER_DNS_CACHE_ENABLED, false);
      hostPatternForDnsCache = props.getString(ROUTE_DNS_CACHE_HOST_PATTERN, ".*prod.linkedin.com");
      dnsCacheRefreshIntervalInMs = props.getLong(ROUTER_DNS_CACHE_REFRESH_INTERVAL_MS, TimeUnit.MINUTES.toMillis(3));

      singleGetTardyLatencyThresholdMs =
          props.getLong(ROUTER_SINGLEGET_TARDY_LATENCY_MS, TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS));
      multiGetTardyLatencyThresholdMs =
          props.getLong(ROUTER_MULTIGET_TARDY_LATENCY_MS, TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS));
      computeTardyLatencyThresholdMs =
          props.getLong(ROUTER_COMPUTE_TARDY_LATENCY_MS, TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS));
      slowScatterRequestThresholdMs = props.getLong(ROUTER_SLOW_SCATTER_REQUEST_THRESHOLD_MS, 1000);

      readThrottlingEnabled = props.getBoolean(ROUTER_ENABLE_READ_THROTTLING, true);
      maxPendingRequest = props.getLong(ROUTER_MAX_PENDING_REQUEST, 2500L * 12L);

      storageNodeClientType = StorageNodeClientType
          .valueOf(props.getString(ROUTER_STORAGE_NODE_CLIENT_TYPE, StorageNodeClientType.HTTP_CLIENT_5_CLIENT.name()));
      decompressOnClient = props.getBoolean(ROUTER_CLIENT_DECOMPRESSION_ENABLED, true);

      socketTimeout = props.getInt(ROUTER_SOCKET_TIMEOUT, 5000); // 5s
      connectionTimeout = props.getInt(ROUTER_CONNECTION_TIMEOUT, 5000); // 5s

      statefulRouterHealthCheckEnabled = props.getBoolean(ROUTER_STATEFUL_HEALTHCHECK_ENABLED, true);
      latencyBasedRoutingEnabled = props.getBoolean(ROUTER_LATENCY_BASED_ROUTING_ENABLED, false);
      routerUnhealthyPendingConnThresholdPerRoute =
          props.getInt(ROUTER_UNHEALTHY_PENDING_CONNECTION_THRESHOLD_PER_ROUTE, 100);
      routerPendingConnResumeThresholdPerRoute = props.getInt(ROUTER_PENDING_CONNECTION_RESUME_THRESHOLD_PER_ROUTE, 15);

      perNodeClientAllocationEnabled = props.getBoolean(ROUTER_PER_NODE_CLIENT_ENABLED, false);
      perNodeClientThreadCount = props.getInt(ROUTER_PER_NODE_CLIENT_THREAD_COUNT, 2);

      keyValueProfilingEnabled = props.getBoolean(KEY_VALUE_PROFILING_ENABLED, false);

      leakedFutureCleanupPollIntervalMs =
          props.getLong(ROUTER_LEAKED_FUTURE_CLEANUP_POLL_INTERVAL_MS, TimeUnit.MINUTES.toMillis(1));
      leakedFutureCleanupThresholdMs =
          props.getLong(ROUTER_LEAKED_FUTURE_CLEANUP_THRESHOLD_MS, TimeUnit.MINUTES.toMillis(1));

      idleConnectionToServerCleanupEnabled = props.getBoolean(ROUTER_IDLE_CONNECTION_TO_SERVER_CLEANUP_ENABLED, true);
      idleConnectionToServerCleanupThresholdMins =
          props.getLong(ROUTER_IDLE_CONNECTION_TO_SERVER_CLEANUP_THRESHOLD_MINS, TimeUnit.HOURS.toMinutes(3));
      fullPendingQueueServerOORMs =
          props.getLong(ROUTER_FULL_PENDING_QUEUE_SERVER_OOR_MS, TimeUnit.SECONDS.toMillis(0));
      httpasyncclientConnectionWarmingEnabled =
          props.getBoolean(ROUTER_HTTPASYNCCLIENT_CONNECTION_WARMING_ENABLED, false);
      httpasyncclientConnectionWarmingSleepIntervalMs =
          props.getLong(ROUTER_HTTPASYNCCLIENT_CONNECTION_WARMING_SLEEP_INTERVAL_MS, 100); // 100ms
      dictionaryRetrievalTimeMs =
          (int) props.getLong(ROUTER_DICTIONARY_RETRIEVAL_TIME_MS, TimeUnit.SECONDS.toMillis(30));
      routerDictionaryProcessingThreads = props.getInt(ROUTER_DICTIONARY_PROCESSING_THREADS, 3);
      httpasyncclientConnectionWarmingLowWaterMark =
          props.getInt(ROUTER_HTTPASYNCCLIENT_CONNECTION_WARMING_LOW_WATER_MARK, 60);
      httpasyncclientConnectionWarmingExecutorThreadNum =
          props.getInt(ROUTER_HTTPASYNCCLIENT_CONNECTION_WARMING_EXECUTOR_THREAD_NUM, 6); // 6 threads
      httpasyncclientConnectionWarmingNewInstanceDelayJoinMs = props
          .getLong(ROUTER_HTTPASYNCCLIENT_CONNECTION_WARMING_NEW_INSTANCE_DELAY_JOIN_MS, TimeUnit.MINUTES.toMillis(2));
      httpasyncclientConnectionWarmingSocketTimeoutMs =
          props.getInt(ROUTER_HTTPAYSNCCLIENT_CONNECTION_WARMING_SOCKET_TIMEOUT_MS, 5000);
      asyncStartEnabled = props.getBoolean(ROUTER_ASYNC_START_ENABLED, false);

      maxRouterReadCapacityCu = props.getLong(ROUTER_MAX_READ_CAPACITY, 6000);
      routerQuotaCheckWindow = props.getLong(ROUTER_QUOTA_CHECK_WINDOW, 30000);
      earlyThrottleEnabled = props.getBoolean(ROUTER_EARLY_THROTTLE_ENABLED, false);
      helixHybridStoreQuotaEnabled = props.getBoolean(HELIX_HYBRID_STORE_QUOTA_ENABLED, false);
      ioThreadCountInPoolMode =
          props.getInt(ROUTER_HTTPASYNCCLIENT_CLIENT_POOL_THREAD_COUNT, Runtime.getRuntime().availableProcessors());

      maxConcurrentSslHandshakes = props.getInt(ROUTER_MAX_CONCURRENT_SSL_HANDSHAKES, 1000);
      resolveThreads = props.getInt(ROUTER_RESOLVE_THREADS, 0);
      resolveQueueCapacity = props.getInt(ROUTER_RESOLVE_QUEUE_CAPACITY, 500000);
      clientResolutionRetryAttempts = props.getInt(ROUTER_CLIENT_RESOLUTION_RETRY_ATTEMPTS, 3);
      clientResolutionRetryBackoffMs = props.getLong(ROUTER_CLIENT_RESOLUTION_RETRY_BACKOFF_MS, 5 * Time.MS_PER_SECOND);
      clientSslHandshakeQueueCapacity = props.getInt(ROUTER_CLIENT_SSL_HANDSHAKE_QUEUE_CAPACITY, 500000);
      clientIPSpoofingCheckEnabled = props.getBoolean(ROUTER_CLIENT_IP_SPOOFING_CHECK_ENABLED, true);

      readQuotaThrottlingLeaseTimeoutMs =
          props.getLong(ROUTER_READ_QUOTA_THROTTLING_LEASE_TIMEOUT_MS, 6 * Time.MS_PER_HOUR);

      String multiKeyRoutingStrategyStr =
          props.getString(ROUTER_MULTI_KEY_ROUTING_STRATEGY, LEAST_LOADED_ROUTING.name());
      VeniceMultiKeyRoutingStrategy multiKeyRoutingStrategyEnum;
      try {
        multiKeyRoutingStrategyEnum = VeniceMultiKeyRoutingStrategy.valueOf(multiKeyRoutingStrategyStr);
      } catch (Exception e) {
        LOGGER.warn(
            "Invalid {} config: {}, and allowed values are: {}. Using default strategy {}",
            ROUTER_MULTI_KEY_ROUTING_STRATEGY,
            multiKeyRoutingStrategyStr,
            Arrays.toString(VeniceMultiKeyRoutingStrategy.values()),
            LEAST_LOADED_ROUTING.name());
        multiKeyRoutingStrategyEnum = LEAST_LOADED_ROUTING;
      }
      multiKeyRoutingStrategy = multiKeyRoutingStrategyEnum;
      String helixGroupSelectionStrategyStr =
          props.getString(ROUTER_HELIX_ASSISTED_ROUTING_GROUP_SELECTION_STRATEGY, LEAST_LOADED.name());
      try {
        helixGroupSelectionStrategy = HelixGroupSelectionStrategyEnum.valueOf(helixGroupSelectionStrategyStr);
      } catch (Exception e) {
        throw new VeniceException(
            "Invalid " + ROUTER_HELIX_ASSISTED_ROUTING_GROUP_SELECTION_STRATEGY + " config: "
                + helixGroupSelectionStrategyStr + ", and allowed values: "
                + Arrays.toString(HelixGroupSelectionStrategyEnum.values()));
      }
      systemSchemaClusterName = props.getString(SYSTEM_SCHEMA_CLUSTER_NAME, "");
      routerHeartBeatEnabled = props.getBoolean(ROUTER_HEART_BEAT_ENABLED, true);
      httpClient5PoolSize = props.getInt(ROUTER_HTTP_CLIENT5_POOL_SIZE, 1);
      httpClient5TotalIOThreadCount =
          props.getInt(ROUTER_HTTP_CLIENT5_TOTAL_IO_THREAD_COUNT, Runtime.getRuntime().availableProcessors());
      httpClient5SkipCipherCheck = props.getBoolean(ROUTER_HTTP_CLIENT5_SKIP_CIPHER_CHECK_ENABLED, false);
      http2InboundEnabled = props.getBoolean(ROUTER_HTTP2_INBOUND_ENABLED, false);
      http2MaxConcurrentStreams = props.getInt(ROUTER_HTTP2_MAX_CONCURRENT_STREAMS, 100);
      http2MaxFrameSize = props.getInt(ROUTER_HTTP2_MAX_FRAME_SIZE, 8 * 1024 * 1024);
      http2InitialWindowSize = props.getInt(ROUTER_HTTP2_INITIAL_WINDOW_SIZE, 8 * 1024 * 1024);
      http2HeaderTableSize = props.getInt(ROUTER_HTTP2_HEADER_TABLE_SIZE, 4096);
      http2MaxHeaderListSize = props.getInt(ROUTER_HTTP2_MAX_HEADER_LIST_SIZE, 8192);

      metaStoreShadowReadEnabled = props.getBoolean(ROUTER_META_STORE_SHADOW_READ_ENABLED, false);
      unregisterMetricForDeletedStoreEnabled = props.getBoolean(UNREGISTER_METRIC_FOR_DELETED_STORE_ENABLED, false);
      /**
       * This config is used to maintain the existing io thread count being used by Router, and we
       * should consider to use some number, which is proportional to the available cores.
       */
      routerIOWorkerCount = props.getInt(ROUTER_IO_WORKER_COUNT, 24);
      perStoreRouterQuotaBuffer = props.getDouble(ROUTER_PER_STORE_ROUTER_QUOTA_BUFFER, 1.5);
      httpClientOpensslEnabled = props.getBoolean(ROUTER_HTTP_CLIENT_OPENSSL_ENABLED, true);
      identityParserClassName = props.getString(IDENTITY_PARSER_CLASS, DefaultIdentityParser.class.getName());
      singleKeyLongTailRetryBudgetPercentDecimal =
          props.getDouble(ROUTER_SINGLE_KEY_LONG_TAIL_RETRY_BUDGET_PERCENT_DECIMAL, 0.0);
      multiKeyLongTailRetryBudgetPercentDecimal =
          props.getDouble(ROUTER_MULTI_KEY_LONG_TAIL_RETRY_BUDGET_PERCENT_DECIMAL, 0.0);
      longTailRetryBudgetEnforcementWindowInMs =
          props.getLong(ROUTER_LONG_TAIL_RETRY_BUDGET_ENFORCEMENT_WINDOW_MS, Time.MS_PER_MINUTE);
      retryManagerCorePoolSize = props.getInt(ROUTER_RETRY_MANAGER_CORE_POOL_SIZE, 5);
      this.nameRepoMaxEntryCount =
          props.getInt(NAME_REPOSITORY_MAX_ENTRY_COUNT, NameRepository.DEFAULT_MAXIMUM_ENTRY_COUNT);
      aclInMemoryCacheTTLMs = props.getInt(ACL_IN_MEMORY_CACHE_TTL_MS, -1); // acl caching is disabled by default

      String routingComputationModeStr =
          props.getString(ROUTER_ROUTING_COMPUTATION_MODE, RoutingComputationMode.SEQUENTIAL.name());
      try {
        routingComputationMode = RoutingComputationMode.valueOf(routingComputationModeStr);
      } catch (Exception e) {
        throw new VeniceException(
            "Invalid " + ROUTER_ROUTING_COMPUTATION_MODE + " config: " + routingComputationModeStr
                + ", and allowed values are: " + Arrays.toString(RoutingComputationMode.values()));
      }
      parallelRoutingThreadCount =
          props.getInt(ROUTER_PARALLEL_ROUTING_THREAD_POOL_SIZE, Runtime.getRuntime().availableProcessors());
      parallelRoutingChunkSize = props.getInt(ROUTER_PARALLEL_ROUTING_CHUNK_SIZE, 100);
      responseAggregationThreadPoolSize = props.getInt(ROUTER_RESPONSE_AGGREGATION_THREAD_POOL_SIZE, 10);
      responseAggregationQueueCapacity = props.getInt(ROUTER_RESPONSE_AGGREGATION_QUEUE_CAPACITY, 500000);
      LOGGER.info("Loaded configuration");
    } catch (Exception e) {
      String errorMessage = "Can not load properties.";
      LOGGER.error(errorMessage);
      throw new VeniceException(errorMessage, e);
    }
  }

  public double getPerStoreRouterQuotaBuffer() {
    return perStoreRouterQuotaBuffer;
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

  public String getHostname() {
    return hostname;
  }

  public int getSslPort() {
    return sslPort;
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

  public Map<String, String> getClusterToServerD2Map() {
    return clusterToServerD2Map;
  }

  public int getConnectionLimit() {
    return connectionLimit;
  }

  public ConnectionHandleMode getConnectionHandleMode() {
    return connectionHandleMode;
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

  public long getRefreshIntervalForZkReconnectInMs() {
    return refreshIntervalForZkReconnectInMs;
  }

  public int getRefreshAttemptsForZkReconnect() {
    return refreshAttemptsForZkReconnect;
  }

  public int getRouterNettyGracefulShutdownPeriodSeconds() {
    return routerNettyGracefulShutdownPeriodSeconds;
  }

  public int getRouterInFlightMetricWindowSeconds() {
    return routerInFlightMetricWindowSeconds;
  }

  public boolean isEnforcingSecureOnly() {
    return enforceSecureOnly;
  }

  public TreeMap<Integer, Integer> getLongTailRetryForBatchGetThresholdMs() {
    return longTailRetryForBatchGetThresholdMs;
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

  public long getSlowScatterRequestThresholdMs() {
    return slowScatterRequestThresholdMs;
  }

  public boolean isReadThrottlingEnabled() {
    return readThrottlingEnabled;
  }

  public void setReadThrottlingEnabled(boolean readThrottlingEnabled) {
    this.readThrottlingEnabled = readThrottlingEnabled;
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

  public boolean isDecompressOnClient() {
    return decompressOnClient;
  }

  public int getSocketTimeout() {
    return socketTimeout;
  }

  public int getConnectionTimeout() {
    return connectionTimeout;
  }

  public boolean isStatefulRouterHealthCheckEnabled() {
    return statefulRouterHealthCheckEnabled;
  }

  public boolean isLatencyBasedRoutingEnabled() {
    return latencyBasedRoutingEnabled;
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

  public String getKafkaBootstrapServers() {
    return kafkaBootstrapServers;
  }

  public boolean isSslToKafka() {
    return sslToKafka;
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

  public int getHttpasyncclientConnectionWarmingSocketTimeoutMs() {
    return httpasyncclientConnectionWarmingSocketTimeoutMs;
  }

  public boolean isAsyncStartEnabled() {
    return asyncStartEnabled;
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

  public void setEarlyThrottleEnabled(boolean earlyThrottleEnabled) {
    this.earlyThrottleEnabled = earlyThrottleEnabled;
  }

  public boolean isHelixHybridStoreQuotaEnabled() {
    return helixHybridStoreQuotaEnabled;
  }

  public int getIoThreadCountInPoolMode() {
    return ioThreadCountInPoolMode;
  }

  public VeniceMultiKeyRoutingStrategy getMultiKeyRoutingStrategy() {
    return multiKeyRoutingStrategy;
  }

  public HelixGroupSelectionStrategyEnum getHelixGroupSelectionStrategy() {
    return helixGroupSelectionStrategy;
  }

  public String getSystemSchemaClusterName() {
    return systemSchemaClusterName;
  }

  public int getResolveThreads() {
    return resolveThreads;
  }

  public int getResolveQueueCapacity() {
    return resolveQueueCapacity;
  }

  public int getMaxConcurrentSslHandshakes() {
    return maxConcurrentSslHandshakes;
  }

  public int getClientResolutionRetryAttempts() {
    return clientResolutionRetryAttempts;
  }

  public long getClientResolutionRetryBackoffMs() {
    return clientResolutionRetryBackoffMs;
  }

  public int getClientSslHandshakeQueueCapacity() {
    return clientSslHandshakeQueueCapacity;
  }

  public boolean isClientIPSpoofingCheckEnabled() {
    return clientIPSpoofingCheckEnabled;
  }

  public long getReadQuotaThrottlingLeaseTimeoutMs() {
    return readQuotaThrottlingLeaseTimeoutMs;
  }

  public boolean isRouterHeartBeatEnabled() {
    return routerHeartBeatEnabled;
  }

  public int getHttpClient5PoolSize() {
    return httpClient5PoolSize;
  }

  public int getHttpClient5TotalIOThreadCount() {
    return httpClient5TotalIOThreadCount;
  }

  public boolean isHttpClient5SkipCipherCheck() {
    return httpClient5SkipCipherCheck;
  }

  public boolean isHttp2InboundEnabled() {
    return http2InboundEnabled;
  }

  public int getHttp2MaxConcurrentStreams() {
    return http2MaxConcurrentStreams;
  }

  public int getHttp2MaxFrameSize() {
    return http2MaxFrameSize;
  }

  public int getHttp2InitialWindowSize() {
    return http2InitialWindowSize;
  }

  public int getHttp2HeaderTableSize() {
    return http2HeaderTableSize;
  }

  public int getHttp2MaxHeaderListSize() {
    return http2MaxHeaderListSize;
  }

  public boolean isMetaStoreShadowReadEnabled() {
    return metaStoreShadowReadEnabled;
  }

  public boolean isUnregisterMetricForDeletedStoreEnabled() {
    return unregisterMetricForDeletedStoreEnabled;
  }

  public int getRouterIOWorkerCount() {
    return routerIOWorkerCount;
  }

  public boolean isHttpClientOpensslEnabled() {
    return httpClientOpensslEnabled;
  }

  public String getIdentityParserClassName() {
    return identityParserClassName;
  }

  public double getSingleKeyLongTailRetryBudgetPercentDecimal() {
    return singleKeyLongTailRetryBudgetPercentDecimal;
  }

  public double getMultiKeyLongTailRetryBudgetPercentDecimal() {
    return multiKeyLongTailRetryBudgetPercentDecimal;
  }

  public long getLongTailRetryBudgetEnforcementWindowInMs() {
    return longTailRetryBudgetEnforcementWindowInMs;
  }

  public int getRetryManagerCorePoolSize() {
    return retryManagerCorePoolSize;
  }

  public int getNameRepoMaxEntryCount() {
    return this.nameRepoMaxEntryCount;
  }

  public int getAclInMemoryCacheTTLMs() {
    return aclInMemoryCacheTTLMs;
  }

  public String getRegionName() {
    return regionName;
  }

  public LogContext getLogContext() {
    return logContext;
  }

  public RoutingComputationMode getRoutingComputationMode() {
    return routingComputationMode;
  }

  public int getParallelRoutingThreadCount() {
    return parallelRoutingThreadCount;
  }

  public int getParallelRoutingChunkSize() {
    return parallelRoutingChunkSize;
  }

  public int getResponseAggregationThreadPoolSize() {
    return responseAggregationThreadPoolSize;
  }

  public int getResponseAggregationQueueCapacity() {
    return responseAggregationQueueCapacity;
  }
}
