package com.linkedin.venice.router;

import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.CLUSTER_TO_D2;
import static com.linkedin.venice.ConfigKeys.CLUSTER_TO_SERVER_D2;
import static com.linkedin.venice.ConfigKeys.ENFORCE_SECURE_ROUTER;
import static com.linkedin.venice.ConfigKeys.HEARTBEAT_CYCLE;
import static com.linkedin.venice.ConfigKeys.HEARTBEAT_TIMEOUT;
import static com.linkedin.venice.ConfigKeys.HELIX_HYBRID_STORE_QUOTA_ENABLED;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.KAFKA_OVER_SSL;
import static com.linkedin.venice.ConfigKeys.KEY_VALUE_PROFILING_ENABLED;
import static com.linkedin.venice.ConfigKeys.LISTENER_HOSTNAME;
import static com.linkedin.venice.ConfigKeys.LISTENER_PORT;
import static com.linkedin.venice.ConfigKeys.LISTENER_SSL_PORT;
import static com.linkedin.venice.ConfigKeys.MAX_READ_CAPACITY;
import static com.linkedin.venice.ConfigKeys.REFRESH_ATTEMPTS_FOR_ZK_RECONNECT;
import static com.linkedin.venice.ConfigKeys.REFRESH_INTERVAL_FOR_ZK_RECONNECT_MS;
import static com.linkedin.venice.ConfigKeys.ROUTER_ASYNC_START_ENABLED;
import static com.linkedin.venice.ConfigKeys.ROUTER_CLIENT_DECOMPRESSION_ENABLED;
import static com.linkedin.venice.ConfigKeys.ROUTER_CLIENT_SSL_HANDSHAKE_ATTEMPTS;
import static com.linkedin.venice.ConfigKeys.ROUTER_CLIENT_SSL_HANDSHAKE_BACKOFF_MS;
import static com.linkedin.venice.ConfigKeys.ROUTER_CLIENT_SSL_HANDSHAKE_THREADS;
import static com.linkedin.venice.ConfigKeys.ROUTER_COMPUTE_FAST_AVRO_ENABLED;
import static com.linkedin.venice.ConfigKeys.ROUTER_COMPUTE_TARDY_LATENCY_MS;
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
import static com.linkedin.venice.ConfigKeys.ROUTER_HELIX_VIRTUAL_GROUP_FIELD_IN_DOMAIN;
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
import static com.linkedin.venice.ConfigKeys.ROUTER_LEAKED_FUTURE_CLEANUP_POLL_INTERVAL_MS;
import static com.linkedin.venice.ConfigKeys.ROUTER_LEAKED_FUTURE_CLEANUP_THRESHOLD_MS;
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
import static com.linkedin.venice.ConfigKeys.ROUTER_MULTI_KEY_ROUTING_STRATEGY;
import static com.linkedin.venice.ConfigKeys.ROUTER_NETTY_GRACEFUL_SHUTDOWN_PERIOD_SECONDS;
import static com.linkedin.venice.ConfigKeys.ROUTER_PENDING_CONNECTION_RESUME_THRESHOLD_PER_ROUTE;
import static com.linkedin.venice.ConfigKeys.ROUTER_PER_NODE_CLIENT_ENABLED;
import static com.linkedin.venice.ConfigKeys.ROUTER_PER_NODE_CLIENT_THREAD_COUNT;
import static com.linkedin.venice.ConfigKeys.ROUTER_PER_STORAGE_NODE_READ_QUOTA_BUFFER;
import static com.linkedin.venice.ConfigKeys.ROUTER_PER_STORAGE_NODE_THROTTLER_ENABLED;
import static com.linkedin.venice.ConfigKeys.ROUTER_PER_STORE_ROUTER_QUOTA_BUFFER;
import static com.linkedin.venice.ConfigKeys.ROUTER_QUOTA_CHECK_WINDOW;
import static com.linkedin.venice.ConfigKeys.ROUTER_READ_QUOTA_THROTTLING_LEASE_TIMEOUT_MS;
import static com.linkedin.venice.ConfigKeys.ROUTER_SINGLEGET_TARDY_LATENCY_MS;
import static com.linkedin.venice.ConfigKeys.ROUTER_SMART_LONG_TAIL_RETRY_ABORT_THRESHOLD_MS;
import static com.linkedin.venice.ConfigKeys.ROUTER_SMART_LONG_TAIL_RETRY_ENABLED;
import static com.linkedin.venice.ConfigKeys.ROUTER_SOCKET_TIMEOUT;
import static com.linkedin.venice.ConfigKeys.ROUTER_STATEFUL_HEALTHCHECK_ENABLED;
import static com.linkedin.venice.ConfigKeys.ROUTER_STORAGE_NODE_CLIENT_TYPE;
import static com.linkedin.venice.ConfigKeys.ROUTER_THROTTLE_CLIENT_SSL_HANDSHAKES;
import static com.linkedin.venice.ConfigKeys.ROUTER_UNHEALTHY_PENDING_CONNECTION_THRESHOLD_PER_ROUTE;
import static com.linkedin.venice.ConfigKeys.ROUTE_DNS_CACHE_HOST_PATTERN;
import static com.linkedin.venice.ConfigKeys.SSL_TO_KAFKA_LEGACY;
import static com.linkedin.venice.ConfigKeys.SSL_TO_STORAGE_NODES;
import static com.linkedin.venice.ConfigKeys.SYSTEM_SCHEMA_CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.UNREGISTER_METRIC_FOR_DELETED_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;
import static com.linkedin.venice.helix.HelixInstanceConfigRepository.GROUP_FIELD_NAME_IN_DOMAIN;
import static com.linkedin.venice.helix.HelixInstanceConfigRepository.ZONE_FIELD_NAME_IN_DOMAIN;
import static com.linkedin.venice.router.api.VeniceMultiKeyRoutingStrategy.LEAST_LOADED_ROUTING;
import static com.linkedin.venice.router.api.routing.helix.HelixGroupSelectionStrategyEnum.LEAST_LOADED;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.router.api.VeniceMultiKeyRoutingStrategy;
import com.linkedin.venice.router.api.routing.helix.HelixGroupSelectionStrategyEnum;
import com.linkedin.venice.router.httpclient.StorageNodeClientType;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Configuration for Venice Router.
 */
public class VeniceRouterConfig {
  private static final Logger LOGGER = LogManager.getLogger(VeniceRouterConfig.class);

  private String clusterName;
  private String zkConnection;
  private int port;
  private String hostname;
  private int sslPort;
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
  private Map<String, String> clusterToServerD2Map;
  private double perStorageNodeReadQuotaBuffer;
  private int refreshAttemptsForZkReconnect;
  private long refreshIntervalForZkReconnectInMs;
  private int routerNettyGracefulShutdownPeriodSeconds;
  private boolean enforceSecureOnly;
  private boolean dnsCacheEnabled;
  private String hostPatternForDnsCache;
  private long dnsCacheRefreshIntervalInMs;
  private long singleGetTardyLatencyThresholdMs;
  private long multiGetTardyLatencyThresholdMs;
  private long computeTardyLatencyThresholdMs;
  private boolean readThrottlingEnabled;
  private long maxPendingRequest;
  private StorageNodeClientType storageNodeClientType;
  private boolean decompressOnClient;
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
  private String kafkaBootstrapServers;
  private boolean sslToKafka;
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
  private int httpasyncclientConnectionWarmingSocketTimeoutMs;
  private boolean asyncStartEnabled;
  private boolean earlyThrottleEnabled;
  private long routerQuotaCheckWindow;
  private long maxRouterReadCapacityCu;
  private boolean helixHybridStoreQuotaEnabled;
  private int ioThreadCountInPoolMode;
  private boolean useGroupFieldInHelixDomain;
  private VeniceMultiKeyRoutingStrategy multiKeyRoutingStrategy;
  private HelixGroupSelectionStrategyEnum helixGroupSelectionStrategy;
  private String systemSchemaClusterName;
  private boolean throttleClientSslHandshakes;
  private int clientSslHandshakeThreads;
  private int maxConcurrentClientSslHandshakes;
  private int clientSslHandshakeAttempts;
  private long clientSslHandshakeBackoffMs;
  private long readQuotaThrottlingLeaseTimeoutMs;
  private boolean routerHeartBeatEnabled;
  private int httpClient5PoolSize;
  private int httpClient5TotalIOThreadCount;
  private boolean httpClient5SkipCipherCheck;
  private boolean http2InboundEnabled;
  private int http2MaxConcurrentStreams;
  private int http2MaxFrameSize;
  private int http2InitialWindowSize;
  private int http2HeaderTableSize;
  private int http2MaxHeaderListSize;
  private boolean metaStoreShadowReadEnabled;
  private boolean unregisterMetricForDeletedStoreEnabled;
  private int routerIOWorkerCount;
  private boolean perRouterStorageNodeThrottlerEnabled;
  private double perStoreRouterQuotaBuffer;
  private boolean httpClientOpensslEnabled;

  public VeniceRouterConfig(VeniceProperties props) {
    try {
      checkProperties(props);
      LOGGER.info("Loaded configuration");
    } catch (Exception e) {
      String errorMessage = "Can not load properties.";
      LOGGER.error(errorMessage);
      throw new VeniceException(errorMessage, e);
    }
  }

  private void checkProperties(VeniceProperties props) {
    clusterName = props.getString(CLUSTER_NAME);
    port = props.getInt(LISTENER_PORT);
    hostname = props.getString(LISTENER_HOSTNAME, () -> Utils.getHostName());
    sslPort = props.getInt(LISTENER_SSL_PORT);
    zkConnection = props.getString(ZOOKEEPER_ADDRESS);
    kafkaBootstrapServers = props.getString(KAFKA_BOOTSTRAP_SERVERS);
    sslToKafka = props.getBooleanWithAlternative(KAFKA_OVER_SSL, SSL_TO_KAFKA_LEGACY, false);
    heartbeatTimeoutMs = props.getDouble(HEARTBEAT_TIMEOUT, TimeUnit.MINUTES.toMillis(1));
    heartbeatCycleMs = props.getLong(HEARTBEAT_CYCLE, TimeUnit.SECONDS.toMillis(5));
    sslToStorageNodes = props.getBoolean(SSL_TO_STORAGE_NODES, false);
    maxReadCapacityCu = props.getLong(MAX_READ_CAPACITY, 100000);
    longTailRetryForSingleGetThresholdMs = props.getInt(ROUTER_LONG_TAIL_RETRY_FOR_SINGLE_GET_THRESHOLD_MS, 15);
    longTailRetryForBatchGetThresholdMs = parseRetryThresholdForBatchGet(
        props.getString(
            ROUTER_LONG_TAIL_RETRY_FOR_BATCH_GET_THRESHOLD_MS,
            "1-5:15,6-20:30,21-150:50,151-500:100,501-:500"));
    // Enable smart long tail retry by default
    smartLongTailRetryEnabled = props.getBoolean(ROUTER_SMART_LONG_TAIL_RETRY_ENABLED, true);
    smartLongTailRetryAbortThresholdMs = props.getInt(ROUTER_SMART_LONG_TAIL_RETRY_ABORT_THRESHOLD_MS, 100);
    // Default: -1 means this feature is not enabled.
    longTailRetryMaxRouteForMultiKeyReq = props.getInt(ROUTER_LONG_TAIL_RETRY_MAX_ROUTE_FOR_MULTI_KEYS_REQ, -1);
    maxKeyCountInMultiGetReq = props.getInt(ROUTER_MAX_KEY_COUNT_IN_MULTIGET_REQ, 500);
    connectionLimit = props.getInt(ROUTER_CONNECTION_LIMIT, 10000);
    httpClientPoolSize = props.getInt(ROUTER_HTTP_CLIENT_POOL_SIZE, 12);
    maxOutgoingConnPerRoute = props.getInt(ROUTER_MAX_OUTGOING_CONNECTION_PER_ROUTE, 120);
    maxOutgoingConn = props.getInt(ROUTER_MAX_OUTGOING_CONNECTION, 1200);
    clusterToD2Map = props.getMap(CLUSTER_TO_D2);
    clusterToServerD2Map = props.getMap(CLUSTER_TO_SERVER_D2, Collections.emptyMap());
    perStorageNodeReadQuotaBuffer = props.getDouble(ROUTER_PER_STORAGE_NODE_READ_QUOTA_BUFFER, 1.0);
    refreshAttemptsForZkReconnect = props.getInt(REFRESH_ATTEMPTS_FOR_ZK_RECONNECT, 3);
    refreshIntervalForZkReconnectInMs =
        props.getLong(REFRESH_INTERVAL_FOR_ZK_RECONNECT_MS, java.util.concurrent.TimeUnit.SECONDS.toMillis(10));
    routerNettyGracefulShutdownPeriodSeconds = props.getInt(ROUTER_NETTY_GRACEFUL_SHUTDOWN_PERIOD_SECONDS, 30);
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

    readThrottlingEnabled = props.getBoolean(ROUTER_ENABLE_READ_THROTTLING, true);
    maxPendingRequest = props.getLong(ROUTER_MAX_PENDING_REQUEST, 2500L * 12L);

    storageNodeClientType = StorageNodeClientType
        .valueOf(props.getString(ROUTER_STORAGE_NODE_CLIENT_TYPE, StorageNodeClientType.HTTP_CLIENT_5_CLIENT.name()));
    decompressOnClient = props.getBoolean(ROUTER_CLIENT_DECOMPRESSION_ENABLED, true);
    computeFastAvroEnabled = props.getBoolean(ROUTER_COMPUTE_FAST_AVRO_ENABLED, false);

    socketTimeout = props.getInt(ROUTER_SOCKET_TIMEOUT, 5000); // 5s
    connectionTimeout = props.getInt(ROUTER_CONNECTION_TIMEOUT, 5000); // 5s

    statefulRouterHealthCheckEnabled = props.getBoolean(ROUTER_STATEFUL_HEALTHCHECK_ENABLED, true);
    routerUnhealthyPendingConnThresholdPerRoute =
        props.getInt(ROUTER_UNHEALTHY_PENDING_CONNECTION_THRESHOLD_PER_ROUTE, 500);
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
    fullPendingQueueServerOORMs = props.getLong(ROUTER_FULL_PENDING_QUEUE_SERVER_OOR_MS, TimeUnit.SECONDS.toMillis(0));
    httpasyncclientConnectionWarmingEnabled =
        props.getBoolean(ROUTER_HTTPASYNCCLIENT_CONNECTION_WARMING_ENABLED, false);
    httpasyncclientConnectionWarmingSleepIntervalMs =
        props.getLong(ROUTER_HTTPASYNCCLIENT_CONNECTION_WARMING_SLEEP_INTERVAL_MS, 100); // 100ms
    dictionaryRetrievalTimeMs = (int) props.getLong(ROUTER_DICTIONARY_RETRIEVAL_TIME_MS, TimeUnit.SECONDS.toMillis(30));
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

    throttleClientSslHandshakes = props.getBoolean(ROUTER_THROTTLE_CLIENT_SSL_HANDSHAKES, false);
    clientSslHandshakeThreads = props.getInt(ROUTER_CLIENT_SSL_HANDSHAKE_THREADS, 4);
    maxConcurrentClientSslHandshakes = props.getInt(ROUTER_MAX_CONCURRENT_SSL_HANDSHAKES, 100);
    clientSslHandshakeAttempts = props.getInt(ROUTER_CLIENT_SSL_HANDSHAKE_ATTEMPTS, 5);
    clientSslHandshakeBackoffMs = props.getLong(ROUTER_CLIENT_SSL_HANDSHAKE_BACKOFF_MS, 5 * Time.MS_PER_SECOND);

    readQuotaThrottlingLeaseTimeoutMs =
        props.getLong(ROUTER_READ_QUOTA_THROTTLING_LEASE_TIMEOUT_MS, 6 * Time.MS_PER_HOUR);

    String helixVirtualGroupFieldNameInDomain =
        props.getString(ROUTER_HELIX_VIRTUAL_GROUP_FIELD_IN_DOMAIN, GROUP_FIELD_NAME_IN_DOMAIN);
    if (helixVirtualGroupFieldNameInDomain.equals(GROUP_FIELD_NAME_IN_DOMAIN)) {
      useGroupFieldInHelixDomain = true;
    } else if (helixVirtualGroupFieldNameInDomain.equals(ZONE_FIELD_NAME_IN_DOMAIN)) {
      useGroupFieldInHelixDomain = false;
    } else {
      throw new VeniceException(
          "Unknown value: " + helixVirtualGroupFieldNameInDomain + " for config: "
              + ROUTER_HELIX_VIRTUAL_GROUP_FIELD_IN_DOMAIN + ", and " + "allowed values: [" + GROUP_FIELD_NAME_IN_DOMAIN
              + ", " + ZONE_FIELD_NAME_IN_DOMAIN + "]");
    }
    String multiKeyRoutingStrategyStr = props.getString(ROUTER_MULTI_KEY_ROUTING_STRATEGY, LEAST_LOADED_ROUTING.name());
    try {
      multiKeyRoutingStrategy = VeniceMultiKeyRoutingStrategy.valueOf(multiKeyRoutingStrategyStr);
    } catch (Exception e) {
      LOGGER.warn(
          "Invalid {} config: {}, and allowed values are: {}. Using default strategy {}",
          ROUTER_MULTI_KEY_ROUTING_STRATEGY,
          multiKeyRoutingStrategyStr,
          Arrays.toString(VeniceMultiKeyRoutingStrategy.values()),
          LEAST_LOADED_ROUTING.name());
      multiKeyRoutingStrategy = LEAST_LOADED_ROUTING;
    }
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
    perRouterStorageNodeThrottlerEnabled = props.getBoolean(ROUTER_PER_STORAGE_NODE_THROTTLER_ENABLED, true);
    perStoreRouterQuotaBuffer = props.getDouble(ROUTER_PER_STORE_ROUTER_QUOTA_BUFFER, 1.5);
    httpClientOpensslEnabled = props.getBoolean(ROUTER_HTTP_CLIENT_OPENSSL_ENABLED, true);
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

  public int getRouterNettyGracefulShutdownPeriodSeconds() {
    return routerNettyGracefulShutdownPeriodSeconds;
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

  public boolean isComputeFastAvroEnabled() {
    return computeFastAvroEnabled;
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

  public boolean isUseGroupFieldInHelixDomain() {
    return useGroupFieldInHelixDomain;
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

  /**
   * The expected config format is like the following:
   * "1-10:20,11-50:50,51-200:80,201-:1000"
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
      String[] keyRange1 = range1.split(keyRangeSeparator);
      String[] keyRange2 = range2.split(keyRangeSeparator);
      if (keyRange1.length != 2) {
        throw new VeniceException(
            "Invalid single retry threshold config: " + range1 + ", which contains two parts separated by '"
                + keyRangeSeparator + "'");
      }
      if (keyRange2.length != 2) {
        throw new VeniceException(
            "Invalid single retry threshold config: " + range2 + ", which should contain two parts separated by '"
                + keyRangeSeparator + "'");
      }
      return Integer.parseInt(keyRange1[0]) - Integer.parseInt(keyRange2[0]);
    });
    TreeMap<Integer, Integer> retryThresholdMap = new TreeMap<>();
    // Check whether the key ranges are continuous, and store the mapping if everything is good
    int previousUpperBound = 0;
    final int MAX_KEY_COUNT = Integer.MAX_VALUE;
    for (String singleRetryThreshold: retryThresholdList) {
      // parse the range and retry threshold
      String[] singleRetryThresholdParts = singleRetryThreshold.split(retryThresholdSeparator);
      if (singleRetryThresholdParts.length != 2) {
        throw new VeniceException(
            "Invalid single retry threshold config: " + singleRetryThreshold + ", which"
                + " should contain two parts separated by '" + retryThresholdSeparator + "'");
      }
      Integer threshold = Integer.parseInt(singleRetryThresholdParts[1]);
      String[] keyCountRange = singleRetryThresholdParts[0].split(keyRangeSeparator);
      int upperBoundKeyCount = MAX_KEY_COUNT;
      if (keyCountRange.length > 2) {
        throw new VeniceException(
            "Invalid single retry threshold config: " + singleRetryThreshold + ", which"
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
        throw new VeniceException(
            "Current retry threshold config: " + retryThresholdStr + " is not continuous according to key count range");
      }
      retryThresholdMap.put(lowerBoundKeyCount, threshold);
      previousUpperBound = upperBoundKeyCount;
    }
    if (!retryThresholdMap.containsKey(1)) {
      throw new VeniceException(
          "Retry threshold for batch-get: " + retryThresholdStr + " should be setup starting from 1");
    }
    if (previousUpperBound != MAX_KEY_COUNT) {
      throw new VeniceException(
          " Retry threshold for batch-get: " + retryThresholdStr + " doesn't cover unlimited key count");
    }

    return retryThresholdMap;
  }

  public boolean isThrottleClientSslHandshakesEnabled() {
    return throttleClientSslHandshakes;
  }

  public int getClientSslHandshakeThreads() {
    return clientSslHandshakeThreads;
  }

  public int getMaxConcurrentClientSslHandshakes() {
    return maxConcurrentClientSslHandshakes;
  }

  public int getClientSslHandshakeAttempts() {
    return clientSslHandshakeAttempts;
  }

  public long getClientSslHandshakeBackoffMs() {
    return clientSslHandshakeBackoffMs;
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

  public boolean isPerRouterStorageNodeThrottlerEnabled() {
    return perRouterStorageNodeThrottlerEnabled;
  }

  public boolean isHttpClientOpensslEnabled() {
    return httpClientOpensslEnabled;
  }
}
