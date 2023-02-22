package com.linkedin.davinci.config;

import static com.linkedin.davinci.config.BlockingQueueType.ARRAY_BLOCKING_QUEUE;
import static com.linkedin.venice.ConfigKeys.AUTOCREATE_DATA_PATH;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.ENABLE_SERVER_ALLOW_LIST;
import static com.linkedin.venice.ConfigKeys.FREEZE_INGESTION_IF_READY_TO_SERVE_OR_LOCAL_DATA_EXISTS;
import static com.linkedin.venice.ConfigKeys.HELIX_HYBRID_STORE_QUOTA_ENABLED;
import static com.linkedin.venice.ConfigKeys.HYBRID_QUOTA_ENFORCEMENT_ENABLED;
import static com.linkedin.venice.ConfigKeys.INGESTION_USE_DA_VINCI_CLIENT;
import static com.linkedin.venice.ConfigKeys.KAFKA_ADMIN_CLASS;
import static com.linkedin.venice.ConfigKeys.KAFKA_PRODUCER_METRICS;
import static com.linkedin.venice.ConfigKeys.KAFKA_READ_ONLY_ADMIN_CLASS;
import static com.linkedin.venice.ConfigKeys.KAFKA_WRITE_ONLY_ADMIN_CLASS;
import static com.linkedin.venice.ConfigKeys.KEY_VALUE_PROFILING_ENABLED;
import static com.linkedin.venice.ConfigKeys.LEADER_FOLLOWER_STATE_TRANSITION_THREAD_POOL_STRATEGY;
import static com.linkedin.venice.ConfigKeys.LISTENER_PORT;
import static com.linkedin.venice.ConfigKeys.MAX_FUTURE_VERSION_LEADER_FOLLOWER_STATE_TRANSITION_THREAD_NUMBER;
import static com.linkedin.venice.ConfigKeys.MAX_LEADER_FOLLOWER_STATE_TRANSITION_THREAD_NUMBER;
import static com.linkedin.venice.ConfigKeys.OFFSET_LAG_DELTA_RELAX_FACTOR_FOR_FAST_ONLINE_TRANSITION_IN_RESTART;
import static com.linkedin.venice.ConfigKeys.PARTICIPANT_MESSAGE_CONSUMPTION_DELAY_MS;
import static com.linkedin.venice.ConfigKeys.SERVER_BLOCKING_QUEUE_TYPE;
import static com.linkedin.venice.ConfigKeys.SERVER_COMPUTE_FAST_AVRO_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_COMPUTE_QUEUE_CAPACITY;
import static com.linkedin.venice.ConfigKeys.SERVER_COMPUTE_THREAD_NUM;
import static com.linkedin.venice.ConfigKeys.SERVER_CONSUMER_POOL_SIZE_PER_KAFKA_CLUSTER;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_LOOKUP_QUEUE_CAPACITY;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_MEMORY_STATS_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_TRANSACTIONAL_MODE;
import static com.linkedin.venice.ConfigKeys.SERVER_DB_READ_ONLY_FOR_BATCH_ONLY_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DEBUG_LOGGING_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DEDICATED_DRAINER_FOR_SORTED_INPUT_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DELAY_REPORT_READY_TO_SERVE_MS;
import static com.linkedin.venice.ConfigKeys.SERVER_DISK_FULL_THRESHOLD;
import static com.linkedin.venice.ConfigKeys.SERVER_DISK_HEALTH_CHECK_INTERVAL_IN_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_DISK_HEALTH_CHECK_SERVICE_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DISK_HEALTH_CHECK_TIMEOUT_IN_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_ENABLE_KAFKA_OPENSSL;
import static com.linkedin.venice.ConfigKeys.SERVER_ENABLE_LIVE_CONFIG_BASED_KAFKA_THROTTLING;
import static com.linkedin.venice.ConfigKeys.SERVER_ENABLE_PARALLEL_BATCH_GET;
import static com.linkedin.venice.ConfigKeys.SERVER_HTTP2_HEADER_TABLE_SIZE;
import static com.linkedin.venice.ConfigKeys.SERVER_HTTP2_INBOUND_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_HTTP2_INITIAL_WINDOW_SIZE;
import static com.linkedin.venice.ConfigKeys.SERVER_HTTP2_MAX_CONCURRENT_STREAMS;
import static com.linkedin.venice.ConfigKeys.SERVER_HTTP2_MAX_FRAME_SIZE;
import static com.linkedin.venice.ConfigKeys.SERVER_HTTP2_MAX_HEADER_LIST_SIZE;
import static com.linkedin.venice.ConfigKeys.SERVER_INGESTION_CHECKPOINT_DURING_GRACEFUL_SHUTDOWN_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_INGESTION_ISOLATION_APPLICATION_PORT;
import static com.linkedin.venice.ConfigKeys.SERVER_INGESTION_ISOLATION_SERVICE_PORT;
import static com.linkedin.venice.ConfigKeys.SERVER_INGESTION_MODE;
import static com.linkedin.venice.ConfigKeys.SERVER_KAFKA_CONSUMER_OFFSET_COLLECTION_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_KAFKA_MAX_POLL_RECORDS;
import static com.linkedin.venice.ConfigKeys.SERVER_KAFKA_POLL_RETRY_BACKOFF_MS;
import static com.linkedin.venice.ConfigKeys.SERVER_KAFKA_POLL_RETRY_TIMES;
import static com.linkedin.venice.ConfigKeys.SERVER_KAFKA_PRODUCER_POOL_SIZE_PER_KAFKA_CLUSTER;
import static com.linkedin.venice.ConfigKeys.SERVER_LEAKED_RESOURCE_CLEANUP_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_LEAKED_RESOURCE_CLEAN_UP_INTERVAL_IN_MINUTES;
import static com.linkedin.venice.ConfigKeys.SERVER_LOCAL_CONSUMER_CONFIG_PREFIX;
import static com.linkedin.venice.ConfigKeys.SERVER_MAX_REQUEST_SIZE;
import static com.linkedin.venice.ConfigKeys.SERVER_NETTY_GRACEFUL_SHUTDOWN_PERIOD_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_NETTY_IDLE_TIME_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_NETTY_WORKER_THREADS;
import static com.linkedin.venice.ConfigKeys.SERVER_NODE_CAPACITY_RCU;
import static com.linkedin.venice.ConfigKeys.SERVER_NUM_SCHEMA_FAST_CLASS_WARMUP;
import static com.linkedin.venice.ConfigKeys.SERVER_OPTIMIZE_DATABASE_FOR_BACKUP_VERSION_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_OPTIMIZE_DATABASE_FOR_BACKUP_VERSION_NO_READ_THRESHOLD_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_OPTIMIZE_DATABASE_SERVICE_SCHEDULE_INTERNAL_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_PARALLEL_BATCH_GET_CHUNK_SIZE;
import static com.linkedin.venice.ConfigKeys.SERVER_PARTITION_GRACEFUL_DROP_DELAY_IN_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_QUOTA_ENFORCEMENT_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_REMOTE_CONSUMER_CONFIG_PREFIX;
import static com.linkedin.venice.ConfigKeys.SERVER_REMOTE_INGESTION_REPAIR_SLEEP_INTERVAL_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_REST_SERVICE_EPOLL_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_REST_SERVICE_STORAGE_THREAD_NUM;
import static com.linkedin.venice.ConfigKeys.SERVER_ROCKSDB_STORAGE_CONFIG_CHECK_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_ROUTER_CONNECTION_WARMING_DELAY_MS;
import static com.linkedin.venice.ConfigKeys.SERVER_SCHEMA_FAST_CLASS_WARMUP_TIMEOUT;
import static com.linkedin.venice.ConfigKeys.SERVER_SCHEMA_PRESENCE_CHECK_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_SHARED_CONSUMER_ASSIGNMENT_STRATEGY;
import static com.linkedin.venice.ConfigKeys.SERVER_SHARED_CONSUMER_NON_EXISTING_TOPIC_CLEANUP_DELAY_MS;
import static com.linkedin.venice.ConfigKeys.SERVER_SHARED_KAFKA_PRODUCER_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_SHUTDOWN_DISK_UNHEALTHY_TIME_MS;
import static com.linkedin.venice.ConfigKeys.SERVER_SOURCE_TOPIC_OFFSET_CHECK_INTERVAL_MS;
import static com.linkedin.venice.ConfigKeys.SERVER_STORE_TO_EARLY_TERMINATION_THRESHOLD_MS_MAP;
import static com.linkedin.venice.ConfigKeys.SERVER_SYSTEM_STORE_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_UNSUB_AFTER_BATCHPUSH;
import static com.linkedin.venice.ConfigKeys.SEVER_CALCULATE_QUOTA_USAGE_BASED_ON_PARTITIONS_ASSIGNMENT_ENABLED;
import static com.linkedin.venice.ConfigKeys.SORTED_INPUT_DRAINER_SIZE;
import static com.linkedin.venice.ConfigKeys.STORE_WRITER_BUFFER_MEMORY_CAPACITY;
import static com.linkedin.venice.ConfigKeys.STORE_WRITER_BUFFER_NOTIFY_DELTA;
import static com.linkedin.venice.ConfigKeys.STORE_WRITER_NUMBER;
import static com.linkedin.venice.ConfigKeys.SYSTEM_SCHEMA_CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.UNREGISTER_METRIC_FOR_DELETED_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.UNSORTED_INPUT_DRAINER_SIZE;

import com.linkedin.davinci.helix.LeaderFollowerPartitionStateModelFactory;
import com.linkedin.davinci.kafka.consumer.KafkaConsumerService;
import com.linkedin.davinci.kafka.consumer.RemoteIngestionRepairService;
import com.linkedin.davinci.store.rocksdb.RocksDBServerConfig;
import com.linkedin.venice.exceptions.ConfigurationException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.admin.KafkaAdminClient;
import com.linkedin.venice.meta.IngestionMode;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.VeniceProperties;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;


/**
 * VeniceServerConfig maintains configs specific to Venice Server, Da Vinci client and Isolated Ingestion Service.
 */
public class VeniceServerConfig extends VeniceClusterConfig {
  /**
   * Since the RT topic could be consumed by multiple store versions for Hybrid stores, we couldn't share the consumer across
   * different Hybrid store versions.
   * Considering there will be at most 3 store versions (backup, current and new), we need to make sure the consumer pool
   * size should be at least 3.
   */
  public static final int MINIMUM_CONSUMER_NUM_IN_CONSUMER_POOL_PER_KAFKA_CLUSTER = 3;

  private final int listenerPort;
  private final String dataBasePath;
  private final RocksDBServerConfig rocksDBServerConfig;
  private final boolean enableServerAllowList;
  private final boolean autoCreateDataPath; // default true

  /**
   *  Maximum number of thread that the thread pool would keep to run the Helix leader follower state transition.
   */
  private final int maxLeaderFollowerStateTransitionThreadNumber;

  // The maximum number of threads in thread pool for the future version Helix leader follower state transition.
  private final int maxFutureVersionLeaderFollowerStateTransitionThreadNumber;

  // Leader follower thread pool strategy configuration specifies how thread pools are allocated for Helix state
  // transition.
  private final LeaderFollowerPartitionStateModelFactory.LeaderFollowerThreadPoolStrategy leaderFollowerThreadPoolStrategy;

  /**
   * Thread number of store writers, which will process all the incoming records from all the topics.
   */
  private final int storeWriterNumber;

  /**
   * Thread pool size of sorted ingestion drainer when dedicatedDrainerQueue is enabled.
   */
  private final int drainerPoolSizeSortedInput;

  /**
   * Thread pool size of unsorted ingestion drainer when dedicatedDrainerQueue is enabled.
   */
  private final int drainerPoolSizeUnsortedInput;

  /**
   * Buffer capacity being used by each writer.
   * We need to be careful when tuning this param.
   * If the queue capacity is too small, the throughput will be impacted greatly,
   * and if it is too big, the memory usage used by buffered queue could be potentially high.
   * The overall memory usage is: {@link #storeWriterNumber} * {@link #storeWriterBufferMemoryCapacity}
   */
  private final long storeWriterBufferMemoryCapacity;

  /**
   * Considering the consumer thread could put various sizes of messages into the shared queue, the internal
   * {@link com.linkedin.davinci.kafka.consumer.MemoryBoundBlockingQueue} won't notify the waiting thread (consumer thread)
   * right away when some message gets processed until the freed memory hit the follow config: {@link #storeWriterBufferNotifyDelta}.
   * The reason behind this design:
   * When the buffered queue is full, and the processing thread keeps processing small message, the bigger message won't
   * have chance to get queued into the buffer since the memory freed by the processed small message is not enough to
   * fit the bigger message.
   *
   * With this delta config, {@link com.linkedin.davinci.kafka.consumer.MemoryBoundBlockingQueue} will guarantee some fairness
   * among various sizes of messages when buffered queue is full.
   *
   * When tuning this config, we need to consider the following tradeoffs:
   * 1. {@link #storeWriterBufferNotifyDelta} must be smaller than {@link #storeWriterBufferMemoryCapacity};
   * 2. If the delta is too big, it will waste some buffer space since it won't notify the waiting threads even there
   * are some memory available (less than the delta);
   * 3. If the delta is too small, the big message may not be able to get chance to be buffered when the queue is full;
   *
   */
  private final long storeWriterBufferNotifyDelta;

  /**
   * The number of threads being used to serve get requests.
   */
  private final int restServiceStorageThreadNum;

  /**
   * Idle timeout for Storage Node Netty connections.
   */
  private final int nettyIdleTimeInSeconds;

  /**
   * Max request size for request to Storage Node.
   */
  private final int maxRequestSize;

  /**
   * Time interval for offset check of topic in Hybrid Store lag measurement.
   */
  private final int topicOffsetCheckIntervalMs;

  /**
   * Graceful shutdown period.
   * Venice SN needs to explicitly do graceful shutdown since Netty's graceful shutdown logic
   * will close all the connections right away, which is not expected.
   */
  private final int nettyGracefulShutdownPeriodSeconds;

  /**
   * number of worker threads for the netty listener.  If not specified, netty uses twice cpu count.
   */
  private final int nettyWorkerThreadCount;

  private final long databaseSyncBytesIntervalForTransactionalMode;

  private final long databaseSyncBytesIntervalForDeferredWriteMode;

  private final double diskFullThreshold;

  private final int partitionGracefulDropDelaySeconds;

  private final long leakedResourceCleanUpIntervalInMS;

  private final boolean quotaEnforcementEnabled;

  private final boolean serverCalculateQuotaUsageBasedOnPartitionsAssignmentEnabled;

  private final long nodeCapacityInRcu;

  private final int kafkaMaxPollRecords;

  private final int kafkaPollRetryTimes;

  private final int kafkaPollRetryBackoffMs;

  /**
   * The number of threads being used to serve compute request.
   */
  private final int serverComputeThreadNum;

  /**
   * Health check cycle in server.
   */
  private final long diskHealthCheckIntervalInMS;

  /**
   * Server disk health check timeout.
   */
  private final long diskHealthCheckTimeoutInMs;

  private final boolean diskHealthCheckServiceEnabled;

  private final boolean computeFastAvroEnabled;

  private final long participantMessageConsumptionDelayMs;

  /**
   * Feature flag for hybrid quota, default false
   */
  private final boolean hybridQuotaEnabled;

  /**
   * When a server replica is promoted to leader from standby, it wait for some time after the last message consumed
   * before it switches to the leader role.
   */
  private final long serverPromotionToLeaderReplicaDelayMs;

  private final long serverSystemStorePromotionToLeaderReplicaDelayMs;

  private final boolean enableParallelBatchGet;

  private final int parallelBatchGetChunkSize;

  private final boolean keyValueProfilingEnabled;

  private final boolean enableDatabaseMemoryStats;

  private final Map<String, Integer> storeToEarlyTerminationThresholdMSMap;

  private final int databaseLookupQueueCapacity;
  private final int computeQueueCapacity;
  private final BlockingQueueType blockingQueueType;
  private final boolean restServiceEpollEnabled;
  private final String kafkaAdminClass;
  private final String kafkaWriteOnlyClass;
  private final String kafkaReadOnlyClass;
  private final boolean kafkaOpenSSLEnabled;
  private final long routerConnectionWarmingDelayMs;
  private final boolean helixHybridStoreQuotaEnabled;
  private final long ssdHealthCheckShutdownTimeMs;
  private final KafkaConsumerService.ConsumerAssignmentStrategy sharedConsumerAssignmentStrategy;
  private final int consumerPoolSizePerKafkaCluster;
  private final boolean leakedResourceCleanupEnabled;
  private final long delayReadyToServeMS;

  private final IngestionMode ingestionMode;
  private final int ingestionServicePort;
  private final int ingestionApplicationPort;
  private final boolean databaseChecksumVerificationEnabled;
  private final boolean rocksDbStorageEngineConfigCheckEnabled;

  private final VeniceProperties kafkaConsumerConfigsForLocalConsumption;
  private final VeniceProperties kafkaConsumerConfigsForRemoteConsumption;

  private final boolean freezeIngestionIfReadyToServeOrLocalDataExists;

  private final String systemSchemaClusterName;

  private final long sharedConsumerNonExistingTopicCleanupDelayMS;
  private final int offsetLagDeltaRelaxFactorForFastOnlineTransitionInRestart;

  private final boolean sharedKafkaProducerEnabled;
  private final int sharedProducerPoolSizePerKafkaCluster;
  private final Set<String> kafkaProducerMetrics;
  /**
   * Boolean flag indicating if it is a Da Vinci application.
   */
  private final boolean isDaVinciClient;

  private final boolean http2InboundEnabled;
  private final int http2MaxConcurrentStreams;
  private final int http2MaxFrameSize;
  private final int http2InitialWindowSize;
  private final int http2HeaderTableSize;
  private final int http2MaxHeaderListSize;

  private final boolean unsubscribeAfterBatchpushEnabled;

  private final boolean enableKafkaConsumerOffsetCollection;
  private final boolean dedicatedDrainerQueueEnabled;

  private final boolean debugLoggingEnabled;

  private final int numSchemaFastClassWarmup;
  private final long fastClassSchemaWarmupTimeout;

  private final boolean schemaPresenceCheckEnabled;

  private final boolean enableLiveConfigBasedKafkaThrottling;

  private final boolean serverIngestionCheckpointDuringGracefulShutdownEnabled;

  private final int remoteIngestionRepairSleepInterval;

  private final boolean optimizeDatabaseForBackupVersionEnabled;
  private final long optimizeDatabaseForBackupVersionNoReadThresholdMS;
  private final long optimizeDatabaseServiceScheduleIntervalSeconds;
  private final boolean unregisterMetricForDeletedStoreEnabled;
  private final boolean readOnlyForBatchOnlyStoreEnabled; // TODO: remove this config as its never used in prod

  public VeniceServerConfig(VeniceProperties serverProperties) throws ConfigurationException {
    this(serverProperties, Collections.emptyMap());
  }

  public VeniceServerConfig(VeniceProperties serverProperties, Map<String, Map<String, String>> kafkaClusterMap)
      throws ConfigurationException {
    super(serverProperties, kafkaClusterMap);
    listenerPort = serverProperties.getInt(LISTENER_PORT, 0);
    dataBasePath = serverProperties.getString(
        DATA_BASE_PATH,
        Paths.get(System.getProperty("java.io.tmpdir"), "venice-server-data").toAbsolutePath().toString());
    autoCreateDataPath = Boolean.parseBoolean(serverProperties.getString(AUTOCREATE_DATA_PATH, "true"));
    rocksDBServerConfig = new RocksDBServerConfig(serverProperties);
    enableServerAllowList = serverProperties.getBoolean(ENABLE_SERVER_ALLOW_LIST, false);
    maxLeaderFollowerStateTransitionThreadNumber =
        serverProperties.getInt(MAX_LEADER_FOLLOWER_STATE_TRANSITION_THREAD_NUMBER, 3);

    String lfThreadPoolStrategyStr = serverProperties.getString(
        LEADER_FOLLOWER_STATE_TRANSITION_THREAD_POOL_STRATEGY,
        LeaderFollowerPartitionStateModelFactory.LeaderFollowerThreadPoolStrategy.SINGLE_POOL_STRATEGY.name());
    try {
      leaderFollowerThreadPoolStrategy =
          LeaderFollowerPartitionStateModelFactory.LeaderFollowerThreadPoolStrategy.valueOf(lfThreadPoolStrategyStr);
    } catch (IllegalArgumentException e) {
      throw new VeniceException(
          String.format("Invalid leader follower thread pool strategy: %s", lfThreadPoolStrategyStr));
    }
    maxFutureVersionLeaderFollowerStateTransitionThreadNumber =
        serverProperties.getInt(MAX_FUTURE_VERSION_LEADER_FOLLOWER_STATE_TRANSITION_THREAD_NUMBER, 10);
    storeWriterNumber = serverProperties.getInt(STORE_WRITER_NUMBER, 8);
    drainerPoolSizeSortedInput = serverProperties.getInt(SORTED_INPUT_DRAINER_SIZE, 8);
    drainerPoolSizeUnsortedInput = serverProperties.getInt(UNSORTED_INPUT_DRAINER_SIZE, 8);

    // To minimize the GC impact during heavy ingestion.
    storeWriterBufferMemoryCapacity =
        serverProperties.getSizeInBytes(STORE_WRITER_BUFFER_MEMORY_CAPACITY, 10 * 1024 * 1024);
    storeWriterBufferNotifyDelta = serverProperties.getSizeInBytes(STORE_WRITER_BUFFER_NOTIFY_DELTA, 1 * 1024 * 1024);
    restServiceStorageThreadNum = serverProperties.getInt(SERVER_REST_SERVICE_STORAGE_THREAD_NUM, 16);
    serverComputeThreadNum = serverProperties.getInt(SERVER_COMPUTE_THREAD_NUM, 16);
    nettyIdleTimeInSeconds = serverProperties.getInt(SERVER_NETTY_IDLE_TIME_SECONDS, (int) TimeUnit.HOURS.toSeconds(3));
    maxRequestSize = (int) serverProperties.getSizeInBytes(SERVER_MAX_REQUEST_SIZE, 256 * 1024);
    topicOffsetCheckIntervalMs =
        serverProperties.getInt(SERVER_SOURCE_TOPIC_OFFSET_CHECK_INTERVAL_MS, (int) TimeUnit.SECONDS.toMillis(60));
    nettyGracefulShutdownPeriodSeconds = serverProperties.getInt(SERVER_NETTY_GRACEFUL_SHUTDOWN_PERIOD_SECONDS, 30);
    nettyWorkerThreadCount = serverProperties.getInt(SERVER_NETTY_WORKER_THREADS, 0);

    remoteIngestionRepairSleepInterval = serverProperties.getInt(
        SERVER_REMOTE_INGESTION_REPAIR_SLEEP_INTERVAL_SECONDS,
        RemoteIngestionRepairService.DEFAULT_REPAIR_THREAD_SLEEP_INTERVAL_SECONDS);

    readOnlyForBatchOnlyStoreEnabled =
        serverProperties.getBoolean(SERVER_DB_READ_ONLY_FOR_BATCH_ONLY_STORE_ENABLED, true);
    databaseSyncBytesIntervalForTransactionalMode =
        serverProperties.getSizeInBytes(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_TRANSACTIONAL_MODE, 32 * 1024 * 1024);
    databaseSyncBytesIntervalForDeferredWriteMode =
        serverProperties.getSizeInBytes(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE, 60 * 1024 * 1024);
    diskFullThreshold = serverProperties.getDouble(SERVER_DISK_FULL_THRESHOLD, 0.95);
    partitionGracefulDropDelaySeconds = serverProperties.getInt(SERVER_PARTITION_GRACEFUL_DROP_DELAY_IN_SECONDS, 30);
    leakedResourceCleanUpIntervalInMS =
        TimeUnit.MINUTES.toMillis(serverProperties.getLong(SERVER_LEAKED_RESOURCE_CLEAN_UP_INTERVAL_IN_MINUTES, 10));
    quotaEnforcementEnabled = serverProperties.getBoolean(SERVER_QUOTA_ENFORCEMENT_ENABLED, false);
    serverCalculateQuotaUsageBasedOnPartitionsAssignmentEnabled =
        serverProperties.getBoolean(SEVER_CALCULATE_QUOTA_USAGE_BASED_ON_PARTITIONS_ASSIGNMENT_ENABLED, true);

    nodeCapacityInRcu = serverProperties.getLong(SERVER_NODE_CAPACITY_RCU, 50000);
    kafkaMaxPollRecords = serverProperties.getInt(SERVER_KAFKA_MAX_POLL_RECORDS, 100);
    kafkaPollRetryTimes = serverProperties.getInt(SERVER_KAFKA_POLL_RETRY_TIMES, 100);
    kafkaPollRetryBackoffMs = serverProperties.getInt(SERVER_KAFKA_POLL_RETRY_BACKOFF_MS, 0);
    diskHealthCheckIntervalInMS =
        TimeUnit.SECONDS.toMillis(serverProperties.getLong(SERVER_DISK_HEALTH_CHECK_INTERVAL_IN_SECONDS, 10));
    diskHealthCheckTimeoutInMs =
        TimeUnit.SECONDS.toMillis(serverProperties.getLong(SERVER_DISK_HEALTH_CHECK_TIMEOUT_IN_SECONDS, 30));
    diskHealthCheckServiceEnabled = serverProperties.getBoolean(SERVER_DISK_HEALTH_CHECK_SERVICE_ENABLED, true);
    computeFastAvroEnabled = serverProperties.getBoolean(SERVER_COMPUTE_FAST_AVRO_ENABLED, true);
    participantMessageConsumptionDelayMs = serverProperties.getLong(PARTICIPANT_MESSAGE_CONSUMPTION_DELAY_MS, 60000);
    serverPromotionToLeaderReplicaDelayMs =
        TimeUnit.SECONDS.toMillis(serverProperties.getLong(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, 300));
    serverSystemStorePromotionToLeaderReplicaDelayMs = TimeUnit.SECONDS
        .toMillis(serverProperties.getLong(SERVER_SYSTEM_STORE_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, 1));
    hybridQuotaEnabled = serverProperties.getBoolean(HYBRID_QUOTA_ENFORCEMENT_ENABLED, false);

    enableParallelBatchGet = serverProperties.getBoolean(SERVER_ENABLE_PARALLEL_BATCH_GET, false);
    parallelBatchGetChunkSize = serverProperties.getInt(SERVER_PARALLEL_BATCH_GET_CHUNK_SIZE, 5);

    keyValueProfilingEnabled = serverProperties.getBoolean(KEY_VALUE_PROFILING_ENABLED, false);
    enableDatabaseMemoryStats = serverProperties.getBoolean(SERVER_DATABASE_MEMORY_STATS_ENABLED, true);

    Map<String, String> storeToEarlyTerminationThresholdMSMapProp =
        serverProperties.getMap(SERVER_STORE_TO_EARLY_TERMINATION_THRESHOLD_MS_MAP, Collections.emptyMap());
    storeToEarlyTerminationThresholdMSMap = new HashMap<>();
    storeToEarlyTerminationThresholdMSMapProp.forEach(
        (storeName, thresholdStr) -> storeToEarlyTerminationThresholdMSMap
            .put(storeName, Integer.parseInt(thresholdStr.trim())));
    databaseLookupQueueCapacity = serverProperties.getInt(SERVER_DATABASE_LOOKUP_QUEUE_CAPACITY, Integer.MAX_VALUE);
    computeQueueCapacity = serverProperties.getInt(SERVER_COMPUTE_QUEUE_CAPACITY, Integer.MAX_VALUE);
    kafkaOpenSSLEnabled = serverProperties.getBoolean(SERVER_ENABLE_KAFKA_OPENSSL, false);
    helixHybridStoreQuotaEnabled = serverProperties.getBoolean(HELIX_HYBRID_STORE_QUOTA_ENABLED, false);
    ssdHealthCheckShutdownTimeMs = serverProperties.getLong(SERVER_SHUTDOWN_DISK_UNHEALTHY_TIME_MS, 200000);

    /**
     * In the test of feature store user case, when we did a rolling bounce of storage nodes, the high latency happened
     * to one or two storage nodes randomly. And when we restarted the node with high latency, the high latency could
     * disappear, but other nodes could start high latency.
     * After switching to {@link java.util.concurrent.LinkedBlockingQueue}, this issue never happened.
     */
    String blockingQueueTypeStr =
        serverProperties.getString(SERVER_BLOCKING_QUEUE_TYPE, BlockingQueueType.LINKED_BLOCKING_QUEUE.name());
    try {
      blockingQueueType = BlockingQueueType.valueOf(blockingQueueTypeStr);
    } catch (IllegalArgumentException e) {
      throw new VeniceException("Valid blocking queue options: " + Arrays.toString(BlockingQueueType.values()));
    }

    restServiceEpollEnabled = serverProperties.getBoolean(SERVER_REST_SERVICE_EPOLL_ENABLED, false);
    kafkaAdminClass = serverProperties.getString(KAFKA_ADMIN_CLASS, KafkaAdminClient.class.getName());
    kafkaWriteOnlyClass = serverProperties.getString(KAFKA_WRITE_ONLY_ADMIN_CLASS, kafkaAdminClass);
    kafkaReadOnlyClass = serverProperties.getString(KAFKA_READ_ONLY_ADMIN_CLASS, kafkaAdminClass);
    // Disable it by default, and when router connection warming is enabled, we need to adjust this config.
    routerConnectionWarmingDelayMs = serverProperties.getLong(SERVER_ROUTER_CONNECTION_WARMING_DELAY_MS, 0);
    String sharedConsumerAssignmentStrategyStr = serverProperties.getString(
        SERVER_SHARED_CONSUMER_ASSIGNMENT_STRATEGY,
        KafkaConsumerService.ConsumerAssignmentStrategy.TOPIC_WISE_SHARED_CONSUMER_ASSIGNMENT_STRATEGY.name());
    try {
      sharedConsumerAssignmentStrategy =
          KafkaConsumerService.ConsumerAssignmentStrategy.valueOf(sharedConsumerAssignmentStrategyStr);
    } catch (IllegalArgumentException e) {
      throw new VeniceException(
          "Invalid consumer assignment strategy: "
              + Arrays.toString(KafkaConsumerService.ConsumerAssignmentStrategy.values()));
    }

    consumerPoolSizePerKafkaCluster = serverProperties.getInt(SERVER_CONSUMER_POOL_SIZE_PER_KAFKA_CLUSTER, 5);
    if (consumerPoolSizePerKafkaCluster < MINIMUM_CONSUMER_NUM_IN_CONSUMER_POOL_PER_KAFKA_CLUSTER) {
      throw new VeniceException(
          SERVER_CONSUMER_POOL_SIZE_PER_KAFKA_CLUSTER + " shouldn't be less than: "
              + MINIMUM_CONSUMER_NUM_IN_CONSUMER_POOL_PER_KAFKA_CLUSTER + ", but it is "
              + consumerPoolSizePerKafkaCluster);
    }
    leakedResourceCleanupEnabled = serverProperties.getBoolean(SERVER_LEAKED_RESOURCE_CLEANUP_ENABLED, true);
    delayReadyToServeMS = serverProperties.getLong(SERVER_DELAY_REPORT_READY_TO_SERVE_MS, 0);

    ingestionMode =
        IngestionMode.valueOf(serverProperties.getString(SERVER_INGESTION_MODE, IngestionMode.BUILT_IN.toString()));
    ingestionServicePort = serverProperties.getInt(SERVER_INGESTION_ISOLATION_SERVICE_PORT, 27015);
    ingestionApplicationPort = serverProperties.getInt(SERVER_INGESTION_ISOLATION_APPLICATION_PORT, 27016);
    databaseChecksumVerificationEnabled =
        serverProperties.getBoolean(SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED, false);

    kafkaConsumerConfigsForLocalConsumption =
        serverProperties.clipAndFilterNamespace(SERVER_LOCAL_CONSUMER_CONFIG_PREFIX);
    kafkaConsumerConfigsForRemoteConsumption =
        serverProperties.clipAndFilterNamespace(SERVER_REMOTE_CONSUMER_CONFIG_PREFIX);

    rocksDbStorageEngineConfigCheckEnabled =
        serverProperties.getBoolean(SERVER_ROCKSDB_STORAGE_CONFIG_CHECK_ENABLED, true);

    freezeIngestionIfReadyToServeOrLocalDataExists =
        serverProperties.getBoolean(FREEZE_INGESTION_IF_READY_TO_SERVE_OR_LOCAL_DATA_EXISTS, false);

    systemSchemaClusterName = serverProperties.getString(SYSTEM_SCHEMA_CLUSTER_NAME, "");
    sharedConsumerNonExistingTopicCleanupDelayMS = serverProperties
        .getLong(SERVER_SHARED_CONSUMER_NON_EXISTING_TOPIC_CLEANUP_DELAY_MS, TimeUnit.MINUTES.toMillis(10));
    sharedKafkaProducerEnabled = serverProperties.getBoolean(SERVER_SHARED_KAFKA_PRODUCER_ENABLED, false);
    sharedProducerPoolSizePerKafkaCluster =
        serverProperties.getInt(SERVER_KAFKA_PRODUCER_POOL_SIZE_PER_KAFKA_CLUSTER, 8);

    List<String> kafkaProducerMetricsList = serverProperties.getList(
        KAFKA_PRODUCER_METRICS,
        Arrays.asList(
            "outgoing-byte-rate",
            "record-send-rate",
            "batch-size-max",
            "batch-size-avg",
            "buffer-available-bytes",
            "buffer-exhausted-rate"));
    kafkaProducerMetrics = new HashSet<>(kafkaProducerMetricsList);

    isDaVinciClient = serverProperties.getBoolean(INGESTION_USE_DA_VINCI_CLIENT, false);
    unsubscribeAfterBatchpushEnabled = serverProperties.getBoolean(SERVER_UNSUB_AFTER_BATCHPUSH, false);

    http2InboundEnabled = serverProperties.getBoolean(SERVER_HTTP2_INBOUND_ENABLED, false);
    http2MaxConcurrentStreams = serverProperties.getInt(SERVER_HTTP2_MAX_CONCURRENT_STREAMS, 100);
    http2MaxFrameSize = serverProperties.getInt(SERVER_HTTP2_MAX_FRAME_SIZE, 8 * 1024 * 1024);
    http2InitialWindowSize = serverProperties.getInt(SERVER_HTTP2_INITIAL_WINDOW_SIZE, 8 * 1024 * 1024);
    http2HeaderTableSize = serverProperties.getInt(SERVER_HTTP2_HEADER_TABLE_SIZE, 4096);
    http2MaxHeaderListSize = serverProperties.getInt(SERVER_HTTP2_MAX_HEADER_LIST_SIZE, 8192);

    offsetLagDeltaRelaxFactorForFastOnlineTransitionInRestart =
        serverProperties.getInt(OFFSET_LAG_DELTA_RELAX_FACTOR_FOR_FAST_ONLINE_TRANSITION_IN_RESTART, 2);
    enableKafkaConsumerOffsetCollection =
        serverProperties.getBoolean(SERVER_KAFKA_CONSUMER_OFFSET_COLLECTION_ENABLED, true);
    dedicatedDrainerQueueEnabled =
        serverProperties.getBoolean(SERVER_DEDICATED_DRAINER_FOR_SORTED_INPUT_ENABLED, false);
    debugLoggingEnabled = serverProperties.getBoolean(SERVER_DEBUG_LOGGING_ENABLED, false);
    numSchemaFastClassWarmup = serverProperties.getInt(SERVER_NUM_SCHEMA_FAST_CLASS_WARMUP, 10);
    fastClassSchemaWarmupTimeout =
        serverProperties.getLong(SERVER_SCHEMA_FAST_CLASS_WARMUP_TIMEOUT, 2 * Time.MS_PER_MINUTE);
    schemaPresenceCheckEnabled = serverProperties.getBoolean(SERVER_SCHEMA_PRESENCE_CHECK_ENABLED, true);
    enableLiveConfigBasedKafkaThrottling =
        serverProperties.getBoolean(SERVER_ENABLE_LIVE_CONFIG_BASED_KAFKA_THROTTLING, false);
    /**
     * Enable graceful shutdown by default.
     * Speculative risks when disabling graceful shutdown:
     * 1. Servers may need to reconsume a decent amount of data, depending on the last ingestion checkpoint. Hybrid stores may take longer
     *    time to go online, and thus delay or fail server deployments which will check and maintain minimum number of online replicas.
     * Potential mitigations:
     * 1. Enable {@link com.linkedin.venice.ConfigKeys#OFFSET_LAG_DELTA_RELAX_FACTOR_FOR_FAST_ONLINE_TRANSITION_IN_RESTART} so that previously
     *    online hybrid replicas can become online faster after nodes restart.
     * 2. Reduce checkpoint threshold
     */
    serverIngestionCheckpointDuringGracefulShutdownEnabled =
        serverProperties.getBoolean(SERVER_INGESTION_CHECKPOINT_DURING_GRACEFUL_SHUTDOWN_ENABLED, true);
    optimizeDatabaseForBackupVersionEnabled =
        serverProperties.getBoolean(SERVER_OPTIMIZE_DATABASE_FOR_BACKUP_VERSION_ENABLED, false);
    optimizeDatabaseForBackupVersionNoReadThresholdMS = serverProperties
        .getLong(SERVER_OPTIMIZE_DATABASE_FOR_BACKUP_VERSION_NO_READ_THRESHOLD_SECONDS, TimeUnit.MINUTES.toMillis(3));
    optimizeDatabaseServiceScheduleIntervalSeconds = serverProperties
        .getLong(SERVER_OPTIMIZE_DATABASE_SERVICE_SCHEDULE_INTERNAL_SECONDS, TimeUnit.MINUTES.toSeconds(1));
    unregisterMetricForDeletedStoreEnabled =
        serverProperties.getBoolean(UNREGISTER_METRIC_FOR_DELETED_STORE_ENABLED, false);
  }

  public int getListenerPort() {
    return listenerPort;
  }

  /**
   * Get base path of Venice storage data.
   *
   * @return Base path of persisted Venice database files.
   */
  public String getDataBasePath() {
    return this.dataBasePath;
  }

  public boolean isAutoCreateDataPath() {
    return autoCreateDataPath;
  }

  public RocksDBServerConfig getRocksDBServerConfig() {
    return rocksDBServerConfig;
  }

  public boolean isServerAllowlistEnabled() {
    return enableServerAllowList;
  }

  public int getMaxLeaderFollowerStateTransitionThreadNumber() {
    return maxLeaderFollowerStateTransitionThreadNumber;
  }

  public int getMaxFutureVersionLeaderFollowerStateTransitionThreadNumber() {
    return maxFutureVersionLeaderFollowerStateTransitionThreadNumber;
  }

  public LeaderFollowerPartitionStateModelFactory.LeaderFollowerThreadPoolStrategy getLeaderFollowerThreadPoolStrategy() {
    return leaderFollowerThreadPoolStrategy;
  }

  public int getStoreWriterNumber() {
    return this.storeWriterNumber;
  }

  public long getStoreWriterBufferMemoryCapacity() {
    return this.storeWriterBufferMemoryCapacity;
  }

  public long getStoreWriterBufferNotifyDelta() {
    return this.storeWriterBufferNotifyDelta;
  }

  public int getRestServiceStorageThreadNum() {
    return restServiceStorageThreadNum;
  }

  public int getServerComputeThreadNum() {
    return serverComputeThreadNum;
  }

  public int getNettyIdleTimeInSeconds() {
    return nettyIdleTimeInSeconds;
  }

  public int getMaxRequestSize() {
    return maxRequestSize;
  }

  public int getTopicOffsetCheckIntervalMs() {
    return topicOffsetCheckIntervalMs;
  }

  public int getNettyGracefulShutdownPeriodSeconds() {
    return nettyGracefulShutdownPeriodSeconds;
  }

  public int getNettyWorkerThreadCount() {
    return nettyWorkerThreadCount;
  }

  public long getDatabaseSyncBytesIntervalForTransactionalMode() {
    return databaseSyncBytesIntervalForTransactionalMode;
  }

  public long getDatabaseSyncBytesIntervalForDeferredWriteMode() {
    return databaseSyncBytesIntervalForDeferredWriteMode;
  }

  public double getDiskFullThreshold() {
    return diskFullThreshold;
  }

  public int getPartitionGracefulDropDelaySeconds() {
    return partitionGracefulDropDelaySeconds;
  }

  public long getLeakedResourceCleanUpIntervalInMS() {
    return leakedResourceCleanUpIntervalInMS;
  }

  public boolean isQuotaEnforcementEnabled() {
    return quotaEnforcementEnabled;
  }

  public boolean isServerCalculateQuotaUsageBasedOnPartitionsAssignmentEnabled() {
    return serverCalculateQuotaUsageBasedOnPartitionsAssignmentEnabled;
  }

  public long getNodeCapacityInRcu() {
    return nodeCapacityInRcu;
  }

  public int getKafkaMaxPollRecords() {
    return kafkaMaxPollRecords;
  }

  public int getKafkaPollRetryTimes() {
    return kafkaPollRetryTimes;
  }

  public int getKafkaPollRetryBackoffMs() {
    return kafkaPollRetryBackoffMs;
  }

  public long getDiskHealthCheckIntervalInMS() {
    return diskHealthCheckIntervalInMS;
  }

  public long getDiskHealthCheckTimeoutInMs() {
    return diskHealthCheckTimeoutInMs;
  }

  public boolean isDiskHealthCheckServiceEnabled() {
    return diskHealthCheckServiceEnabled;
  }

  public BlockingQueue<Runnable> getExecutionQueue(int capacity) {
    switch (blockingQueueType) {
      case LINKED_BLOCKING_QUEUE:
        return new LinkedBlockingQueue<>(capacity);
      case ARRAY_BLOCKING_QUEUE:
        if (capacity == Integer.MAX_VALUE) {
          throw new VeniceException("Queue capacity must be specified when using " + ARRAY_BLOCKING_QUEUE);
        }
        return new ArrayBlockingQueue<>(capacity);
      default:
        throw new VeniceException("Unknown blocking queue type: " + blockingQueueType);
    }
  }

  public boolean isComputeFastAvroEnabled() {
    return computeFastAvroEnabled;
  }

  public long getParticipantMessageConsumptionDelayMs() {
    return participantMessageConsumptionDelayMs;
  }

  public long getServerPromotionToLeaderReplicaDelayMs() {
    return serverPromotionToLeaderReplicaDelayMs;
  }

  public long getServerSystemStorePromotionToLeaderReplicaDelayMs() {
    return serverSystemStorePromotionToLeaderReplicaDelayMs;
  }

  public boolean isHybridQuotaEnabled() {
    return hybridQuotaEnabled;
  }

  public boolean isEnableParallelBatchGet() {
    return enableParallelBatchGet;
  }

  public int getParallelBatchGetChunkSize() {
    return parallelBatchGetChunkSize;
  }

  public boolean isKeyValueProfilingEnabled() {
    return keyValueProfilingEnabled;
  }

  public boolean isDatabaseMemoryStatsEnabled() {
    return enableDatabaseMemoryStats;
  }

  public Map<String, Integer> getStoreToEarlyTerminationThresholdMSMap() {
    return storeToEarlyTerminationThresholdMSMap;
  }

  public int getDatabaseLookupQueueCapacity() {
    return databaseLookupQueueCapacity;
  }

  public int getComputeQueueCapacity() {
    return computeQueueCapacity;
  }

  public boolean isRestServiceEpollEnabled() {
    return restServiceEpollEnabled;
  }

  public String getKafkaAdminClass() {
    return kafkaAdminClass;
  }

  public String getKafkaWriteOnlyClass() {
    return kafkaWriteOnlyClass;
  }

  public String getKafkaReadOnlyClass() {
    return kafkaReadOnlyClass;
  }

  public boolean isKafkaOpenSSLEnabled() {
    return kafkaOpenSSLEnabled;
  }

  public long getRouterConnectionWarmingDelayMs() {
    return routerConnectionWarmingDelayMs;
  }

  public boolean isHelixHybridStoreQuotaEnabled() {
    return helixHybridStoreQuotaEnabled;
  }

  public long getSsdHealthCheckShutdownTimeMs() {
    return ssdHealthCheckShutdownTimeMs;
  }

  public KafkaConsumerService.ConsumerAssignmentStrategy getSharedConsumerAssignmentStrategy() {
    return sharedConsumerAssignmentStrategy;
  }

  public int getConsumerPoolSizePerKafkaCluster() {
    return consumerPoolSizePerKafkaCluster;
  }

  public boolean isLeakedResourceCleanupEnabled() {
    return leakedResourceCleanupEnabled;
  }

  public long getDelayReadyToServeMS() {
    return delayReadyToServeMS;
  }

  public IngestionMode getIngestionMode() {
    return ingestionMode;
  }

  public int getIngestionServicePort() {
    return ingestionServicePort;
  }

  public int getIngestionApplicationPort() {
    return ingestionApplicationPort;
  }

  public boolean isDatabaseChecksumVerificationEnabled() {
    return databaseChecksumVerificationEnabled;
  }

  public VeniceProperties getKafkaConsumerConfigsForLocalConsumption() {
    return kafkaConsumerConfigsForLocalConsumption;
  }

  public VeniceProperties getKafkaConsumerConfigsForRemoteConsumption() {
    return kafkaConsumerConfigsForRemoteConsumption;
  }

  public boolean isRocksDbStorageEngineConfigCheckEnabled() {
    return rocksDbStorageEngineConfigCheckEnabled;
  }

  public boolean freezeIngestionIfReadyToServeOrLocalDataExists() {
    return freezeIngestionIfReadyToServeOrLocalDataExists;
  }

  public String getSystemSchemaClusterName() {
    return systemSchemaClusterName;
  }

  public long getSharedConsumerNonExistingTopicCleanupDelayMS() {
    return sharedConsumerNonExistingTopicCleanupDelayMS;
  }

  public boolean isSharedKafkaProducerEnabled() {
    return sharedKafkaProducerEnabled;
  }

  public int getSharedProducerPoolSizePerKafkaCluster() {
    return sharedProducerPoolSizePerKafkaCluster;
  }

  public Set<String> getKafkaProducerMetrics() {
    return kafkaProducerMetrics;
  }

  public boolean isDaVinciClient() {
    return isDaVinciClient;
  }

  public boolean isUnsubscribeAfterBatchpushEnabled() {
    return unsubscribeAfterBatchpushEnabled;
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

  public int getOffsetLagDeltaRelaxFactorForFastOnlineTransitionInRestart() {
    return offsetLagDeltaRelaxFactorForFastOnlineTransitionInRestart;
  }

  public boolean isKafkaConsumerOffsetCollectionEnabled() {
    return enableKafkaConsumerOffsetCollection;
  }

  public boolean isDedicatedDrainerQueueEnabled() {
    return dedicatedDrainerQueueEnabled;
  }

  public int getDrainerPoolSizeSortedInput() {
    return drainerPoolSizeSortedInput;
  }

  public int getDrainerPoolSizeUnsortedInput() {
    return drainerPoolSizeUnsortedInput;
  }

  public boolean isDebugLoggingEnabled() {
    return debugLoggingEnabled;
  }

  public int getNumSchemaFastClassWarmup() {
    return numSchemaFastClassWarmup;
  }

  public long getFastClassSchemaWarmupTimeout() {
    return fastClassSchemaWarmupTimeout;
  }

  public boolean isSchemaPresenceCheckEnabled() {
    return schemaPresenceCheckEnabled;
  }

  public boolean isLiveConfigBasedKafkaThrottlingEnabled() {
    return enableLiveConfigBasedKafkaThrottling;
  }

  public boolean isServerIngestionCheckpointDuringGracefulShutdownEnabled() {
    return serverIngestionCheckpointDuringGracefulShutdownEnabled;
  }

  public int getRemoteIngestionRepairSleepInterval() {
    return remoteIngestionRepairSleepInterval;
  }

  public boolean isOptimizeDatabaseForBackupVersionEnabled() {
    return optimizeDatabaseForBackupVersionEnabled;
  }

  public long getOptimizeDatabaseForBackupVersionNoReadThresholdMS() {
    return optimizeDatabaseForBackupVersionNoReadThresholdMS;
  }

  public long getOptimizeDatabaseServiceScheduleIntervalSeconds() {
    return optimizeDatabaseServiceScheduleIntervalSeconds;
  }

  public boolean isUnregisterMetricForDeletedStoreEnabled() {
    return unregisterMetricForDeletedStoreEnabled;
  }

  public boolean isReadOnlyForBatchOnlyStoreEnabled() {
    return readOnlyForBatchOnlyStoreEnabled;
  }
}
