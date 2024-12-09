package com.linkedin.davinci.config;

import static com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils.INGESTION_ISOLATION_CONFIG_PREFIX;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_TOTAL_MEMTABLE_USAGE_CAP_IN_BYTES;
import static com.linkedin.venice.ConfigConstants.DEFAULT_MAX_RECORD_SIZE_BYTES_BACKFILL;
import static com.linkedin.venice.ConfigKeys.AUTOCREATE_DATA_PATH;
import static com.linkedin.venice.ConfigKeys.BLOB_TRANSFER_DISABLED_OFFSET_LAG_THRESHOLD;
import static com.linkedin.venice.ConfigKeys.BLOB_TRANSFER_MANAGER_ENABLED;
import static com.linkedin.venice.ConfigKeys.BLOB_TRANSFER_MAX_CONCURRENT_SNAPSHOT_USER;
import static com.linkedin.venice.ConfigKeys.BLOB_TRANSFER_MAX_TIMEOUT_IN_MIN;
import static com.linkedin.venice.ConfigKeys.BLOB_TRANSFER_SNAPSHOT_RETENTION_TIME_IN_MIN;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.DAVINCI_P2P_BLOB_TRANSFER_CLIENT_PORT;
import static com.linkedin.venice.ConfigKeys.DAVINCI_P2P_BLOB_TRANSFER_SERVER_PORT;
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_CHECK_INTERVAL_IN_MS;
import static com.linkedin.venice.ConfigKeys.DA_VINCI_CURRENT_VERSION_BOOTSTRAPPING_QUOTA_BYTES_PER_SECOND;
import static com.linkedin.venice.ConfigKeys.DA_VINCI_CURRENT_VERSION_BOOTSTRAPPING_QUOTA_RECORDS_PER_SECOND;
import static com.linkedin.venice.ConfigKeys.DA_VINCI_CURRENT_VERSION_BOOTSTRAPPING_SPEEDUP_ENABLED;
import static com.linkedin.venice.ConfigKeys.DEFAULT_MAX_RECORD_SIZE_BYTES;
import static com.linkedin.venice.ConfigKeys.DIV_PRODUCER_STATE_MAX_AGE_MS;
import static com.linkedin.venice.ConfigKeys.ENABLE_GRPC_READ_SERVER;
import static com.linkedin.venice.ConfigKeys.ENABLE_SERVER_ALLOW_LIST;
import static com.linkedin.venice.ConfigKeys.FAST_AVRO_FIELD_LIMIT_PER_METHOD;
import static com.linkedin.venice.ConfigKeys.FREEZE_INGESTION_IF_READY_TO_SERVE_OR_LOCAL_DATA_EXISTS;
import static com.linkedin.venice.ConfigKeys.GRPC_READ_SERVER_PORT;
import static com.linkedin.venice.ConfigKeys.GRPC_SERVER_WORKER_THREAD_COUNT;
import static com.linkedin.venice.ConfigKeys.HELIX_HYBRID_STORE_QUOTA_ENABLED;
import static com.linkedin.venice.ConfigKeys.HYBRID_QUOTA_ENFORCEMENT_ENABLED;
import static com.linkedin.venice.ConfigKeys.IDENTITY_PARSER_CLASS;
import static com.linkedin.venice.ConfigKeys.INGESTION_MEMORY_LIMIT;
import static com.linkedin.venice.ConfigKeys.INGESTION_MEMORY_LIMIT_STORE_LIST;
import static com.linkedin.venice.ConfigKeys.INGESTION_MLOCK_ENABLED;
import static com.linkedin.venice.ConfigKeys.INGESTION_USE_DA_VINCI_CLIENT;
import static com.linkedin.venice.ConfigKeys.KAFKA_ADMIN_CLASS;
import static com.linkedin.venice.ConfigKeys.KAFKA_PRODUCER_METRICS;
import static com.linkedin.venice.ConfigKeys.KAFKA_READ_ONLY_ADMIN_CLASS;
import static com.linkedin.venice.ConfigKeys.KAFKA_WRITE_ONLY_ADMIN_CLASS;
import static com.linkedin.venice.ConfigKeys.KEY_VALUE_PROFILING_ENABLED;
import static com.linkedin.venice.ConfigKeys.KME_REGISTRATION_FROM_MESSAGE_HEADER_ENABLED;
import static com.linkedin.venice.ConfigKeys.LEADER_FOLLOWER_STATE_TRANSITION_THREAD_POOL_STRATEGY;
import static com.linkedin.venice.ConfigKeys.LISTENER_HOSTNAME;
import static com.linkedin.venice.ConfigKeys.LISTENER_PORT;
import static com.linkedin.venice.ConfigKeys.LOCAL_CONTROLLER_D2_SERVICE_NAME;
import static com.linkedin.venice.ConfigKeys.LOCAL_CONTROLLER_URL;
import static com.linkedin.venice.ConfigKeys.LOCAL_D2_ZK_HOST;
import static com.linkedin.venice.ConfigKeys.MAX_FUTURE_VERSION_LEADER_FOLLOWER_STATE_TRANSITION_THREAD_NUMBER;
import static com.linkedin.venice.ConfigKeys.MAX_LEADER_FOLLOWER_STATE_TRANSITION_THREAD_NUMBER;
import static com.linkedin.venice.ConfigKeys.META_STORE_WRITER_CLOSE_CONCURRENCY;
import static com.linkedin.venice.ConfigKeys.META_STORE_WRITER_CLOSE_TIMEOUT_MS;
import static com.linkedin.venice.ConfigKeys.MIN_CONSUMER_IN_CONSUMER_POOL_PER_KAFKA_CLUSTER;
import static com.linkedin.venice.ConfigKeys.OFFSET_LAG_DELTA_RELAX_FACTOR_FOR_FAST_ONLINE_TRANSITION_IN_RESTART;
import static com.linkedin.venice.ConfigKeys.PARTICIPANT_MESSAGE_CONSUMPTION_DELAY_MS;
import static com.linkedin.venice.ConfigKeys.PUBSUB_TOPIC_MANAGER_METADATA_FETCHER_CONSUMER_POOL_SIZE;
import static com.linkedin.venice.ConfigKeys.PUBSUB_TOPIC_MANAGER_METADATA_FETCHER_THREAD_POOL_SIZE;
import static com.linkedin.venice.ConfigKeys.ROUTER_PRINCIPAL_NAME;
import static com.linkedin.venice.ConfigKeys.SERVER_AA_WC_LEADER_QUOTA_RECORDS_PER_SECOND;
import static com.linkedin.venice.ConfigKeys.SERVER_AA_WC_WORKLOAD_PARALLEL_PROCESSING_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_AA_WC_WORKLOAD_PARALLEL_PROCESSING_THREAD_POOL_SIZE;
import static com.linkedin.venice.ConfigKeys.SERVER_BATCH_REPORT_END_OF_INCREMENTAL_PUSH_STATUS_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_BLOCKING_QUEUE_TYPE;
import static com.linkedin.venice.ConfigKeys.SERVER_CHANNEL_OPTION_WRITE_BUFFER_WATERMARK_HIGH_BYTES;
import static com.linkedin.venice.ConfigKeys.SERVER_COMPUTE_FAST_AVRO_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_COMPUTE_QUEUE_CAPACITY;
import static com.linkedin.venice.ConfigKeys.SERVER_COMPUTE_THREAD_NUM;
import static com.linkedin.venice.ConfigKeys.SERVER_CONSUMER_POOL_ALLOCATION_STRATEGY;
import static com.linkedin.venice.ConfigKeys.SERVER_CONSUMER_POOL_SIZE_FOR_CURRENT_VERSION_AA_WC_LEADER;
import static com.linkedin.venice.ConfigKeys.SERVER_CONSUMER_POOL_SIZE_FOR_CURRENT_VERSION_NON_AA_WC_LEADER;
import static com.linkedin.venice.ConfigKeys.SERVER_CONSUMER_POOL_SIZE_FOR_CURRENT_VERSION_SEPARATE_RT_LEADER;
import static com.linkedin.venice.ConfigKeys.SERVER_CONSUMER_POOL_SIZE_FOR_NON_CURRENT_VERSION_AA_WC_LEADER;
import static com.linkedin.venice.ConfigKeys.SERVER_CONSUMER_POOL_SIZE_FOR_NON_CURRENT_VERSION_NON_AA_WC_LEADER;
import static com.linkedin.venice.ConfigKeys.SERVER_CONSUMER_POOL_SIZE_PER_KAFKA_CLUSTER;
import static com.linkedin.venice.ConfigKeys.SERVER_CURRENT_VERSION_AA_WC_LEADER_QUOTA_RECORDS_PER_SECOND;
import static com.linkedin.venice.ConfigKeys.SERVER_CURRENT_VERSION_NON_AA_WC_LEADER_QUOTA_RECORDS_PER_SECOND;
import static com.linkedin.venice.ConfigKeys.SERVER_CURRENT_VERSION_SEPARATE_RT_LEADER_QUOTA_RECORDS_PER_SECOND;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_LOOKUP_QUEUE_CAPACITY;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_MEMORY_STATS_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_TRANSACTIONAL_MODE;
import static com.linkedin.venice.ConfigKeys.SERVER_DB_READ_ONLY_FOR_BATCH_ONLY_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DEBUG_LOGGING_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DEDICATED_CONSUMER_POOL_FOR_AA_WC_LEADER_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DEDICATED_CONSUMER_POOL_SIZE_FOR_AA_WC_LEADER;
import static com.linkedin.venice.ConfigKeys.SERVER_DEDICATED_DRAINER_FOR_SORTED_INPUT_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DISK_FULL_THRESHOLD;
import static com.linkedin.venice.ConfigKeys.SERVER_DISK_HEALTH_CHECK_INTERVAL_IN_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_DISK_HEALTH_CHECK_SERVICE_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DISK_HEALTH_CHECK_TIMEOUT_IN_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_ENABLE_LIVE_CONFIG_BASED_KAFKA_THROTTLING;
import static com.linkedin.venice.ConfigKeys.SERVER_ENABLE_PARALLEL_BATCH_GET;
import static com.linkedin.venice.ConfigKeys.SERVER_FORKED_PROCESS_JVM_ARGUMENT_LIST;
import static com.linkedin.venice.ConfigKeys.SERVER_GLOBAL_RT_DIV_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_HELIX_JOIN_AS_UNKNOWN;
import static com.linkedin.venice.ConfigKeys.SERVER_HTTP2_HEADER_TABLE_SIZE;
import static com.linkedin.venice.ConfigKeys.SERVER_HTTP2_INBOUND_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_HTTP2_INITIAL_WINDOW_SIZE;
import static com.linkedin.venice.ConfigKeys.SERVER_HTTP2_MAX_CONCURRENT_STREAMS;
import static com.linkedin.venice.ConfigKeys.SERVER_HTTP2_MAX_FRAME_SIZE;
import static com.linkedin.venice.ConfigKeys.SERVER_HTTP2_MAX_HEADER_LIST_SIZE;
import static com.linkedin.venice.ConfigKeys.SERVER_INCREMENTAL_PUSH_STATUS_WRITE_MODE;
import static com.linkedin.venice.ConfigKeys.SERVER_INGESTION_CHECKPOINT_DURING_GRACEFUL_SHUTDOWN_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_INGESTION_HEARTBEAT_INTERVAL_MS;
import static com.linkedin.venice.ConfigKeys.SERVER_INGESTION_ISOLATION_APPLICATION_PORT;
import static com.linkedin.venice.ConfigKeys.SERVER_INGESTION_ISOLATION_SERVICE_PORT;
import static com.linkedin.venice.ConfigKeys.SERVER_INGESTION_MODE;
import static com.linkedin.venice.ConfigKeys.SERVER_INGESTION_TASK_MAX_IDLE_COUNT;
import static com.linkedin.venice.ConfigKeys.SERVER_KAFKA_CONSUMER_OFFSET_COLLECTION_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_KAFKA_MAX_POLL_RECORDS;
import static com.linkedin.venice.ConfigKeys.SERVER_LEADER_COMPLETE_STATE_CHECK_IN_FOLLOWER_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_LEADER_COMPLETE_STATE_CHECK_IN_FOLLOWER_VALID_INTERVAL_MS;
import static com.linkedin.venice.ConfigKeys.SERVER_LEAKED_RESOURCE_CLEANUP_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_LEAKED_RESOURCE_CLEAN_UP_INTERVAL_IN_MINUTES;
import static com.linkedin.venice.ConfigKeys.SERVER_LOCAL_CONSUMER_CONFIG_PREFIX;
import static com.linkedin.venice.ConfigKeys.SERVER_MAX_REQUEST_SIZE;
import static com.linkedin.venice.ConfigKeys.SERVER_MAX_WAIT_AFTER_UNSUBSCRIBE_MS;
import static com.linkedin.venice.ConfigKeys.SERVER_MAX_WAIT_FOR_VERSION_INFO_MS_CONFIG;
import static com.linkedin.venice.ConfigKeys.SERVER_NEARLINE_WORKLOAD_PRODUCER_THROUGHPUT_OPTIMIZATION_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_NETTY_GRACEFUL_SHUTDOWN_PERIOD_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_NETTY_IDLE_TIME_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_NETTY_WORKER_THREADS;
import static com.linkedin.venice.ConfigKeys.SERVER_NODE_CAPACITY_RCU;
import static com.linkedin.venice.ConfigKeys.SERVER_NON_CURRENT_VERSION_AA_WC_LEADER_QUOTA_RECORDS_PER_SECOND;
import static com.linkedin.venice.ConfigKeys.SERVER_NON_CURRENT_VERSION_NON_AA_WC_LEADER_QUOTA_RECORDS_PER_SECOND;
import static com.linkedin.venice.ConfigKeys.SERVER_NON_EXISTING_TOPIC_CHECK_RETRY_INTERNAL_SECOND;
import static com.linkedin.venice.ConfigKeys.SERVER_NON_EXISTING_TOPIC_INGESTION_TASK_KILL_THRESHOLD_SECOND;
import static com.linkedin.venice.ConfigKeys.SERVER_NUM_SCHEMA_FAST_CLASS_WARMUP;
import static com.linkedin.venice.ConfigKeys.SERVER_OPTIMIZE_DATABASE_FOR_BACKUP_VERSION_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_OPTIMIZE_DATABASE_FOR_BACKUP_VERSION_NO_READ_THRESHOLD_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_OPTIMIZE_DATABASE_SERVICE_SCHEDULE_INTERNAL_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_PARALLEL_BATCH_GET_CHUNK_SIZE;
import static com.linkedin.venice.ConfigKeys.SERVER_PARTITION_GRACEFUL_DROP_DELAY_IN_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_PUBSUB_CONSUMER_POLL_RETRY_BACKOFF_MS;
import static com.linkedin.venice.ConfigKeys.SERVER_PUBSUB_CONSUMER_POLL_RETRY_TIMES;
import static com.linkedin.venice.ConfigKeys.SERVER_QUOTA_ENFORCEMENT_CAPACITY_MULTIPLE;
import static com.linkedin.venice.ConfigKeys.SERVER_QUOTA_ENFORCEMENT_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_QUOTA_ENFORCEMENT_INTERVAL_IN_MILLIS;
import static com.linkedin.venice.ConfigKeys.SERVER_RECORD_LEVEL_METRICS_WHEN_BOOTSTRAPPING_CURRENT_VERSION_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_REMOTE_CONSUMER_CONFIG_PREFIX;
import static com.linkedin.venice.ConfigKeys.SERVER_REMOTE_INGESTION_REPAIR_SLEEP_INTERVAL_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_RESET_ERROR_REPLICA_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_REST_SERVICE_EPOLL_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_REST_SERVICE_STORAGE_THREAD_NUM;
import static com.linkedin.venice.ConfigKeys.SERVER_RESUBSCRIPTION_TRIGGERED_BY_VERSION_INGESTION_CONTEXT_CHANGE_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_ROCKSDB_STORAGE_CONFIG_CHECK_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_ROUTER_CONNECTION_WARMING_DELAY_MS;
import static com.linkedin.venice.ConfigKeys.SERVER_SCHEMA_FAST_CLASS_WARMUP_TIMEOUT;
import static com.linkedin.venice.ConfigKeys.SERVER_SCHEMA_PRESENCE_CHECK_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_SHARED_CONSUMER_ASSIGNMENT_STRATEGY;
import static com.linkedin.venice.ConfigKeys.SERVER_SHARED_CONSUMER_NON_EXISTING_TOPIC_CLEANUP_DELAY_MS;
import static com.linkedin.venice.ConfigKeys.SERVER_SHUTDOWN_DISK_UNHEALTHY_TIME_MS;
import static com.linkedin.venice.ConfigKeys.SERVER_SOURCE_TOPIC_OFFSET_CHECK_INTERVAL_MS;
import static com.linkedin.venice.ConfigKeys.SERVER_SSL_HANDSHAKE_QUEUE_CAPACITY;
import static com.linkedin.venice.ConfigKeys.SERVER_SSL_HANDSHAKE_THREAD_POOL_SIZE;
import static com.linkedin.venice.ConfigKeys.SERVER_STOP_CONSUMPTION_TIMEOUT_IN_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_STORE_TO_EARLY_TERMINATION_THRESHOLD_MS_MAP;
import static com.linkedin.venice.ConfigKeys.SERVER_STUCK_CONSUMER_REPAIR_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_STUCK_CONSUMER_REPAIR_INTERVAL_SECOND;
import static com.linkedin.venice.ConfigKeys.SERVER_STUCK_CONSUMER_REPAIR_THRESHOLD_SECOND;
import static com.linkedin.venice.ConfigKeys.SERVER_SYSTEM_STORE_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_UNSUB_AFTER_BATCHPUSH;
import static com.linkedin.venice.ConfigKeys.SERVER_ZSTD_DICT_COMPRESSION_LEVEL;
import static com.linkedin.venice.ConfigKeys.SEVER_CALCULATE_QUOTA_USAGE_BASED_ON_PARTITIONS_ASSIGNMENT_ENABLED;
import static com.linkedin.venice.ConfigKeys.SORTED_INPUT_DRAINER_SIZE;
import static com.linkedin.venice.ConfigKeys.STORE_WRITER_BUFFER_AFTER_LEADER_LOGIC_ENABLED;
import static com.linkedin.venice.ConfigKeys.STORE_WRITER_BUFFER_MEMORY_CAPACITY;
import static com.linkedin.venice.ConfigKeys.STORE_WRITER_BUFFER_NOTIFY_DELTA;
import static com.linkedin.venice.ConfigKeys.STORE_WRITER_NUMBER;
import static com.linkedin.venice.ConfigKeys.SYSTEM_SCHEMA_CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.SYSTEM_SCHEMA_INITIALIZATION_AT_START_TIME_ENABLED;
import static com.linkedin.venice.ConfigKeys.UNREGISTER_METRIC_FOR_DELETED_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.UNSORTED_INPUT_DRAINER_SIZE;
import static com.linkedin.venice.ConfigKeys.USE_DA_VINCI_SPECIFIC_EXECUTION_STATUS_FOR_ERROR;
import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_TOPIC_MANAGER_METADATA_FETCHER_CONSUMER_POOL_SIZE_DEFAULT_VALUE;
import static com.linkedin.venice.utils.ByteUtils.BYTES_PER_MB;
import static com.linkedin.venice.utils.ByteUtils.generateHumanReadableByteCountString;

import com.github.luben.zstd.Zstd;
import com.linkedin.davinci.helix.LeaderFollowerPartitionStateModelFactory;
import com.linkedin.davinci.kafka.consumer.KafkaConsumerService;
import com.linkedin.davinci.kafka.consumer.KafkaConsumerServiceDelegator;
import com.linkedin.davinci.kafka.consumer.RemoteIngestionRepairService;
import com.linkedin.davinci.store.rocksdb.RocksDBServerConfig;
import com.linkedin.davinci.validation.KafkaDataIntegrityValidator;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.authorization.DefaultIdentityParser;
import com.linkedin.venice.exceptions.ConfigurationException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.IngestionMode;
import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.adapter.kafka.admin.ApacheKafkaAdminAdapter;
import com.linkedin.venice.throttle.VeniceRateLimiter;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.collections.MemoryBoundBlockingQueue;
import com.linkedin.venice.utils.concurrent.BlockingQueueType;
import io.netty.channel.WriteBufferWaterMark;
import java.io.File;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * VeniceServerConfig maintains configs specific to Venice Server, Da Vinci client and Isolated Ingestion Service.
 */
public class VeniceServerConfig extends VeniceClusterConfig {
  private static final Logger LOGGER = LogManager.getLogger(VeniceServerConfig.class);
  /**
   * Since the RT topic could be consumed by multiple store versions for Hybrid stores, we couldn't share the consumer across
   * different Hybrid store versions.
   * Considering there will be at most 3 store versions (backup, current and new), we need to make sure the consumer pool
   * size should be at least 3.
   */
  public static final int MINIMUM_CONSUMER_NUM_IN_CONSUMER_POOL_PER_KAFKA_CLUSTER = 3;

  private final int listenerPort;
  private final int grpcPort;
  private final boolean isGrpcEnabled;
  private final String listenerHostname;
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
   * Whether to queue writes into the {@link com.linkedin.davinci.kafka.consumer.StoreBufferService} after the
   * leader-specific logic (DCR, produce to Kafka) has been performed.
   */
  private final boolean storeWriterBufferAfterLeaderLogicEnabled;

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
   * {@link MemoryBoundBlockingQueue} won't notify the waiting thread (consumer thread)
   * right away when some message gets processed until the freed memory hit the follow config: {@link #storeWriterBufferNotifyDelta}.
   * The reason behind this design:
   * When the buffered queue is full, and the processing thread keeps processing small message, the bigger message won't
   * have chance to get queued into the buffer since the memory freed by the processed small message is not enough to
   * fit the bigger message.
   *
   * With this delta config, {@link MemoryBoundBlockingQueue} will guarantee some fairness
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
   * Rate limiter type for store version QPS rate limiter.
   */
  private VeniceRateLimiter.RateLimiterType storeVersionQpsRateLimiterType;

  /**
   * Rate limiter type for storage node.
   */
  private VeniceRateLimiter.RateLimiterType storageNodeRateLimiterType;

  /**
   * Server quota enforcement interval in seconds.
   */
  private final int quotaEnforcementIntervalInMs;

  /**
   * Server quota enforcement capacity multiple.
   */
  private final int quotaEnforcementCapacityMultiple;

  /**
   * Time interval for offset check of topic in Hybrid Store lag measurement.
   */
  private final int topicOffsetCheckIntervalMs;

  private final int topicManagerMetadataFetcherConsumerPoolSize;
  private final int topicManagerMetadataFetcherThreadPoolSize;

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
  private final boolean helixJoinAsUnknown;
  private final int grpcWorkerThreadCount;

  private final long databaseSyncBytesIntervalForTransactionalMode;

  private final long databaseSyncBytesIntervalForDeferredWriteMode;

  private final double diskFullThreshold;

  private final int partitionGracefulDropDelaySeconds;

  private final int stopConsumptionTimeoutInSeconds;
  private final long leakedResourceCleanUpIntervalInMS;

  private final boolean quotaEnforcementEnabled;

  private final boolean serverCalculateQuotaUsageBasedOnPartitionsAssignmentEnabled;

  private final long nodeCapacityInRcu;

  private final int kafkaMaxPollRecords;

  private final int pubSubConsumerPollRetryTimes;

  private final int pubSubConsumerPollRetryBackoffMs;

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

  private final Duration serverMaxWaitForVersionInfo;

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
  private final long routerConnectionWarmingDelayMs;
  private final boolean helixHybridStoreQuotaEnabled;
  private final long ssdHealthCheckShutdownTimeMs;
  private final KafkaConsumerService.ConsumerAssignmentStrategy sharedConsumerAssignmentStrategy;
  private final int consumerPoolSizePerKafkaCluster;
  private final boolean leakedResourceCleanupEnabled;

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
  private final boolean systemSchemaInitializationAtStartTimeEnabled;
  private final boolean isKMERegistrationFromMessageHeaderEnabled;
  private final String localControllerUrl;
  private final String localControllerD2ServiceName;
  private final String localD2ZkHost;

  private final boolean enableLiveConfigBasedKafkaThrottling;

  private final boolean serverIngestionCheckpointDuringGracefulShutdownEnabled;

  private final int remoteIngestionRepairSleepInterval;

  private final boolean optimizeDatabaseForBackupVersionEnabled;
  private final long optimizeDatabaseForBackupVersionNoReadThresholdMS;
  private final long optimizeDatabaseServiceScheduleIntervalSeconds;
  private final boolean unregisterMetricForDeletedStoreEnabled;
  protected final boolean readOnlyForBatchOnlyStoreEnabled; // TODO: remove this config as its never used in prod
  private final boolean resetErrorReplicaEnabled;
  private final int fastAvroFieldLimitPerMethod;

  /**
   * The number of threads used to limit the concurrency of ssl handshake for servers. The feature to use a thread pool
   * executor for handling ssl handshakes is disabled if the value of this config is <= 0. The default value is 0.
   */
  private final int sslHandshakeThreadPoolSize;

  /**
   * The queue capacity for ssl handshake threadpool executor.
   */
  private final int sslHandshakeQueueCapacity;

  private final long ingestionMemoryLimit;
  private final boolean ingestionMlockEnabled;
  private final Set<String> ingestionMemoryLimitStoreSet;
  private final List<String> forkedProcessJvmArgList;

  private final long divProducerStateMaxAgeMs;
  private final PubSubClientsFactory pubSubClientsFactory;
  private final String routerPrincipalName;

  private final int ingestionTaskMaxIdleCount;

  private final long metaStoreWriterCloseTimeoutInMS;
  private final int metaStoreWriterCloseConcurrency;

  private final boolean batchReportEOIPEnabled;
  private final IncrementalPushStatusWriteMode incrementalPushStatusWriteMode;
  private final long ingestionHeartbeatIntervalMs;
  private final boolean leaderCompleteStateCheckInFollowerEnabled;
  private final long leaderCompleteStateCheckInFollowerValidIntervalMs;
  private final boolean stuckConsumerRepairEnabled;
  private final int stuckConsumerRepairIntervalSecond;
  private final int stuckConsumerDetectionRepairThresholdSecond;
  private final int nonExistingTopicIngestionTaskKillThresholdSecond;
  private final int nonExistingTopicCheckRetryIntervalSecond;
  private final boolean dedicatedConsumerPoolForAAWCLeaderEnabled;
  private final KafkaConsumerServiceDelegator.ConsumerPoolStrategyType consumerPoolStrategyType;
  private final int consumerPoolSizeForCurrentVersionAAWCLeader;
  private final int consumerPoolSizeForCurrentVersionSepRTLeader;
  private final int consumerPoolSizeForNonCurrentVersionAAWCLeader;
  private final int consumerPoolSizeForCurrentVersionNonAAWCLeader;
  private final int consumerPoolSizeForNonCurrentVersionNonAAWCLeader;

  private final int dedicatedConsumerPoolSizeForAAWCLeader;
  private final boolean useDaVinciSpecificExecutionStatusForError;
  private final long daVinciPushStatusCheckIntervalInMs;
  private final boolean recordLevelMetricWhenBootstrappingCurrentVersionEnabled;
  private final String identityParserClassName;
  private final boolean blobTransferManagerEnabled;
  private final int snapshotRetentionTimeInMin;
  private final int maxConcurrentSnapshotUser;
  private final int blobTransferMaxTimeoutInMin;
  private final long blobTransferDisabledOffsetLagThreshold;
  private final int dvcP2pBlobTransferServerPort;
  private final int dvcP2pBlobTransferClientPort;
  private final boolean daVinciCurrentVersionBootstrappingSpeedupEnabled;
  private final long daVinciCurrentVersionBootstrappingQuotaRecordsPerSecond;
  private final long daVinciCurrentVersionBootstrappingQuotaBytesPerSecond;
  private final boolean resubscriptionTriggeredByVersionIngestionContextChangeEnabled;
  private final int defaultMaxRecordSizeBytes;
  private final int aaWCLeaderQuotaRecordsPerSecond;
  private final int currentVersionAAWCLeaderQuotaRecordsPerSecond;
  private final int currentVersionSepRTLeaderQuotaRecordsPerSecond;
  private final int currentVersionNonAAWCLeaderQuotaRecordsPerSecond;
  private final int nonCurrentVersionAAWCLeaderQuotaRecordsPerSecond;
  private final int nonCurrentVersionNonAAWCLeaderQuotaRecordsPerSecond;
  private final int channelOptionWriteBufferHighBytes;
  private final boolean aaWCWorkloadParallelProcessingEnabled;
  private final int aaWCWorkloadParallelProcessingThreadPoolSize;
  private final boolean isGlobalRtDivEnabled;
  private final boolean nearlineWorkloadProducerThroughputOptimizationEnabled;
  private final int zstdDictCompressionLevel;
  private final long maxWaitAfterUnsubscribeMs;

  public VeniceServerConfig(VeniceProperties serverProperties) throws ConfigurationException {
    this(serverProperties, Collections.emptyMap());
  }

  public VeniceServerConfig(VeniceProperties serverProperties, Map<String, Map<String, String>> kafkaClusterMap)
      throws ConfigurationException {
    super(serverProperties, kafkaClusterMap);
    listenerPort = serverProperties.getInt(LISTENER_PORT, 0);
    listenerHostname = serverProperties.getString(LISTENER_HOSTNAME, () -> Utils.getHostName());
    isGrpcEnabled = serverProperties.getBoolean(ENABLE_GRPC_READ_SERVER, false);
    grpcPort = isGrpcEnabled ? serverProperties.getInt(GRPC_READ_SERVER_PORT) : -1;

    dataBasePath = serverProperties.getString(
        DATA_BASE_PATH,
        Paths.get(System.getProperty("java.io.tmpdir"), "venice-server-data").toAbsolutePath().toString());
    autoCreateDataPath = Boolean.parseBoolean(serverProperties.getString(AUTOCREATE_DATA_PATH, "true"));
    rocksDBServerConfig = new RocksDBServerConfig(serverProperties);
    enableServerAllowList = serverProperties.getBoolean(ENABLE_SERVER_ALLOW_LIST, false);
    maxLeaderFollowerStateTransitionThreadNumber =
        serverProperties.getInt(MAX_LEADER_FOLLOWER_STATE_TRANSITION_THREAD_NUMBER, 20);

    blobTransferManagerEnabled = serverProperties.getBoolean(BLOB_TRANSFER_MANAGER_ENABLED, false);
    snapshotRetentionTimeInMin = serverProperties.getInt(BLOB_TRANSFER_SNAPSHOT_RETENTION_TIME_IN_MIN, 60);
    maxConcurrentSnapshotUser = serverProperties.getInt(BLOB_TRANSFER_MAX_CONCURRENT_SNAPSHOT_USER, 5);
    blobTransferMaxTimeoutInMin = serverProperties.getInt(BLOB_TRANSFER_MAX_TIMEOUT_IN_MIN, 60);
    blobTransferDisabledOffsetLagThreshold =
        serverProperties.getLong(BLOB_TRANSFER_DISABLED_OFFSET_LAG_THRESHOLD, 100000L);
    dvcP2pBlobTransferServerPort = serverProperties.getInt(DAVINCI_P2P_BLOB_TRANSFER_SERVER_PORT, -1);
    dvcP2pBlobTransferClientPort =
        serverProperties.getInt(DAVINCI_P2P_BLOB_TRANSFER_CLIENT_PORT, dvcP2pBlobTransferServerPort);

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

    storeWriterBufferAfterLeaderLogicEnabled =
        serverProperties.getBoolean(STORE_WRITER_BUFFER_AFTER_LEADER_LOGIC_ENABLED, true);
    // To minimize the GC impact during heavy ingestion.
    storeWriterBufferMemoryCapacity =
        serverProperties.getSizeInBytes(STORE_WRITER_BUFFER_MEMORY_CAPACITY, 10 * 1024 * 1024);
    storeWriterBufferNotifyDelta = serverProperties.getSizeInBytes(STORE_WRITER_BUFFER_NOTIFY_DELTA, 1 * 1024 * 1024);
    restServiceStorageThreadNum = serverProperties.getInt(SERVER_REST_SERVICE_STORAGE_THREAD_NUM, 16);
    serverComputeThreadNum = serverProperties.getInt(SERVER_COMPUTE_THREAD_NUM, 16);
    nettyIdleTimeInSeconds = serverProperties.getInt(SERVER_NETTY_IDLE_TIME_SECONDS, (int) TimeUnit.HOURS.toSeconds(3));
    maxRequestSize = (int) serverProperties.getSizeInBytes(SERVER_MAX_REQUEST_SIZE, 256 * 1024);
    storeVersionQpsRateLimiterType = extractRateLimiterType(
        serverProperties.getString(
            ConfigKeys.SERVER_STORE_VERSION_QPS_RATE_LIMITER,
            VeniceRateLimiter.RateLimiterType.TOKEN_BUCKET_INCREMENTAL_REFILL.name()));
    storageNodeRateLimiterType = extractRateLimiterType(
        serverProperties.getString(
            ConfigKeys.SERVER_STORAGE_NODE_RATE_LIMITER,
            VeniceRateLimiter.RateLimiterType.TOKEN_BUCKET_INCREMENTAL_REFILL.name()));
    quotaEnforcementIntervalInMs = serverProperties.getInt(SERVER_QUOTA_ENFORCEMENT_INTERVAL_IN_MILLIS, 10_000);
    quotaEnforcementCapacityMultiple = serverProperties.getInt(SERVER_QUOTA_ENFORCEMENT_CAPACITY_MULTIPLE, 5);
    topicOffsetCheckIntervalMs =
        serverProperties.getInt(SERVER_SOURCE_TOPIC_OFFSET_CHECK_INTERVAL_MS, (int) TimeUnit.SECONDS.toMillis(60));
    this.topicManagerMetadataFetcherConsumerPoolSize = serverProperties.getInt(
        PUBSUB_TOPIC_MANAGER_METADATA_FETCHER_CONSUMER_POOL_SIZE,
        PUBSUB_TOPIC_MANAGER_METADATA_FETCHER_CONSUMER_POOL_SIZE_DEFAULT_VALUE);
    this.topicManagerMetadataFetcherThreadPoolSize = serverProperties
        .getInt(PUBSUB_TOPIC_MANAGER_METADATA_FETCHER_THREAD_POOL_SIZE, topicManagerMetadataFetcherConsumerPoolSize);
    nettyGracefulShutdownPeriodSeconds = serverProperties.getInt(SERVER_NETTY_GRACEFUL_SHUTDOWN_PERIOD_SECONDS, 30);
    nettyWorkerThreadCount = serverProperties.getInt(SERVER_NETTY_WORKER_THREADS, 0);
    helixJoinAsUnknown = serverProperties.getBoolean(SERVER_HELIX_JOIN_AS_UNKNOWN, false);
    grpcWorkerThreadCount =
        serverProperties.getInt(GRPC_SERVER_WORKER_THREAD_COUNT, Runtime.getRuntime().availableProcessors());

    remoteIngestionRepairSleepInterval = serverProperties.getInt(
        SERVER_REMOTE_INGESTION_REPAIR_SLEEP_INTERVAL_SECONDS,
        RemoteIngestionRepairService.DEFAULT_REPAIR_THREAD_SLEEP_INTERVAL_SECONDS);
    readOnlyForBatchOnlyStoreEnabled =
        serverProperties.getBoolean(SERVER_DB_READ_ONLY_FOR_BATCH_ONLY_STORE_ENABLED, true);
    resetErrorReplicaEnabled = serverProperties.getBoolean(SERVER_RESET_ERROR_REPLICA_ENABLED, false);
    databaseSyncBytesIntervalForTransactionalMode =
        serverProperties.getSizeInBytes(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_TRANSACTIONAL_MODE, 32 * 1024 * 1024);
    databaseSyncBytesIntervalForDeferredWriteMode =
        serverProperties.getSizeInBytes(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE, 60 * 1024 * 1024);
    diskFullThreshold = serverProperties.getDouble(SERVER_DISK_FULL_THRESHOLD, 0.95);
    partitionGracefulDropDelaySeconds = serverProperties.getInt(SERVER_PARTITION_GRACEFUL_DROP_DELAY_IN_SECONDS, 30);
    stopConsumptionTimeoutInSeconds = serverProperties.getInt(SERVER_STOP_CONSUMPTION_TIMEOUT_IN_SECONDS, 180);
    leakedResourceCleanUpIntervalInMS =
        TimeUnit.MINUTES.toMillis(serverProperties.getLong(SERVER_LEAKED_RESOURCE_CLEAN_UP_INTERVAL_IN_MINUTES, 10));
    quotaEnforcementEnabled = serverProperties.getBoolean(SERVER_QUOTA_ENFORCEMENT_ENABLED, false);
    serverCalculateQuotaUsageBasedOnPartitionsAssignmentEnabled =
        serverProperties.getBoolean(SEVER_CALCULATE_QUOTA_USAGE_BASED_ON_PARTITIONS_ASSIGNMENT_ENABLED, true);

    nodeCapacityInRcu = serverProperties.getLong(SERVER_NODE_CAPACITY_RCU, 100000);
    kafkaMaxPollRecords = serverProperties.getInt(SERVER_KAFKA_MAX_POLL_RECORDS, 100);
    pubSubConsumerPollRetryTimes = serverProperties.getInt(SERVER_PUBSUB_CONSUMER_POLL_RETRY_TIMES, 100);
    pubSubConsumerPollRetryBackoffMs = serverProperties.getInt(SERVER_PUBSUB_CONSUMER_POLL_RETRY_BACKOFF_MS, 0);
    diskHealthCheckIntervalInMS =
        TimeUnit.SECONDS.toMillis(serverProperties.getLong(SERVER_DISK_HEALTH_CHECK_INTERVAL_IN_SECONDS, 10));
    diskHealthCheckTimeoutInMs =
        TimeUnit.SECONDS.toMillis(serverProperties.getLong(SERVER_DISK_HEALTH_CHECK_TIMEOUT_IN_SECONDS, 30));
    diskHealthCheckServiceEnabled = serverProperties.getBoolean(SERVER_DISK_HEALTH_CHECK_SERVICE_ENABLED, true);
    serverMaxWaitForVersionInfo =
        Duration.ofMillis(serverProperties.getLong(SERVER_MAX_WAIT_FOR_VERSION_INFO_MS_CONFIG, 5000));
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
    helixHybridStoreQuotaEnabled = serverProperties.getBoolean(HELIX_HYBRID_STORE_QUOTA_ENABLED, false);
    ssdHealthCheckShutdownTimeMs = serverProperties.getLong(SERVER_SHUTDOWN_DISK_UNHEALTHY_TIME_MS, 200000);
    sslHandshakeThreadPoolSize = serverProperties.getInt(SERVER_SSL_HANDSHAKE_THREAD_POOL_SIZE, 0);
    sslHandshakeQueueCapacity = serverProperties.getInt(SERVER_SSL_HANDSHAKE_QUEUE_CAPACITY, Integer.MAX_VALUE);

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
    kafkaAdminClass = serverProperties.getString(KAFKA_ADMIN_CLASS, ApacheKafkaAdminAdapter.class.getName());
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
    int minimumConsumerNumInConsumerPoolPerKafkaCluster = serverProperties.getInt(
        MIN_CONSUMER_IN_CONSUMER_POOL_PER_KAFKA_CLUSTER,
        VeniceServerConfig.MINIMUM_CONSUMER_NUM_IN_CONSUMER_POOL_PER_KAFKA_CLUSTER);

    if (consumerPoolSizePerKafkaCluster < minimumConsumerNumInConsumerPoolPerKafkaCluster) {
      throw new VeniceException(
          SERVER_CONSUMER_POOL_SIZE_PER_KAFKA_CLUSTER + " shouldn't be less than: "
              + minimumConsumerNumInConsumerPoolPerKafkaCluster + ", but it is " + consumerPoolSizePerKafkaCluster);
    }
    leakedResourceCleanupEnabled = serverProperties.getBoolean(SERVER_LEAKED_RESOURCE_CLEANUP_ENABLED, true);

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
    systemSchemaInitializationAtStartTimeEnabled =
        serverProperties.getBoolean(SYSTEM_SCHEMA_INITIALIZATION_AT_START_TIME_ENABLED, false);
    isKMERegistrationFromMessageHeaderEnabled =
        serverProperties.getBoolean(KME_REGISTRATION_FROM_MESSAGE_HEADER_ENABLED, false);
    localControllerUrl = serverProperties.getString(LOCAL_CONTROLLER_URL, "");
    localControllerD2ServiceName = serverProperties.getString(LOCAL_CONTROLLER_D2_SERVICE_NAME, "");
    localD2ZkHost = serverProperties.getString(LOCAL_D2_ZK_HOST, "");
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
    fastAvroFieldLimitPerMethod = serverProperties.getInt(FAST_AVRO_FIELD_LIMIT_PER_METHOD, 100);

    forkedProcessJvmArgList =
        Arrays.asList(serverProperties.getString(SERVER_FORKED_PROCESS_JVM_ARGUMENT_LIST, "").split(";"))
            .stream()
            .map(s -> s.trim())
            .filter(s -> s.length() > 0)
            .collect(Collectors.toList());
    ingestionMemoryLimit = extractIngestionMemoryLimit(serverProperties, ingestionMode, forkedProcessJvmArgList);
    LOGGER.debug("Ingestion memory limit: {} after subtracting other usages", ingestionMemoryLimit);
    ingestionMlockEnabled = serverProperties.getBoolean(INGESTION_MLOCK_ENABLED, false);
    if (!serverProperties.getString(INGESTION_MEMORY_LIMIT_STORE_LIST, "").isEmpty()) {
      ingestionMemoryLimitStoreSet =
          serverProperties.getList(INGESTION_MEMORY_LIMIT_STORE_LIST, Collections.emptyList())
              .stream()
              .collect(Collectors.toSet());
    } else {
      ingestionMemoryLimitStoreSet = Collections.emptySet();
    }

    divProducerStateMaxAgeMs =
        serverProperties.getLong(DIV_PRODUCER_STATE_MAX_AGE_MS, KafkaDataIntegrityValidator.DISABLED);
    pubSubClientsFactory = new PubSubClientsFactory(serverProperties);
    routerPrincipalName = serverProperties.getString(ROUTER_PRINCIPAL_NAME, "venice-router");
    ingestionTaskMaxIdleCount = serverProperties.getInt(SERVER_INGESTION_TASK_MAX_IDLE_COUNT, 10000);
    metaStoreWriterCloseTimeoutInMS = serverProperties.getLong(META_STORE_WRITER_CLOSE_TIMEOUT_MS, 300000L);
    metaStoreWriterCloseConcurrency = serverProperties.getInt(META_STORE_WRITER_CLOSE_CONCURRENCY, -1);
    ingestionHeartbeatIntervalMs =
        serverProperties.getLong(SERVER_INGESTION_HEARTBEAT_INTERVAL_MS, TimeUnit.MINUTES.toMillis(1));
    batchReportEOIPEnabled =
        serverProperties.getBoolean(SERVER_BATCH_REPORT_END_OF_INCREMENTAL_PUSH_STATUS_ENABLED, false);
    incrementalPushStatusWriteMode =
        extractIncPushStatusWriteMode(serverProperties.getString(SERVER_INCREMENTAL_PUSH_STATUS_WRITE_MODE, "DUAL"));
    stuckConsumerRepairEnabled = serverProperties.getBoolean(SERVER_STUCK_CONSUMER_REPAIR_ENABLED, true);
    stuckConsumerRepairIntervalSecond = serverProperties.getInt(SERVER_STUCK_CONSUMER_REPAIR_INTERVAL_SECOND, 60);
    stuckConsumerDetectionRepairThresholdSecond =
        serverProperties.getInt(SERVER_STUCK_CONSUMER_REPAIR_THRESHOLD_SECOND, 5 * 60); // 5 mins
    if (stuckConsumerRepairEnabled && stuckConsumerDetectionRepairThresholdSecond < stuckConsumerRepairIntervalSecond) {
      throw new VeniceException(
          "Config for " + SERVER_STUCK_CONSUMER_REPAIR_THRESHOLD_SECOND + ": "
              + stuckConsumerDetectionRepairThresholdSecond + " should be equal to or larger than "
              + SERVER_STUCK_CONSUMER_REPAIR_INTERVAL_SECOND + ": " + stuckConsumerRepairIntervalSecond);
    }
    nonExistingTopicIngestionTaskKillThresholdSecond =
        serverProperties.getInt(SERVER_NON_EXISTING_TOPIC_INGESTION_TASK_KILL_THRESHOLD_SECOND, 15 * 60); // 15 mins
    nonExistingTopicCheckRetryIntervalSecond =
        serverProperties.getInt(SERVER_NON_EXISTING_TOPIC_CHECK_RETRY_INTERNAL_SECOND, 60); // 1min
    leaderCompleteStateCheckInFollowerEnabled =
        serverProperties.getBoolean(SERVER_LEADER_COMPLETE_STATE_CHECK_IN_FOLLOWER_ENABLED, false);
    leaderCompleteStateCheckInFollowerValidIntervalMs = serverProperties
        .getLong(SERVER_LEADER_COMPLETE_STATE_CHECK_IN_FOLLOWER_VALID_INTERVAL_MS, TimeUnit.MINUTES.toMillis(5));
    dedicatedConsumerPoolForAAWCLeaderEnabled =
        serverProperties.getBoolean(SERVER_DEDICATED_CONSUMER_POOL_FOR_AA_WC_LEADER_ENABLED, false);
    consumerPoolStrategyType = KafkaConsumerServiceDelegator.ConsumerPoolStrategyType.valueOf(
        serverProperties.getString(
            SERVER_CONSUMER_POOL_ALLOCATION_STRATEGY,
            KafkaConsumerServiceDelegator.ConsumerPoolStrategyType.DEFAULT.name()));
    consumerPoolSizeForCurrentVersionAAWCLeader =
        serverProperties.getInt(SERVER_CONSUMER_POOL_SIZE_FOR_CURRENT_VERSION_AA_WC_LEADER, 10);
    consumerPoolSizeForCurrentVersionSepRTLeader =
        serverProperties.getInt(SERVER_CONSUMER_POOL_SIZE_FOR_CURRENT_VERSION_SEPARATE_RT_LEADER, 10);
    consumerPoolSizeForNonCurrentVersionAAWCLeader =
        serverProperties.getInt(SERVER_CONSUMER_POOL_SIZE_FOR_NON_CURRENT_VERSION_AA_WC_LEADER, 10);
    consumerPoolSizeForCurrentVersionNonAAWCLeader =
        serverProperties.getInt(SERVER_CONSUMER_POOL_SIZE_FOR_CURRENT_VERSION_NON_AA_WC_LEADER, 10);
    consumerPoolSizeForNonCurrentVersionNonAAWCLeader =
        serverProperties.getInt(SERVER_CONSUMER_POOL_SIZE_FOR_NON_CURRENT_VERSION_NON_AA_WC_LEADER, 10);
    dedicatedConsumerPoolSizeForAAWCLeader =
        serverProperties.getInt(SERVER_DEDICATED_CONSUMER_POOL_SIZE_FOR_AA_WC_LEADER, 5);
    useDaVinciSpecificExecutionStatusForError =
        serverProperties.getBoolean(USE_DA_VINCI_SPECIFIC_EXECUTION_STATUS_FOR_ERROR, false);
    daVinciPushStatusCheckIntervalInMs = serverProperties.getLong(DAVINCI_PUSH_STATUS_CHECK_INTERVAL_IN_MS, -1L);
    recordLevelMetricWhenBootstrappingCurrentVersionEnabled =
        serverProperties.getBoolean(SERVER_RECORD_LEVEL_METRICS_WHEN_BOOTSTRAPPING_CURRENT_VERSION_ENABLED, true);
    identityParserClassName = serverProperties.getString(IDENTITY_PARSER_CLASS, DefaultIdentityParser.class.getName());
    daVinciCurrentVersionBootstrappingSpeedupEnabled =
        serverProperties.getBoolean(DA_VINCI_CURRENT_VERSION_BOOTSTRAPPING_SPEEDUP_ENABLED, false);
    daVinciCurrentVersionBootstrappingQuotaRecordsPerSecond =
        serverProperties.getLong(DA_VINCI_CURRENT_VERSION_BOOTSTRAPPING_QUOTA_RECORDS_PER_SECOND, -1);
    daVinciCurrentVersionBootstrappingQuotaBytesPerSecond =
        serverProperties.getSizeInBytes(DA_VINCI_CURRENT_VERSION_BOOTSTRAPPING_QUOTA_BYTES_PER_SECOND, -1);
    resubscriptionTriggeredByVersionIngestionContextChangeEnabled =
        serverProperties.getBoolean(SERVER_RESUBSCRIPTION_TRIGGERED_BY_VERSION_INGESTION_CONTEXT_CHANGE_ENABLED, false);
    defaultMaxRecordSizeBytes =
        serverProperties.getInt(DEFAULT_MAX_RECORD_SIZE_BYTES, DEFAULT_MAX_RECORD_SIZE_BYTES_BACKFILL);
    if (defaultMaxRecordSizeBytes < BYTES_PER_MB) {
      throw new VeniceException(
          DEFAULT_MAX_RECORD_SIZE_BYTES + ": " + defaultMaxRecordSizeBytes + " must be at least "
              + generateHumanReadableByteCountString(BYTES_PER_MB));
    }
    aaWCLeaderQuotaRecordsPerSecond = serverProperties.getInt(SERVER_AA_WC_LEADER_QUOTA_RECORDS_PER_SECOND, -1);
    currentVersionAAWCLeaderQuotaRecordsPerSecond =
        serverProperties.getInt(SERVER_CURRENT_VERSION_AA_WC_LEADER_QUOTA_RECORDS_PER_SECOND, -1);
    currentVersionSepRTLeaderQuotaRecordsPerSecond =
        serverProperties.getInt(SERVER_CURRENT_VERSION_SEPARATE_RT_LEADER_QUOTA_RECORDS_PER_SECOND, -1);
    currentVersionNonAAWCLeaderQuotaRecordsPerSecond =
        serverProperties.getInt(SERVER_CURRENT_VERSION_NON_AA_WC_LEADER_QUOTA_RECORDS_PER_SECOND, -1);
    nonCurrentVersionAAWCLeaderQuotaRecordsPerSecond =
        serverProperties.getInt(SERVER_NON_CURRENT_VERSION_AA_WC_LEADER_QUOTA_RECORDS_PER_SECOND, -1);
    nonCurrentVersionNonAAWCLeaderQuotaRecordsPerSecond =
        serverProperties.getInt(SERVER_NON_CURRENT_VERSION_NON_AA_WC_LEADER_QUOTA_RECORDS_PER_SECOND, -1);

    // default 64KB
    channelOptionWriteBufferHighBytes = (int) serverProperties
        .getSizeInBytes(SERVER_CHANNEL_OPTION_WRITE_BUFFER_WATERMARK_HIGH_BYTES, WriteBufferWaterMark.DEFAULT.high());

    this.isGlobalRtDivEnabled = serverProperties.getBoolean(SERVER_GLOBAL_RT_DIV_ENABLED, false);
    if (channelOptionWriteBufferHighBytes <= 0) {
      throw new VeniceException("Invalid channel option write buffer high bytes: " + channelOptionWriteBufferHighBytes);
    }
    aaWCWorkloadParallelProcessingEnabled =
        serverProperties.getBoolean(SERVER_AA_WC_WORKLOAD_PARALLEL_PROCESSING_ENABLED, false);
    aaWCWorkloadParallelProcessingThreadPoolSize =
        serverProperties.getInt(SERVER_AA_WC_WORKLOAD_PARALLEL_PROCESSING_THREAD_POOL_SIZE, 8);
    nearlineWorkloadProducerThroughputOptimizationEnabled =
        serverProperties.getBoolean(SERVER_NEARLINE_WORKLOAD_PRODUCER_THROUGHPUT_OPTIMIZATION_ENABLED, true);
    zstdDictCompressionLevel =
        serverProperties.getInt(SERVER_ZSTD_DICT_COMPRESSION_LEVEL, Zstd.defaultCompressionLevel());
    if (zstdDictCompressionLevel < Zstd.minCompressionLevel()
        || zstdDictCompressionLevel > Zstd.maxCompressionLevel()) {
      throw new VeniceException(
          "Invalid zstd dict compression level: " + zstdDictCompressionLevel + " should be between "
              + Zstd.minCompressionLevel() + " and " + Zstd.maxCompressionLevel());
    }
    maxWaitAfterUnsubscribeMs =
        serverProperties.getLong(SERVER_MAX_WAIT_AFTER_UNSUBSCRIBE_MS, TimeUnit.MINUTES.toMillis(30));
  }

  long extractIngestionMemoryLimit(
      VeniceProperties serverProperties,
      IngestionMode configuredIngestionMode,
      List<String> configuredForkedProcessJvmArgList) {
    long extractedMemoryLimit = -1;
    long configuredIngestionMemoryLimit = serverProperties.getSizeInBytes(INGESTION_MEMORY_LIMIT, -1l);
    if (configuredIngestionMemoryLimit < 0) {
      return extractedMemoryLimit;
    }
    // Check whether it is being used by DaVinci or not
    if (!isDaVinciClient) {
      throw new VeniceException(
          "Config: " + INGESTION_MEMORY_LIMIT
              + " is only meaningful for DaVinci and please remove this config for Venice Server deployment");
    }
    // Check whether rocksdb is using PT or not
    if (!rocksDBServerConfig.isRocksDBPlainTableFormatEnabled()) {
      throw new VeniceException(
          "Config: " + INGESTION_MEMORY_LIMIT + " is only meaningful when using RocksDB plaintable format");
    }
    long totalMemtableUsage = rocksDBServerConfig.getRocksDBTotalMemtableUsageCapInBytes();
    // Check ingestion mode
    if (configuredIngestionMode.equals(IngestionMode.ISOLATED)) {
      /**
       * When ingestion isolation is enabled, we need to subtract the usages from the following componnets:
       * 1. Main process total memtable usage limit.
       * 2. Heap size of isolated JVM process.
       * 3. Total memtable usage limit in isolated process.
       */

      String forkedProcessHeapSizeStr = null;
      for (String s: configuredForkedProcessJvmArgList) {
        if (s.toLowerCase().startsWith("-xmx")) {
          forkedProcessHeapSizeStr = s.toLowerCase().substring(4);
          break;
        }
      }
      if (forkedProcessHeapSizeStr == null || forkedProcessHeapSizeStr.length() == 0) {
        throw new VeniceException(
            "The max heap size of isolated process needs to be configured explicitly when enabling memory limiter");
      }
      LOGGER.info("Extracted max heap size of forked process: {} ", forkedProcessHeapSizeStr);
      long forkedProcessHeapSize = VeniceProperties.convertSizeFromLiteral(forkedProcessHeapSizeStr);

      long totalMemtableUsageInForkedProcess = serverProperties.getSizeInBytes(
          INGESTION_ISOLATION_CONFIG_PREFIX + "." + ROCKSDB_TOTAL_MEMTABLE_USAGE_CAP_IN_BYTES,
          totalMemtableUsage);
      LOGGER.info(
          "Extracted total memtable table usage capacity in forked process: {}",
          totalMemtableUsageInForkedProcess);

      extractedMemoryLimit = configuredIngestionMemoryLimit - totalMemtableUsage - forkedProcessHeapSize
          - totalMemtableUsageInForkedProcess;
      if (extractedMemoryLimit <= 0) {
        throw new VeniceException(
            "Ingestion memory limit: " + extractedMemoryLimit
                + " should be positive after subtracting the usage from other components");
      }
    } else {
      // We need to subtract the memtable usage from the configured limit
      if (configuredIngestionMemoryLimit <= totalMemtableUsage) {
        throw new VeniceException(
            "Ingestion memory limit: " + configuredIngestionMemoryLimit
                + " should be bigger than total memtable usage cap: " + totalMemtableUsage);
      }
      extractedMemoryLimit = configuredIngestionMemoryLimit - totalMemtableUsage;
    }

    return extractedMemoryLimit;
  }

  private VeniceRateLimiter.RateLimiterType extractRateLimiterType(String rateLimiterTypeStr) {
    try {
      return VeniceRateLimiter.RateLimiterType.valueOf(rateLimiterTypeStr);
    } catch (IllegalArgumentException e) {
      throw new VeniceException("Invalid rate limiter type: " + rateLimiterTypeStr);
    }
  }

  public int getListenerPort() {
    return listenerPort;
  }

  public int getGrpcPort() {
    return grpcPort;
  }

  public boolean isGrpcEnabled() {
    return isGrpcEnabled;
  }

  public String getListenerHostname() {
    return listenerHostname;
  }

  public int getDvcP2pBlobTransferServerPort() {
    return dvcP2pBlobTransferServerPort;
  }

  public int getDvcP2pBlobTransferClientPort() {
    return dvcP2pBlobTransferClientPort;
  }

  public boolean isBlobTransferManagerEnabled() {
    return blobTransferManagerEnabled;
  }

  public int getMaxConcurrentSnapshotUser() {
    return maxConcurrentSnapshotUser;
  }

  public int getSnapshotRetentionTimeInMin() {
    return snapshotRetentionTimeInMin;
  }

  public int getBlobTransferMaxTimeoutInMin() {
    return blobTransferMaxTimeoutInMin;
  }

  public long getBlobTransferDisabledOffsetLagThreshold() {
    return blobTransferDisabledOffsetLagThreshold;
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

  public boolean isStoreWriterBufferAfterLeaderLogicEnabled() {
    return this.storeWriterBufferAfterLeaderLogicEnabled;
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

  public boolean isHelixJoinAsUnknownEnabled() {
    return helixJoinAsUnknown;
  }

  public int getGrpcWorkerThreadCount() {
    return grpcWorkerThreadCount;
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

  public int getStopConsumptionTimeoutInSeconds() {
    return stopConsumptionTimeoutInSeconds;
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

  public int getPubSubConsumerPollRetryTimes() {
    return pubSubConsumerPollRetryTimes;
  }

  public int getPubSubConsumerPollRetryBackoffMs() {
    return pubSubConsumerPollRetryBackoffMs;
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

  public Duration getServerMaxWaitForVersionInfo() {
    return serverMaxWaitForVersionInfo;
  }

  public BlockingQueueType getBlockingQueueType() {
    return blockingQueueType;
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

  public boolean isSystemSchemaInitializationAtStartTimeEnabled() {
    return systemSchemaInitializationAtStartTimeEnabled;
  }

  public String getLocalControllerUrl() {
    return localControllerUrl;
  }

  public String getLocalControllerD2ServiceName() {
    return localControllerD2ServiceName;
  }

  public String getLocalD2ZkHost() {
    return localD2ZkHost;
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

  public boolean isResetErrorReplicaEnabled() {
    return resetErrorReplicaEnabled;
  }

  public int getFastAvroFieldLimitPerMethod() {
    return fastAvroFieldLimitPerMethod;
  }

  public int getSslHandshakeThreadPoolSize() {
    return sslHandshakeThreadPoolSize;
  }

  public int getSslHandshakeQueueCapacity() {
    return sslHandshakeQueueCapacity;
  }

  public long getIngestionMemoryLimit() {
    return ingestionMemoryLimit;
  }

  public List<String> getForkedProcessJvmArgList() {
    return forkedProcessJvmArgList;
  }

  public boolean isIngestionMlockEnabled() {
    return ingestionMlockEnabled;
  }

  public boolean enforceMemoryLimitInStore(String storeName) {
    return ingestionMemoryLimitStoreSet.isEmpty() || ingestionMemoryLimitStoreSet.contains(storeName);
  }

  public long getDivProducerStateMaxAgeMs() {
    return this.divProducerStateMaxAgeMs;
  }

  public PubSubClientsFactory getPubSubClientsFactory() {
    return pubSubClientsFactory;
  }

  public String getRouterPrincipalName() {
    return routerPrincipalName;
  }

  public int getIngestionTaskMaxIdleCount() {
    return ingestionTaskMaxIdleCount;
  }

  public boolean isKMERegistrationFromMessageHeaderEnabled() {
    return isKMERegistrationFromMessageHeaderEnabled;
  }

  public long getMetaStoreWriterCloseTimeoutInMS() {
    return metaStoreWriterCloseTimeoutInMS;
  }

  public int getMetaStoreWriterCloseConcurrency() {
    return metaStoreWriterCloseConcurrency;
  }

  public long getIngestionHeartbeatIntervalMs() {
    return ingestionHeartbeatIntervalMs;
  }

  public boolean getBatchReportEOIPEnabled() {
    return batchReportEOIPEnabled;
  }

  public enum IncrementalPushStatusWriteMode {
    /** Write incremental push status to Zookeeper only */
    ZOOKEEPER_ONLY,

    /** Write incremental push status to push status system store only */
    PUSH_STATUS_SYSTEM_STORE_ONLY,

    /** Write incremental push status to both Zookeeper and push status system store */
    DUAL
  }

  public IncrementalPushStatusWriteMode extractIncPushStatusWriteMode(String mode) {
    try {
      return IncrementalPushStatusWriteMode.valueOf(mode);
    } catch (IllegalArgumentException e) {
      LOGGER.error("Invalid incremental push status write mode: {}. Defaulting to DUAL", mode);
      return IncrementalPushStatusWriteMode.DUAL;
    }
  }

  public IncrementalPushStatusWriteMode getIncrementalPushStatusWriteMode() {
    return incrementalPushStatusWriteMode;
  }

  public boolean isLeaderCompleteStateCheckInFollowerEnabled() {
    return leaderCompleteStateCheckInFollowerEnabled;
  }

  public long getLeaderCompleteStateCheckInFollowerValidIntervalMs() {
    return leaderCompleteStateCheckInFollowerValidIntervalMs;
  }

  public boolean isStuckConsumerRepairEnabled() {
    return stuckConsumerRepairEnabled;
  }

  public int getStuckConsumerRepairIntervalSecond() {
    return stuckConsumerRepairIntervalSecond;
  }

  public int getStuckConsumerDetectionRepairThresholdSecond() {
    return stuckConsumerDetectionRepairThresholdSecond;
  }

  public int getNonExistingTopicIngestionTaskKillThresholdSecond() {
    return nonExistingTopicIngestionTaskKillThresholdSecond;
  }

  public int getNonExistingTopicCheckRetryIntervalSecond() {
    return nonExistingTopicCheckRetryIntervalSecond;
  }

  public boolean isDedicatedConsumerPoolForAAWCLeaderEnabled() {
    return dedicatedConsumerPoolForAAWCLeaderEnabled;
  }

  public int getDedicatedConsumerPoolSizeForAAWCLeader() {
    return dedicatedConsumerPoolSizeForAAWCLeader;
  }

  public KafkaConsumerServiceDelegator.ConsumerPoolStrategyType getConsumerPoolStrategyType() {
    return consumerPoolStrategyType;
  }

  public int getConsumerPoolSizeForCurrentVersionAAWCLeader() {
    return consumerPoolSizeForCurrentVersionAAWCLeader;
  }

  public int getConsumerPoolSizeForCurrentVersionSepRTLeader() {
    return consumerPoolSizeForCurrentVersionSepRTLeader;
  }

  public int getConsumerPoolSizeForNonCurrentVersionAAWCLeader() {
    return consumerPoolSizeForNonCurrentVersionAAWCLeader;
  }

  public int getConsumerPoolSizeForCurrentVersionNonAAWCLeader() {
    return consumerPoolSizeForNonCurrentVersionNonAAWCLeader;
  }

  public int getConsumerPoolSizeForNonCurrentVersionNonAAWCLeader() {
    return consumerPoolSizeForNonCurrentVersionNonAAWCLeader;
  }

  public int getTopicManagerMetadataFetcherConsumerPoolSize() {
    return topicManagerMetadataFetcherConsumerPoolSize;
  }

  public int getTopicManagerMetadataFetcherThreadPoolSize() {
    return topicManagerMetadataFetcherThreadPoolSize;
  }

  public boolean useDaVinciSpecificExecutionStatusForError() {
    return useDaVinciSpecificExecutionStatusForError;
  }

  public long getDaVinciPushStatusCheckIntervalInMs() {
    return daVinciPushStatusCheckIntervalInMs;
  }

  public boolean isRecordLevelMetricWhenBootstrappingCurrentVersionEnabled() {
    return recordLevelMetricWhenBootstrappingCurrentVersionEnabled;
  }

  public String getIdentityParserClassName() {
    return identityParserClassName;
  }

  public int getDefaultMaxRecordSizeBytes() {
    return defaultMaxRecordSizeBytes;
  }

  public String getRocksDBPath() {
    return getDataBasePath() + File.separator + "rocksdb";
  }

  public boolean isDaVinciCurrentVersionBootstrappingSpeedupEnabled() {
    return daVinciCurrentVersionBootstrappingSpeedupEnabled;
  }

  public long getDaVinciCurrentVersionBootstrappingQuotaRecordsPerSecond() {
    return daVinciCurrentVersionBootstrappingQuotaRecordsPerSecond;
  }

  public long getDaVinciCurrentVersionBootstrappingQuotaBytesPerSecond() {
    return daVinciCurrentVersionBootstrappingQuotaBytesPerSecond;
  }

  public boolean isResubscriptionTriggeredByVersionIngestionContextChangeEnabled() {
    return resubscriptionTriggeredByVersionIngestionContextChangeEnabled;
  }

  public int getAaWCLeaderQuotaRecordsPerSecond() {
    return aaWCLeaderQuotaRecordsPerSecond;
  }

  public int getCurrentVersionAAWCLeaderQuotaRecordsPerSecond() {
    return currentVersionAAWCLeaderQuotaRecordsPerSecond;
  }

  public int getCurrentVersionSepRTLeaderQuotaRecordsPerSecond() {
    return currentVersionSepRTLeaderQuotaRecordsPerSecond;
  }

  public int getCurrentVersionNonAAWCLeaderQuotaRecordsPerSecond() {
    return currentVersionNonAAWCLeaderQuotaRecordsPerSecond;
  }

  public int getNonCurrentVersionAAWCLeaderQuotaRecordsPerSecond() {
    return nonCurrentVersionAAWCLeaderQuotaRecordsPerSecond;
  }

  public int getNonCurrentVersionNonAAWCLeaderQuotaRecordsPerSecond() {
    return nonCurrentVersionNonAAWCLeaderQuotaRecordsPerSecond;
  }

  public int getChannelOptionWriteBufferHighBytes() {
    return channelOptionWriteBufferHighBytes;
  }

  public VeniceRateLimiter.RateLimiterType getStoreVersionQpsRateLimiterType() {
    return storeVersionQpsRateLimiterType;
  }

  public VeniceRateLimiter.RateLimiterType getStorageNodeRateLimiterType() {
    return storageNodeRateLimiterType;
  }

  public int getQuotaEnforcementIntervalInMs() {
    return quotaEnforcementIntervalInMs;
  }

  public int getQuotaEnforcementCapacityMultiple() {
    return quotaEnforcementCapacityMultiple;
  }

  public boolean isAAWCWorkloadParallelProcessingEnabled() {
    return aaWCWorkloadParallelProcessingEnabled;
  }

  public int getAAWCWorkloadParallelProcessingThreadPoolSize() {
    return aaWCWorkloadParallelProcessingThreadPoolSize;
  }

  public boolean isGlobalRtDivEnabled() {
    return isGlobalRtDivEnabled;
  }

  public boolean isNearlineWorkloadProducerThroughputOptimizationEnabled() {
    return nearlineWorkloadProducerThroughputOptimizationEnabled;
  }

  public int getZstdDictCompressionLevel() {
    return zstdDictCompressionLevel;
  }

  public long getMaxWaitAfterUnsubscribeMs() {
    return maxWaitAfterUnsubscribeMs;
  }
}
