package com.linkedin.venice.controller;

import static com.linkedin.venice.CommonConfigKeys.SSL_FACTORY_CLASS_NAME;
import static com.linkedin.venice.ConfigConstants.DEFAULT_MAX_RECORD_SIZE_BYTES_BACKFILL;
import static com.linkedin.venice.ConfigConstants.DEFAULT_PUSH_STATUS_STORE_HEARTBEAT_EXPIRATION_TIME_IN_SECONDS;
import static com.linkedin.venice.ConfigKeys.ACTIVE_ACTIVE_REAL_TIME_SOURCE_FABRIC_LIST;
import static com.linkedin.venice.ConfigKeys.ADMIN_CHECK_READ_METHOD_FOR_KAFKA;
import static com.linkedin.venice.ConfigKeys.ADMIN_CONSUMPTION_CYCLE_TIMEOUT_MS;
import static com.linkedin.venice.ConfigKeys.ADMIN_CONSUMPTION_MAX_WORKER_THREAD_POOL_SIZE;
import static com.linkedin.venice.ConfigKeys.ADMIN_HELIX_MESSAGING_CHANNEL_ENABLED;
import static com.linkedin.venice.ConfigKeys.ADMIN_HOSTNAME;
import static com.linkedin.venice.ConfigKeys.ADMIN_PORT;
import static com.linkedin.venice.ConfigKeys.ADMIN_SECURE_PORT;
import static com.linkedin.venice.ConfigKeys.ADMIN_TOPIC_REPLICATION_FACTOR;
import static com.linkedin.venice.ConfigKeys.ADMIN_TOPIC_SOURCE_REGION;
import static com.linkedin.venice.ConfigKeys.AGGREGATE_REAL_TIME_SOURCE_REGION;
import static com.linkedin.venice.ConfigKeys.ALLOW_CLUSTER_WIPE;
import static com.linkedin.venice.ConfigKeys.CHILD_CLUSTER_ALLOWLIST;
import static com.linkedin.venice.ConfigKeys.CHILD_CLUSTER_D2_PREFIX;
import static com.linkedin.venice.ConfigKeys.CHILD_CLUSTER_D2_SERVICE_NAME;
import static com.linkedin.venice.ConfigKeys.CHILD_CLUSTER_URL_PREFIX;
import static com.linkedin.venice.ConfigKeys.CHILD_CLUSTER_WHITELIST;
import static com.linkedin.venice.ConfigKeys.CHILD_DATA_CENTER_KAFKA_URL_PREFIX;
import static com.linkedin.venice.ConfigKeys.CLUSTER_DISCOVERY_D2_SERVICE;
import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.CLUSTER_TO_D2;
import static com.linkedin.venice.ConfigKeys.CLUSTER_TO_SERVER_D2;
import static com.linkedin.venice.ConfigKeys.CONCURRENT_INIT_ROUTINES_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_BACKUP_VERSION_DEFAULT_RETENTION_MS;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_BACKUP_VERSION_DELETION_SLEEP_MS;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_BACKUP_VERSION_METADATA_FETCH_BASED_CLEANUP_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_BACKUP_VERSION_RETENTION_BASED_CLEANUP_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_CLUSTER;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_CLUSTER_HELIX_CLOUD_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_CLUSTER_LEADER_HAAS;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_CLUSTER_REPLICA;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_CLUSTER_ZK_ADDRESSS;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_DANGLING_TOPIC_CLEAN_UP_INTERVAL_SECOND;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_DANGLING_TOPIC_OCCURRENCE_THRESHOLD_FOR_CLEANUP;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_DEFAULT_READ_QUOTA_PER_ROUTER;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_DISABLED_REPLICA_ENABLER_INTERVAL_MS;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_DISABLED_ROUTES;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_DISABLE_PARENT_REQUEST_TOPIC_FOR_STREAM_PUSHES;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_DISABLE_PARENT_TOPIC_TRUNCATION_UPON_COMPLETION;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_EARLY_DELETE_BACKUP_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_ENABLE_DISABLED_REPLICA_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_ENFORCE_SSL;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_HAAS_SUPER_CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_HELIX_CLOUD_ID;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_HELIX_CLOUD_INFO_PROCESSOR_NAME;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_HELIX_CLOUD_INFO_PROCESSOR_PACKAGE;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_HELIX_CLOUD_INFO_SOURCES;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_HELIX_CLOUD_PROVIDER;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_HELIX_REST_CUSTOMIZED_HEALTH_URL;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_INSTANCE_TAG_LIST;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_JETTY_CONFIG_OVERRIDE_PREFIX;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_MIN_SCHEMA_COUNT_TO_KEEP;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_NAME;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_PARENT_EXTERNAL_SUPERSET_SCHEMA_GENERATION_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_PARENT_MODE;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_PARENT_REGION_STATE;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_PARENT_SYSTEM_STORE_HEARTBEAT_CHECK_WAIT_TIME_SECONDS;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_PARENT_SYSTEM_STORE_REPAIR_CHECK_INTERVAL_SECONDS;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_PARENT_SYSTEM_STORE_REPAIR_RETRY_COUNT;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_PARENT_SYSTEM_STORE_REPAIR_SERVICE_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_RESOURCE_INSTANCE_GROUP_TAG;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_SCHEMA_VALIDATION_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_SSL_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_STORAGE_CLUSTER_HELIX_CLOUD_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_STORE_GRAVEYARD_CLEANUP_DELAY_MINUTES;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_STORE_GRAVEYARD_CLEANUP_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_STORE_GRAVEYARD_CLEANUP_SLEEP_INTERVAL_BETWEEN_LIST_FETCH_MINUTES;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_SYSTEM_SCHEMA_CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_SYSTEM_STORE_ACL_SYNCHRONIZATION_DELAY_MS;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_UNUSED_SCHEMA_CLEANUP_INTERVAL_SECONDS;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_UNUSED_VALUE_SCHEMA_CLEANUP_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_ZK_SHARED_DAVINCI_PUSH_STATUS_SYSTEM_SCHEMA_STORE_AUTO_CREATION_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_ZK_SHARED_META_SYSTEM_SCHEMA_STORE_AUTO_CREATION_ENABLED;
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_SCAN_ENABLED;
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_SCAN_INTERVAL_IN_SECONDS;
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_SCAN_MAX_OFFLINE_INSTANCE_COUNT;
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_SCAN_MAX_OFFLINE_INSTANCE_RATIO;
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_SCAN_NO_REPORT_RETRY_MAX_ATTEMPTS;
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_SCAN_THREAD_NUMBER;
import static com.linkedin.venice.ConfigKeys.DEFAULT_MAX_NUMBER_OF_PARTITIONS;
import static com.linkedin.venice.ConfigKeys.DEFAULT_MAX_RECORD_SIZE_BYTES;
import static com.linkedin.venice.ConfigKeys.DEFAULT_NUMBER_OF_PARTITION;
import static com.linkedin.venice.ConfigKeys.DEFAULT_NUMBER_OF_PARTITION_FOR_HYBRID;
import static com.linkedin.venice.ConfigKeys.DEFAULT_OFFLINE_PUSH_STRATEGY;
import static com.linkedin.venice.ConfigKeys.DEFAULT_PARTITION_SIZE;
import static com.linkedin.venice.ConfigKeys.DEFAULT_READ_STRATEGY;
import static com.linkedin.venice.ConfigKeys.DEFAULT_REPLICA_FACTOR;
import static com.linkedin.venice.ConfigKeys.DEFAULT_ROUTING_STRATEGY;
import static com.linkedin.venice.ConfigKeys.DELAY_TO_REBALANCE_MS;
import static com.linkedin.venice.ConfigKeys.DEPRECATED_TOPIC_MAX_RETENTION_MS;
import static com.linkedin.venice.ConfigKeys.DEPRECATED_TOPIC_RETENTION_MS;
import static com.linkedin.venice.ConfigKeys.EMERGENCY_SOURCE_REGION;
import static com.linkedin.venice.ConfigKeys.ENABLE_ACTIVE_ACTIVE_REPLICATION_AS_DEFAULT_FOR_HYBRID_STORE;
import static com.linkedin.venice.ConfigKeys.ENABLE_HYBRID_PUSH_SSL_ALLOWLIST;
import static com.linkedin.venice.ConfigKeys.ENABLE_HYBRID_PUSH_SSL_WHITELIST;
import static com.linkedin.venice.ConfigKeys.ENABLE_INCREMENTAL_PUSH_FOR_HYBRID_ACTIVE_ACTIVE_USER_STORES;
import static com.linkedin.venice.ConfigKeys.ENABLE_OFFLINE_PUSH_SSL_ALLOWLIST;
import static com.linkedin.venice.ConfigKeys.ENABLE_OFFLINE_PUSH_SSL_WHITELIST;
import static com.linkedin.venice.ConfigKeys.ENABLE_PARTIAL_UPDATE_FOR_HYBRID_ACTIVE_ACTIVE_USER_STORES;
import static com.linkedin.venice.ConfigKeys.ENABLE_PARTIAL_UPDATE_FOR_HYBRID_NON_ACTIVE_ACTIVE_USER_STORES;
import static com.linkedin.venice.ConfigKeys.ENABLE_PARTITION_COUNT_ROUND_UP;
import static com.linkedin.venice.ConfigKeys.ENABLE_SEPARATE_REAL_TIME_TOPIC_FOR_STORE_WITH_INCREMENTAL_PUSH;
import static com.linkedin.venice.ConfigKeys.ERROR_PARTITION_AUTO_RESET_LIMIT;
import static com.linkedin.venice.ConfigKeys.ERROR_PARTITION_PROCESSING_CYCLE_DELAY;
import static com.linkedin.venice.ConfigKeys.FATAL_DATA_VALIDATION_FAILURE_TOPIC_RETENTION_MS;
import static com.linkedin.venice.ConfigKeys.FORCE_LEADER_ERROR_REPLICA_FAIL_OVER_ENABLED;
import static com.linkedin.venice.ConfigKeys.HELIX_REBALANCE_ALG;
import static com.linkedin.venice.ConfigKeys.HELIX_SEND_MESSAGE_TIMEOUT_MS;
import static com.linkedin.venice.ConfigKeys.IDENTITY_PARSER_CLASS;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.KAFKA_LOG_COMPACTION_FOR_HYBRID_STORES;
import static com.linkedin.venice.ConfigKeys.KAFKA_MIN_IN_SYNC_REPLICAS;
import static com.linkedin.venice.ConfigKeys.KAFKA_MIN_IN_SYNC_REPLICAS_ADMIN_TOPICS;
import static com.linkedin.venice.ConfigKeys.KAFKA_MIN_IN_SYNC_REPLICAS_RT_TOPICS;
import static com.linkedin.venice.ConfigKeys.KAFKA_OVER_SSL;
import static com.linkedin.venice.ConfigKeys.KAFKA_REPLICATION_FACTOR;
import static com.linkedin.venice.ConfigKeys.KAFKA_REPLICATION_FACTOR_RT_TOPICS;
import static com.linkedin.venice.ConfigKeys.KAFKA_SECURITY_PROTOCOL;
import static com.linkedin.venice.ConfigKeys.KME_REGISTRATION_FROM_MESSAGE_HEADER_ENABLED;
import static com.linkedin.venice.ConfigKeys.LEAKED_PUSH_STATUS_CLEAN_UP_SERVICE_SLEEP_INTERVAL_MS;
import static com.linkedin.venice.ConfigKeys.LEAKED_RESOURCE_ALLOWED_LINGER_TIME_MS;
import static com.linkedin.venice.ConfigKeys.META_STORE_WRITER_CLOSE_CONCURRENCY;
import static com.linkedin.venice.ConfigKeys.META_STORE_WRITER_CLOSE_TIMEOUT_MS;
import static com.linkedin.venice.ConfigKeys.MIN_NUMBER_OF_STORE_VERSIONS_TO_PRESERVE;
import static com.linkedin.venice.ConfigKeys.MIN_NUMBER_OF_UNUSED_KAFKA_TOPICS_TO_PRESERVE;
import static com.linkedin.venice.ConfigKeys.MULTI_REGION;
import static com.linkedin.venice.ConfigKeys.NATIVE_REPLICATION_FABRIC_ALLOWLIST;
import static com.linkedin.venice.ConfigKeys.NATIVE_REPLICATION_FABRIC_WHITELIST;
import static com.linkedin.venice.ConfigKeys.NATIVE_REPLICATION_SOURCE_FABRIC;
import static com.linkedin.venice.ConfigKeys.NATIVE_REPLICATION_SOURCE_FABRIC_AS_DEFAULT_FOR_BATCH_ONLY_STORES;
import static com.linkedin.venice.ConfigKeys.NATIVE_REPLICATION_SOURCE_FABRIC_AS_DEFAULT_FOR_HYBRID_STORES;
import static com.linkedin.venice.ConfigKeys.OFFLINE_JOB_START_TIMEOUT_MS;
import static com.linkedin.venice.ConfigKeys.PARENT_CONTROLLER_MAX_ERRORED_TOPIC_NUM_TO_KEEP;
import static com.linkedin.venice.ConfigKeys.PARENT_CONTROLLER_WAITING_TIME_FOR_CONSUMPTION_MS;
import static com.linkedin.venice.ConfigKeys.PARENT_KAFKA_CLUSTER_FABRIC_LIST;
import static com.linkedin.venice.ConfigKeys.PARTICIPANT_MESSAGE_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.PARTITION_COUNT_ROUND_UP_SIZE;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.PUBSUB_TOPIC_MANAGER_METADATA_FETCHER_CONSUMER_POOL_SIZE;
import static com.linkedin.venice.ConfigKeys.PUBSUB_TOPIC_MANAGER_METADATA_FETCHER_THREAD_POOL_SIZE;
import static com.linkedin.venice.ConfigKeys.PUSH_JOB_FAILURE_CHECKPOINTS_TO_DEFINE_USER_ERROR;
import static com.linkedin.venice.ConfigKeys.PUSH_JOB_STATUS_STORE_CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.PUSH_SSL_ALLOWLIST;
import static com.linkedin.venice.ConfigKeys.PUSH_SSL_WHITELIST;
import static com.linkedin.venice.ConfigKeys.PUSH_STATUS_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.PUSH_STATUS_STORE_HEARTBEAT_EXPIRATION_TIME_IN_SECONDS;
import static com.linkedin.venice.ConfigKeys.REFRESH_ATTEMPTS_FOR_ZK_RECONNECT;
import static com.linkedin.venice.ConfigKeys.REFRESH_INTERVAL_FOR_ZK_RECONNECT_MS;
import static com.linkedin.venice.ConfigKeys.REPLICATION_METADATA_VERSION;
import static com.linkedin.venice.ConfigKeys.SERVICE_DISCOVERY_REGISTRATION_RETRY_MS;
import static com.linkedin.venice.ConfigKeys.SSL_KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.SSL_TO_KAFKA_LEGACY;
import static com.linkedin.venice.ConfigKeys.STORAGE_ENGINE_OVERHEAD_RATIO;
import static com.linkedin.venice.ConfigKeys.SYSTEM_SCHEMA_INITIALIZATION_AT_START_TIME_ENABLED;
import static com.linkedin.venice.ConfigKeys.TERMINAL_STATE_TOPIC_CHECK_DELAY_MS;
import static com.linkedin.venice.ConfigKeys.TOPIC_CLEANUP_DELAY_FACTOR;
import static com.linkedin.venice.ConfigKeys.TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS;
import static com.linkedin.venice.ConfigKeys.UNREGISTER_METRIC_FOR_DELETED_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.USE_DA_VINCI_SPECIFIC_EXECUTION_STATUS_FOR_ERROR;
import static com.linkedin.venice.ConfigKeys.USE_PUSH_STATUS_STORE_FOR_INCREMENTAL_PUSH;
import static com.linkedin.venice.ConfigKeys.VENICE_STORAGE_CLUSTER_LEADER_HAAS;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;
import static com.linkedin.venice.PushJobCheckpoints.DEFAULT_PUSH_JOB_USER_ERROR_CHECKPOINTS;
import static com.linkedin.venice.SSLConfig.DEFAULT_CONTROLLER_SSL_ENABLED;
import static com.linkedin.venice.VeniceConstants.DEFAULT_PER_ROUTER_READ_QUOTA;
import static com.linkedin.venice.VeniceConstants.DEFAULT_SSL_FACTORY_CLASS_NAME;
import static com.linkedin.venice.controller.ParentControllerRegionState.ACTIVE;
import static com.linkedin.venice.pubsub.PubSubConstants.DEFAULT_KAFKA_REPLICATION_FACTOR;
import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_TOPIC_MANAGER_METADATA_FETCHER_CONSUMER_POOL_SIZE_DEFAULT_VALUE;
import static com.linkedin.venice.utils.ByteUtils.BYTES_PER_MB;
import static com.linkedin.venice.utils.ByteUtils.generateHumanReadableByteCountString;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.PushJobCheckpoints;
import com.linkedin.venice.SSLConfig;
import com.linkedin.venice.authorization.DefaultIdentityParser;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.controllerapi.ControllerRoute;
import com.linkedin.venice.exceptions.ConfigurationException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.pubsub.PubSubAdminAdapterFactory;
import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.api.PubSubSecurityProtocol;
import com.linkedin.venice.pushmonitor.LeakedPushStatusCleanUpService;
import com.linkedin.venice.status.BatchJobHeartbeatConfigs;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.KafkaSSLUtils;
import com.linkedin.venice.utils.RegionUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.helix.cloud.constants.CloudProvider;
import org.apache.helix.controller.rebalancer.strategy.CrushRebalanceStrategy;
import org.apache.helix.model.CloudConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Configuration which is specific to a Venice controller.
 */
public class VeniceControllerClusterConfig {
  private static final Logger LOGGER = LogManager.getLogger(VeniceControllerClusterConfig.class);
  private static final String LIST_SEPARATOR = ",\\s*";

  private final VeniceProperties props;
  private final String clusterName;
  private final String zkAddress;
  private final String controllerName;
  private final String adminHostname;
  private final int adminPort;
  private final int adminSecurePort;
  private final int controllerClusterReplica;
  // Name of the Helix cluster for controllers
  private final String controllerClusterName;
  private final String controllerClusterZkAddress;
  private final String controllerResourceInstanceGroupTag;
  private final List<String> controllerInstanceTagList;
  private final boolean multiRegion;
  private final boolean parent;
  private final ParentControllerRegionState parentControllerRegionState;
  private final Map<String, String> childDataCenterControllerUrlMap;
  private final String d2ServiceName;
  private final String clusterDiscoveryD2ServiceName;
  private final Map<String, String> childDataCenterControllerD2Map;
  private final int parentControllerWaitingTimeForConsumptionMs;
  private final String batchJobHeartbeatStoreCluster;// Name of cluster where the batch job liveness heartbeat store
                                                     // should exist.
  private final boolean batchJobHeartbeatEnabled; // whether the controller is enabled to use batch job liveness
                                                  // heartbeat.
  private final Duration batchJobHeartbeatTimeout;
  private final Duration batchJobHeartbeatInitialBufferTime;
  private final long adminConsumptionCycleTimeoutMs;
  private final int adminConsumptionMaxWorkerThreadPoolSize;
  private final double storageEngineOverheadRatio;
  private final long deprecatedJobTopicRetentionMs;

  private final long fatalDataValidationFailureRetentionMs;
  private final long deprecatedJobTopicMaxRetentionMs;
  private final long topicCleanupSleepIntervalBetweenTopicListFetchMs;

  private final long disabledReplicaEnablerServiceIntervalMs;

  private final int topicCleanupDelayFactor;
  private final int topicManagerMetadataFetcherConsumerPoolSize;
  private final int topicManagerMetadataFetcherThreadPoolSize;
  private final int minNumberOfUnusedKafkaTopicsToPreserve;
  private final int minNumberOfStoreVersionsToPreserve;
  private final int parentControllerMaxErroredTopicNumToKeep;
  private final String pushJobStatusStoreClusterName;
  private final boolean participantMessageStoreEnabled;
  private final String systemSchemaClusterName;
  private final boolean adminHelixMessagingChannelEnabled;
  private final boolean isControllerClusterLeaderHAAS;
  private final boolean isVeniceClusterLeaderHAAS;
  private final String controllerHAASSuperClusterName;
  private final boolean earlyDeleteBackUpEnabled;
  private final boolean adminCheckReadMethodForKafka;
  private final Map<String, String> childDataCenterKafkaUrlMap;
  private final List<String> activeActiveRealTimeSourceKafkaURLs;
  private final String nativeReplicationSourceFabric;
  private final int errorPartitionAutoResetLimit;
  private final long errorPartitionProcessingCycleDelay;
  private final long backupVersionDefaultRetentionMs;
  private final long backupVersionCleanupSleepMs;

  private final boolean backupVersionRetentionBasedCleanupEnabled;
  private final boolean backupVersionMetadataFetchBasedCleanupEnabled;

  private final boolean enforceSSLOnly;
  private final long terminalStateTopicCheckerDelayMs;
  private final List<ControllerRoute> disabledRoutes;
  /**
   * Test only config used to disable parent topic truncation upon job completion. This is needed because kafka cluster
   * in test environment is shared between parent and child controllers. Truncating topic upon completion will confuse
   * child controllers in certain scenarios.
   */
  private final boolean disableParentTopicTruncationUponCompletion;
  private final Set<String> parentFabrics;
  private final boolean zkSharedMetaSystemSchemaStoreAutoCreationEnabled;
  /**
   * To decide whether to initialize push status store related components.
   */
  private final boolean isDaVinciPushStatusStoreEnabled;

  private final boolean daVinciPushStatusScanEnabled;
  private final int daVinciPushStatusScanIntervalInSeconds;

  private final int daVinciPushStatusScanThreadNumber;

  private final boolean enableDisabledReplicaEnabled;

  private final int daVinciPushStatusScanNoReportRetryMaxAttempt;

  private final int daVinciPushStatusScanMaxOfflineInstanceCount;

  private final double daVinciPushStatusScanMaxOfflineInstanceRatio;

  private final boolean zkSharedDaVinciPushStatusSystemSchemaStoreAutoCreationEnabled;

  /**
   * Used to decide if an instance is stale.
   */
  private final long pushStatusStoreHeartbeatExpirationTimeInSeconds;
  private final long systemStoreAclSynchronizationDelayMs;

  /**
   * Region name from the controller config, which might not be the same region name inside LI. The region name can
   * be used by controller admin commands.
   */
  private final String regionName;

  /**
   * A config flag to decide whether child controllers will consume remotely from the source admin topic.
   */
  private final boolean adminTopicRemoteConsumptionEnabled;

  /**
   * Region name of the source admin topic.
   */
  private final String adminTopicSourceRegion;

  /**
   * Region name of aggregate hybrid store real-time data when native replication is enabled.
   */
  private final String aggregateRealTimeSourceRegion;

  /**
   * Automatically perform empty push to create a new version for corresponding meta system store upon new user store
   * creation.
   */
  private final boolean isAutoMaterializeMetaSystemStoreEnabled;

  /**
   * Automatically perform empty push to create a new version for corresponding da-vinci push status system store upon
   * new user store creation.
   */
  private final boolean isAutoMaterializeDaVinciPushStatusSystemStoreEnabled;

  private final String emergencySourceRegion;

  private final boolean allowClusterWipe;

  private final boolean concurrentInitRoutinesEnabled;

  private final boolean controllerClusterHelixCloudEnabled;
  private final boolean storageClusterHelixCloudEnabled;
  private final CloudConfig helixCloudConfig;

  private final String helixRestCustomizedHealthUrl;

  private final boolean usePushStatusStoreForIncrementalPushStatusReads;

  private final long metaStoreWriterCloseTimeoutInMS;

  private final int metaStoreWriterCloseConcurrency;

  private final boolean unregisterMetricForDeletedStoreEnabled;

  private final String identityParserClassName;

  private final boolean storeGraveyardCleanupEnabled;

  private final int storeGraveyardCleanupDelayMinutes;

  private final int storeGraveyardCleanupSleepIntervalBetweenListFetchMinutes;

  private final boolean parentSystemStoreRepairServiceEnabled;

  private final int parentSystemStoreRepairCheckIntervalSeconds;

  private final int parentSystemStoreHeartbeatCheckWaitTimeSeconds;

  private final int parentSystemStoreRepairRetryCount;

  private final boolean parentExternalSupersetSchemaGenerationEnabled;

  private final boolean systemSchemaInitializationAtStartTimeEnabled;

  private final boolean isKMERegistrationFromMessageHeaderEnabled;
  private final boolean unusedValueSchemaCleanupServiceEnabled;

  private final int unusedSchemaCleanupIntervalSeconds;

  private final int minSchemaCountToKeep;
  private final boolean useDaVinciSpecificExecutionStatusForError;
  private final PubSubClientsFactory pubSubClientsFactory;

  private final PubSubAdminAdapterFactory sourceOfTruthAdminAdapterFactory;

  private final long danglingTopicCleanupIntervalSeconds;
  private final int danglingTopicOccurrenceThresholdForCleanup;

  private final PersistenceType persistenceType;
  private final ReadStrategy readStrategy;
  private final OfflinePushStrategy offlinePushStrategy;
  private final RoutingStrategy routingStrategy;
  private final int replicationFactor;
  private final int minNumberOfPartitions;
  private final int minNumberOfPartitionsForHybrid;
  private final int maxNumberOfPartitions;
  private final long partitionSize;
  private final boolean partitionCountRoundUpEnabled;
  private final int partitionCountRoundUpSize;
  private final long offLineJobWaitTimeInMilliseconds;
  private final Map<String, String> clusterToD2Map;
  private final Map<String, String> clusterToServerD2Map;
  private final boolean sslToKafka;
  private final int helixSendMessageTimeoutMilliseconds;
  private final int adminTopicReplicationFactor;

  private final String kafkaSecurityProtocol;
  // SSL related config
  private final Optional<SSLConfig> sslConfig;
  private final String sslFactoryClassName;
  private final int refreshAttemptsForZkReconnect;
  private final long refreshIntervalForZkReconnectInMs;
  private final boolean enableOfflinePushSSLAllowlist;
  private final boolean enableNearlinePushSSLAllowlist;
  private final List<String> pushSSLAllowlist;

  private final String nativeReplicationSourceFabricAsDefaultForBatchOnly;
  private final String nativeReplicationSourceFabricAsDefaultForHybrid;

  /**
   * When the following option is enabled, active-active enabled new user hybrid store will automatically
   * have incremental push enabled.
   */
  private final boolean enabledIncrementalPushForHybridActiveActiveUserStores;

  /**
   * When the following option is enabled, new user hybrid store with incremental push enabled will automatically
   * have separate real time topic enabled.
   */
  private final boolean enabledSeparateRealTimeTopicForStoreWithIncrementalPush;

  private final boolean enablePartialUpdateForHybridActiveActiveUserStores;
  private final boolean enablePartialUpdateForHybridNonActiveActiveUserStores;

  /**
   * When this option is enabled, all new hybrid stores will have active-active replication enabled in store config so long
   * as the store has leader follower also enabled.
   */
  private final boolean activeActiveReplicationEnabledAsDefaultForHybrid;

  /**
   * When this option is enabled, new schema registration will validate the schema against all existing store value schemas.
   */
  private final boolean controllerSchemaValidationEnabled;

  /**
   * After server disconnecting for delayToRebalanceMS, helix would trigger the re-balance immediately.
   */
  private final long delayToRebalanceMS;

  /**
   * kafka Bootstrap Urls . IF there is more than one url, they are separated by commas
   */
  private final String kafkaBootstrapServers;

  private final String sslKafkaBootStrapServers;

  /**
   * Number of replicas for each kafka topic. It can be different from the Venice Storage Node replication factor,
   * defined by {@value com.linkedin.venice.ConfigKeys#DEFAULT_REPLICA_FACTOR}.
   */
  private final int kafkaReplicationFactor;
  private final int kafkaReplicationFactorRTTopics;
  private final Optional<Integer> minInSyncReplicas;
  private final Optional<Integer> minInSyncReplicasRealTimeTopics;
  private final Optional<Integer> minInSyncReplicasAdminTopics;
  private final boolean kafkaLogCompactionForHybridStores;

  /**
   * Alg used by helix to decide the mapping between replicas and nodes.
   */
  private final String helixRebalanceAlg;

  /**
   * Sleep interval inside {@link LeakedPushStatusCleanUpService}
   */
  private final long leakedPushStatusCleanUpServiceSleepIntervalInMs;

  /**
   * The amount of time a leaked resource is allowed to linger before it is cleaned up.
   *
   * A leaked resource per store is allowed to linger for some time in order to gather troubleshooting information.
   */
  private final long leakedResourceAllowedLingerTimeInMs;

  /**
   * Jetty config overrides for Spark server
   */
  private final VeniceProperties jettyConfigOverrides;

  /**
   * Config which disables request_topic calls to the parent controller for stream pushes.  This is meant to discourage
   * the use of the parent colo for aggregating pushed data, users should instead push to their local colo and allow
   * Venice AA to aggregate the data.
   */
  private final boolean disableParentRequestTopicForStreamPushes;

  private final int defaultReadQuotaPerRouter;

  private final int defaultMaxRecordSizeBytes; // default value for VeniceWriter.maxRecordSizeBytes
  private final int replicationMetadataVersion;

  private final boolean errorLeaderReplicaFailOverEnabled;

  private final Set<String> childDatacenters;
  private final long serviceDiscoveryRegistrationRetryMS;

  private Set<PushJobCheckpoints> pushJobUserErrorCheckpoints;
  private boolean isHybridStorePartitionCountUpdateEnabled;

  public VeniceControllerClusterConfig(VeniceProperties props) {
    this.props = props;
    this.clusterName = props.getString(CLUSTER_NAME);
    this.zkAddress = props.getString(ZOOKEEPER_ADDRESS);
    this.controllerName = props.getString(CONTROLLER_NAME);
    this.kafkaReplicationFactor = props.getInt(KAFKA_REPLICATION_FACTOR, DEFAULT_KAFKA_REPLICATION_FACTOR);
    this.kafkaReplicationFactorRTTopics = props.getInt(KAFKA_REPLICATION_FACTOR_RT_TOPICS, kafkaReplicationFactor);
    this.minInSyncReplicas = props.getOptionalInt(KAFKA_MIN_IN_SYNC_REPLICAS);
    this.minInSyncReplicasRealTimeTopics = props.getOptionalInt(KAFKA_MIN_IN_SYNC_REPLICAS_RT_TOPICS);
    this.minInSyncReplicasAdminTopics = props.getOptionalInt(KAFKA_MIN_IN_SYNC_REPLICAS_ADMIN_TOPICS);
    this.kafkaLogCompactionForHybridStores = props.getBoolean(KAFKA_LOG_COMPACTION_FOR_HYBRID_STORES, true);
    this.replicationFactor = props.getInt(DEFAULT_REPLICA_FACTOR);
    this.minNumberOfPartitions = props.getInt(DEFAULT_NUMBER_OF_PARTITION);
    this.minNumberOfPartitionsForHybrid = props.getInt(DEFAULT_NUMBER_OF_PARTITION_FOR_HYBRID, minNumberOfPartitions);
    this.kafkaBootstrapServers = props.getString(KAFKA_BOOTSTRAP_SERVERS);
    this.partitionSize = props.getSizeInBytes(DEFAULT_PARTITION_SIZE);
    this.maxNumberOfPartitions = props.getInt(DEFAULT_MAX_NUMBER_OF_PARTITIONS);
    this.partitionCountRoundUpEnabled = props.getBoolean(ENABLE_PARTITION_COUNT_ROUND_UP, false);
    this.partitionCountRoundUpSize = props.getInt(PARTITION_COUNT_ROUND_UP_SIZE, 1);
    // If the timeout is longer than 3min, we need to update controller client's timeout as well, otherwise creating
    // version would fail.
    this.offLineJobWaitTimeInMilliseconds = props.getLong(OFFLINE_JOB_START_TIMEOUT_MS, 120000);
    // By default, delayed rebalance is disabled.
    this.delayToRebalanceMS = props.getLong(DELAY_TO_REBALANCE_MS, 0);
    if (props.containsKey(PERSISTENCE_TYPE)) {
      this.persistenceType = PersistenceType.valueOf(props.getString(PERSISTENCE_TYPE));
    } else {
      this.persistenceType = PersistenceType.IN_MEMORY;
    }
    if (props.containsKey(DEFAULT_READ_STRATEGY)) {
      this.readStrategy = ReadStrategy.valueOf(props.getString(DEFAULT_READ_STRATEGY));
    } else {
      this.readStrategy = ReadStrategy.ANY_OF_ONLINE;
    }
    if (props.containsKey(DEFAULT_OFFLINE_PUSH_STRATEGY)) {
      this.offlinePushStrategy = OfflinePushStrategy.valueOf(props.getString(DEFAULT_OFFLINE_PUSH_STRATEGY));
    } else {
      this.offlinePushStrategy = OfflinePushStrategy.WAIT_ALL_REPLICAS;
    }
    if (props.containsKey(DEFAULT_ROUTING_STRATEGY)) {
      this.routingStrategy = RoutingStrategy.valueOf(props.getString(DEFAULT_ROUTING_STRATEGY));
    } else {
      this.routingStrategy = RoutingStrategy.CONSISTENT_HASH;
    }

    this.nativeReplicationSourceFabricAsDefaultForBatchOnly =
        props.getString(NATIVE_REPLICATION_SOURCE_FABRIC_AS_DEFAULT_FOR_BATCH_ONLY_STORES, "");
    this.nativeReplicationSourceFabricAsDefaultForHybrid =
        props.getString(NATIVE_REPLICATION_SOURCE_FABRIC_AS_DEFAULT_FOR_HYBRID_STORES, "");
    this.activeActiveReplicationEnabledAsDefaultForHybrid =
        props.getBoolean(ENABLE_ACTIVE_ACTIVE_REPLICATION_AS_DEFAULT_FOR_HYBRID_STORE, false);
    this.controllerSchemaValidationEnabled = props.getBoolean(CONTROLLER_SCHEMA_VALIDATION_ENABLED, true);
    this.enabledIncrementalPushForHybridActiveActiveUserStores =
        props.getBoolean(ENABLE_INCREMENTAL_PUSH_FOR_HYBRID_ACTIVE_ACTIVE_USER_STORES, false);
    this.enabledSeparateRealTimeTopicForStoreWithIncrementalPush =
        props.getBoolean(ENABLE_SEPARATE_REAL_TIME_TOPIC_FOR_STORE_WITH_INCREMENTAL_PUSH, false);
    this.enablePartialUpdateForHybridActiveActiveUserStores =
        props.getBoolean(ENABLE_PARTIAL_UPDATE_FOR_HYBRID_ACTIVE_ACTIVE_USER_STORES, false);
    this.enablePartialUpdateForHybridNonActiveActiveUserStores =
        props.getBoolean(ENABLE_PARTIAL_UPDATE_FOR_HYBRID_NON_ACTIVE_ACTIVE_USER_STORES, false);

    this.clusterToD2Map = props.getMap(CLUSTER_TO_D2);
    this.clusterToServerD2Map = props.getMap(CLUSTER_TO_SERVER_D2, Collections.emptyMap());
    this.sslToKafka = props.getBooleanWithAlternative(KAFKA_OVER_SSL, SSL_TO_KAFKA_LEGACY, false);
    // In case ssl to kafka is enabled, ssl kafka broker list is a mandatory field
    this.sslKafkaBootStrapServers = sslToKafka ? props.getString(SSL_KAFKA_BOOTSTRAP_SERVERS) : null;
    this.helixSendMessageTimeoutMilliseconds = props.getInt(HELIX_SEND_MESSAGE_TIMEOUT_MS, 10000);

    this.kafkaSecurityProtocol = props.getString(KAFKA_SECURITY_PROTOCOL, PubSubSecurityProtocol.PLAINTEXT.name());
    if (!KafkaSSLUtils.isKafkaProtocolValid(kafkaSecurityProtocol)) {
      throw new ConfigurationException("Invalid kafka security protocol: " + kafkaSecurityProtocol);
    }
    if (doesControllerNeedsSslConfig()) {
      this.sslConfig = Optional.of(new SSLConfig(props));
    } else {
      this.sslConfig = Optional.empty();
    }
    this.sslFactoryClassName = props.getString(SSL_FACTORY_CLASS_NAME, DEFAULT_SSL_FACTORY_CLASS_NAME);
    this.refreshAttemptsForZkReconnect = props.getInt(REFRESH_ATTEMPTS_FOR_ZK_RECONNECT, 3);
    this.refreshIntervalForZkReconnectInMs =
        props.getLong(REFRESH_INTERVAL_FOR_ZK_RECONNECT_MS, java.util.concurrent.TimeUnit.SECONDS.toMillis(10));
    this.enableOfflinePushSSLAllowlist = props.getBooleanWithAlternative(
        ENABLE_OFFLINE_PUSH_SSL_ALLOWLIST,
        // go/inclusivecode deferred(Reference will be removed when clients have migrated)
        ENABLE_OFFLINE_PUSH_SSL_WHITELIST,
        true);
    this.enableNearlinePushSSLAllowlist = props.getBooleanWithAlternative(
        ENABLE_HYBRID_PUSH_SSL_ALLOWLIST,
        // go/inclusivecode deferred(Reference will be removed when clients have migrated)
        ENABLE_HYBRID_PUSH_SSL_WHITELIST,
        true);
    this.pushSSLAllowlist = props.getListWithAlternative(PUSH_SSL_ALLOWLIST, PUSH_SSL_WHITELIST, new ArrayList<>());
    this.helixRebalanceAlg = props.getString(HELIX_REBALANCE_ALG, CrushRebalanceStrategy.class.getName());
    this.adminTopicReplicationFactor = props.getInt(ADMIN_TOPIC_REPLICATION_FACTOR, 3);
    if (adminTopicReplicationFactor < 1) {
      throw new ConfigurationException(ADMIN_TOPIC_REPLICATION_FACTOR + " cannot be less than 1.");
    }
    this.leakedPushStatusCleanUpServiceSleepIntervalInMs =
        props.getLong(LEAKED_PUSH_STATUS_CLEAN_UP_SERVICE_SLEEP_INTERVAL_MS, TimeUnit.MINUTES.toMillis(15));
    // 7 days should be enough for detecting and troubleshooting a leaked push status.
    this.leakedResourceAllowedLingerTimeInMs =
        props.getLong(LEAKED_RESOURCE_ALLOWED_LINGER_TIME_MS, TimeUnit.DAYS.toMillis(7));
    this.jettyConfigOverrides = props.clipAndFilterNamespace(CONTROLLER_JETTY_CONFIG_OVERRIDE_PREFIX);
    this.disableParentRequestTopicForStreamPushes =
        props.getBoolean(CONTROLLER_DISABLE_PARENT_REQUEST_TOPIC_FOR_STREAM_PUSHES, false);
    this.defaultReadQuotaPerRouter =
        props.getInt(CONTROLLER_DEFAULT_READ_QUOTA_PER_ROUTER, DEFAULT_PER_ROUTER_READ_QUOTA);
    this.defaultMaxRecordSizeBytes =
        props.getInt(DEFAULT_MAX_RECORD_SIZE_BYTES, DEFAULT_MAX_RECORD_SIZE_BYTES_BACKFILL);
    if (defaultMaxRecordSizeBytes < BYTES_PER_MB) {
      throw new VeniceException(
          "Default max record size must be at least " + generateHumanReadableByteCountString(BYTES_PER_MB));
    }
    this.replicationMetadataVersion = props.getInt(REPLICATION_METADATA_VERSION, 1);
    // go/inclusivecode deferred(Will be replaced when clients have migrated)
    this.childDatacenters = Utils.parseCommaSeparatedStringToSet(
        props.getStringWithAlternative(CHILD_CLUSTER_ALLOWLIST, CHILD_CLUSTER_WHITELIST, null));
    this.errorLeaderReplicaFailOverEnabled = props.getBoolean(FORCE_LEADER_ERROR_REPLICA_FAIL_OVER_ENABLED, true);

    this.adminPort = props.getInt(ADMIN_PORT);
    this.adminHostname = props.getString(ADMIN_HOSTNAME, Utils::getHostName);
    this.adminSecurePort = props.getInt(ADMIN_SECURE_PORT);
    /**
     * Override the config to false if the "Read" method check is not working as expected.
     */
    this.adminCheckReadMethodForKafka = props.getBoolean(ADMIN_CHECK_READ_METHOD_FOR_KAFKA, true);
    this.controllerClusterName = props.getString(CONTROLLER_CLUSTER, "venice-controllers");
    this.controllerResourceInstanceGroupTag = props.getString(CONTROLLER_RESOURCE_INSTANCE_GROUP_TAG, "");

    if (props.getString(CONTROLLER_INSTANCE_TAG_LIST, "").isEmpty()) {
      this.controllerInstanceTagList = Collections.emptyList();
    } else {
      this.controllerInstanceTagList = props.getList(CONTROLLER_INSTANCE_TAG_LIST);
    }

    this.controllerClusterReplica = props.getInt(CONTROLLER_CLUSTER_REPLICA, 3);
    this.controllerClusterZkAddress = props.getString(CONTROLLER_CLUSTER_ZK_ADDRESSS, getZkAddress());
    this.parent = props.getBoolean(CONTROLLER_PARENT_MODE, false);
    this.parentControllerRegionState =
        ParentControllerRegionState.valueOf(props.getString(CONTROLLER_PARENT_REGION_STATE, ACTIVE.name()));

    if (childDatacenters.isEmpty()) {
      this.childDataCenterControllerUrlMap = Collections.emptyMap();
      this.childDataCenterControllerD2Map = Collections.emptyMap();
    } else {
      this.childDataCenterControllerUrlMap = parseClusterMap(props, childDatacenters);
      this.childDataCenterControllerD2Map = parseClusterMap(props, childDatacenters, true);
    }

    Set<String> nativeReplicationSourceFabricAllowlist = Utils.parseCommaSeparatedStringToSet(
        props.getStringWithAlternative(
            NATIVE_REPLICATION_FABRIC_ALLOWLIST,
            // go/inclusivecode deferred(will be removed once all configs have migrated)
            NATIVE_REPLICATION_FABRIC_WHITELIST,
            null));

    this.d2ServiceName =
        childDataCenterControllerD2Map.isEmpty() ? null : props.getString(CHILD_CLUSTER_D2_SERVICE_NAME);
    if (this.parent) {
      if (childDataCenterControllerUrlMap.isEmpty() && childDataCenterControllerD2Map.isEmpty()) {
        throw new VeniceException("child controller list can not be empty");
      }
      this.parentFabrics =
          Utils.parseCommaSeparatedStringToSet(props.getString(PARENT_KAFKA_CLUSTER_FABRIC_LIST, (String) null));
      this.childDataCenterKafkaUrlMap = parseChildDataCenterKafkaUrl(props, nativeReplicationSourceFabricAllowlist);
    } else {
      this.parentFabrics = Collections.emptySet();

      if (nativeReplicationSourceFabricAllowlist.isEmpty()) {
        this.childDataCenterKafkaUrlMap = Collections.emptyMap();
      } else {
        this.childDataCenterKafkaUrlMap = parseChildDataCenterKafkaUrl(props, nativeReplicationSourceFabricAllowlist);
      }
    }

    if (props.containsKey(MULTI_REGION)) {
      this.multiRegion = props.getBoolean(MULTI_REGION);
    } else {
      LOGGER.warn("Config '{}' is not set. Inferring multi-region setup from other configs.", MULTI_REGION);

      /**
       * Historically, {@link MULTI_REGION} was not a supported config. It was handled on a case-by-case basis by
       * carefully setting feature configs. While this works for ramping new features, it makes it hard to remove the
       * feature flags once the feature is fully ramped. Ideally, this should be a mandatory config, but that would break
       * backward compatibility and hence, we infer the multi-region setup through the presence of other configs.
       */
      if (parent) {
        // Parent controllers only run in multi-region mode
        LOGGER.info("Inferring multi-region mode since this is the parent controller.");
        this.multiRegion = true;
      } else if (!childDatacenters.isEmpty()) {
        LOGGER.info("Inferring multi-region mode since there are child regions configured.");
        this.multiRegion = true;
      } else if (!childDataCenterKafkaUrlMap.isEmpty()) {
        LOGGER.info("Inferring multi-region mode since PubSub URLs are set for child regions.");
        // This is implicitly a mandatory config for child controllers in multi-region mode since Admin topic remote
        // consumption is the only supported mode in multi-region setup, and that needs PubSub URLs to be set.
        this.multiRegion = true;
      } else {
        LOGGER.info("Inferring single-region mode.");
        this.multiRegion = false;
      }
    }

    Set<String> activeActiveRealTimeSourceFabrics = Utils
        .parseCommaSeparatedStringToSet(props.getString(ACTIVE_ACTIVE_REAL_TIME_SOURCE_FABRIC_LIST, (String) null));

    if (activeActiveRealTimeSourceFabrics.isEmpty()) {
      LOGGER.info(
          "'{}' not configured explicitly. Using '{}' from '{}'",
          ACTIVE_ACTIVE_REAL_TIME_SOURCE_FABRIC_LIST,
          nativeReplicationSourceFabricAllowlist,
          NATIVE_REPLICATION_FABRIC_ALLOWLIST);
      activeActiveRealTimeSourceFabrics = nativeReplicationSourceFabricAllowlist;
    }

    for (String aaSourceFabric: activeActiveRealTimeSourceFabrics) {
      if (!childDataCenterKafkaUrlMap.containsKey(aaSourceFabric)) {
        throw new VeniceException(String.format("No Kafka URL found for A/A source fabric '%s'", aaSourceFabric));
      }
    }

    this.activeActiveRealTimeSourceKafkaURLs = activeActiveRealTimeSourceFabrics.stream()
        .map(childDataCenterKafkaUrlMap::get)
        .collect(Collectors.collectingAndThen(Collectors.toList(), Collections::unmodifiableList));

    this.nativeReplicationSourceFabric = props.getString(NATIVE_REPLICATION_SOURCE_FABRIC, "");

    this.parentControllerWaitingTimeForConsumptionMs =
        props.getInt(PARENT_CONTROLLER_WAITING_TIME_FOR_CONSUMPTION_MS, 30 * Time.MS_PER_SECOND);
    this.batchJobHeartbeatStoreCluster = props.getString(
        BatchJobHeartbeatConfigs.HEARTBEAT_STORE_CLUSTER_CONFIG.getConfigName(),
        BatchJobHeartbeatConfigs.HEARTBEAT_STORE_CLUSTER_CONFIG.getDefaultValue());
    this.batchJobHeartbeatEnabled = props.getBoolean(
        BatchJobHeartbeatConfigs.HEARTBEAT_ENABLED_CONFIG.getConfigName(),
        BatchJobHeartbeatConfigs.HEARTBEAT_ENABLED_CONFIG.getDefaultValue());
    this.batchJobHeartbeatTimeout = Duration.ofMillis(
        props.getLong(
            BatchJobHeartbeatConfigs.HEARTBEAT_CONTROLLER_TIMEOUT_CONFIG.getConfigName(),
            BatchJobHeartbeatConfigs.HEARTBEAT_CONTROLLER_TIMEOUT_CONFIG.getDefaultValue()));
    this.batchJobHeartbeatInitialBufferTime = Duration.ofMillis(
        props.getLong(
            BatchJobHeartbeatConfigs.HEARTBEAT_CONTROLLER_INITIAL_DELAY_CONFIG.getConfigName(),
            BatchJobHeartbeatConfigs.HEARTBEAT_CONTROLLER_INITIAL_DELAY_CONFIG.getDefaultValue()));
    this.adminConsumptionCycleTimeoutMs =
        props.getLong(ADMIN_CONSUMPTION_CYCLE_TIMEOUT_MS, TimeUnit.MINUTES.toMillis(30));
    // A value of one will result in a bad message for one store to block the admin message consumption of other stores.
    // Consider changing the config to > 1
    this.adminConsumptionMaxWorkerThreadPoolSize = props.getInt(ADMIN_CONSUMPTION_MAX_WORKER_THREAD_POOL_SIZE, 1);
    this.storageEngineOverheadRatio = props.getDouble(STORAGE_ENGINE_OVERHEAD_RATIO, 0.85d);

    // The default retention will allow Kafka remove as much data as possible.
    this.deprecatedJobTopicRetentionMs = props.getLong(DEPRECATED_TOPIC_RETENTION_MS, TimeUnit.SECONDS.toMillis(5)); // 5
                                                                                                                     // seconds
    this.deprecatedJobTopicMaxRetentionMs =
        props.getLong(DEPRECATED_TOPIC_MAX_RETENTION_MS, TimeUnit.SECONDS.toMillis(60)); // 1 min
    if (this.deprecatedJobTopicMaxRetentionMs < this.deprecatedJobTopicRetentionMs) {
      throw new VeniceException(
          "Config: " + DEPRECATED_TOPIC_MAX_RETENTION_MS + " with value: " + this.deprecatedJobTopicMaxRetentionMs
              + " should be larger than config: " + DEPRECATED_TOPIC_RETENTION_MS + " with value: "
              + this.deprecatedJobTopicRetentionMs);
    }

    this.fatalDataValidationFailureRetentionMs =
        props.getLong(FATAL_DATA_VALIDATION_FAILURE_TOPIC_RETENTION_MS, TimeUnit.DAYS.toMillis(2));

    this.topicCleanupSleepIntervalBetweenTopicListFetchMs =
        props.getLong(TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS, TimeUnit.SECONDS.toMillis(30)); // 30
                                                                                                                // seconds
    this.topicCleanupDelayFactor = props.getInt(TOPIC_CLEANUP_DELAY_FACTOR, 20); // thisFactor *
                                                                                 // topicCleanupSleepIntervalBetweenTopicListFetchMs
                                                                                 // = delayBeforeTopicDeletion

    this.disabledReplicaEnablerServiceIntervalMs =
        props.getLong(CONTROLLER_DISABLED_REPLICA_ENABLER_INTERVAL_MS, TimeUnit.HOURS.toMillis(16));
    this.topicManagerMetadataFetcherConsumerPoolSize = props.getInt(
        PUBSUB_TOPIC_MANAGER_METADATA_FETCHER_CONSUMER_POOL_SIZE,
        PUBSUB_TOPIC_MANAGER_METADATA_FETCHER_CONSUMER_POOL_SIZE_DEFAULT_VALUE);
    this.topicManagerMetadataFetcherThreadPoolSize = props
        .getInt(PUBSUB_TOPIC_MANAGER_METADATA_FETCHER_THREAD_POOL_SIZE, topicManagerMetadataFetcherConsumerPoolSize);

    this.minNumberOfUnusedKafkaTopicsToPreserve = props.getInt(MIN_NUMBER_OF_UNUSED_KAFKA_TOPICS_TO_PRESERVE, 2);
    this.minNumberOfStoreVersionsToPreserve = props.getInt(MIN_NUMBER_OF_STORE_VERSIONS_TO_PRESERVE, 2);
    if (minNumberOfStoreVersionsToPreserve < 1) {
      throw new VeniceException(
          "The minimal acceptable value for '" + MIN_NUMBER_OF_STORE_VERSIONS_TO_PRESERVE + "' is 1.");
    }
    // By default, keep 0 errored topics per store in parent controller
    this.parentControllerMaxErroredTopicNumToKeep = props.getInt(PARENT_CONTROLLER_MAX_ERRORED_TOPIC_NUM_TO_KEEP, 0);

    this.pushJobStatusStoreClusterName = props.getString(PUSH_JOB_STATUS_STORE_CLUSTER_NAME, "");
    this.participantMessageStoreEnabled = props.getBoolean(PARTICIPANT_MESSAGE_STORE_ENABLED, true);
    this.adminHelixMessagingChannelEnabled = props.getBoolean(ADMIN_HELIX_MESSAGING_CHANNEL_ENABLED, true);
    if (!adminHelixMessagingChannelEnabled && !participantMessageStoreEnabled) {
      throw new VeniceException(
          "Cannot perform kill push job if both " + ADMIN_HELIX_MESSAGING_CHANNEL_ENABLED + " and "
              + PARTICIPANT_MESSAGE_STORE_ENABLED + " are set to false");
    }
    this.systemSchemaClusterName = props.getString(CONTROLLER_SYSTEM_SCHEMA_CLUSTER_NAME, "");
    this.earlyDeleteBackUpEnabled = props.getBoolean(CONTROLLER_EARLY_DELETE_BACKUP_ENABLED, true);
    this.isControllerClusterLeaderHAAS = props.getBoolean(CONTROLLER_CLUSTER_LEADER_HAAS, false);
    this.isVeniceClusterLeaderHAAS = props.getBoolean(VENICE_STORAGE_CLUSTER_LEADER_HAAS, false);
    this.controllerHAASSuperClusterName = props.getString(CONTROLLER_HAAS_SUPER_CLUSTER_NAME, "");
    if ((isControllerClusterLeaderHAAS || isVeniceClusterLeaderHAAS) && controllerHAASSuperClusterName.isEmpty()) {
      throw new VeniceException(
          CONTROLLER_HAAS_SUPER_CLUSTER_NAME + " is required for " + CONTROLLER_CLUSTER_LEADER_HAAS + " or "
              + VENICE_STORAGE_CLUSTER_LEADER_HAAS + " to be set to true");
    }
    this.errorPartitionAutoResetLimit = props.getInt(ERROR_PARTITION_AUTO_RESET_LIMIT, 0);
    this.errorPartitionProcessingCycleDelay =
        props.getLong(ERROR_PARTITION_PROCESSING_CYCLE_DELAY, 5 * Time.MS_PER_MINUTE);
    this.backupVersionCleanupSleepMs =
        props.getLong(CONTROLLER_BACKUP_VERSION_DELETION_SLEEP_MS, TimeUnit.MINUTES.toMillis(5));
    this.backupVersionDefaultRetentionMs =
        props.getLong(CONTROLLER_BACKUP_VERSION_DEFAULT_RETENTION_MS, TimeUnit.DAYS.toMillis(7)); // 1 week
    this.backupVersionRetentionBasedCleanupEnabled =
        props.getBoolean(CONTROLLER_BACKUP_VERSION_RETENTION_BASED_CLEANUP_ENABLED, false);
    this.backupVersionMetadataFetchBasedCleanupEnabled =
        props.getBoolean(CONTROLLER_BACKUP_VERSION_METADATA_FETCH_BASED_CLEANUP_ENABLED, false);
    this.enforceSSLOnly = props.getBoolean(CONTROLLER_ENFORCE_SSL, false); // By default, allow both secure and insecure
                                                                           // routes
    this.terminalStateTopicCheckerDelayMs =
        props.getLong(TERMINAL_STATE_TOPIC_CHECK_DELAY_MS, TimeUnit.MINUTES.toMillis(10));
    this.disableParentTopicTruncationUponCompletion =
        props.getBoolean(CONTROLLER_DISABLE_PARENT_TOPIC_TRUNCATION_UPON_COMPLETION, false);
    /**
     * Disable the zk shared metadata system schema store by default until the schema is fully finalized.
     */
    this.zkSharedMetaSystemSchemaStoreAutoCreationEnabled =
        props.getBoolean(CONTROLLER_ZK_SHARED_META_SYSTEM_SCHEMA_STORE_AUTO_CREATION_ENABLED, false);
    this.pushStatusStoreHeartbeatExpirationTimeInSeconds = props.getLong(
        PUSH_STATUS_STORE_HEARTBEAT_EXPIRATION_TIME_IN_SECONDS,
        DEFAULT_PUSH_STATUS_STORE_HEARTBEAT_EXPIRATION_TIME_IN_SECONDS);
    this.isDaVinciPushStatusStoreEnabled = props.getBoolean(PUSH_STATUS_STORE_ENABLED, false);
    this.daVinciPushStatusScanEnabled =
        props.getBoolean(DAVINCI_PUSH_STATUS_SCAN_ENABLED, true) && isDaVinciPushStatusStoreEnabled;
    this.daVinciPushStatusScanIntervalInSeconds = props.getInt(DAVINCI_PUSH_STATUS_SCAN_INTERVAL_IN_SECONDS, 30);
    this.daVinciPushStatusScanThreadNumber = props.getInt(DAVINCI_PUSH_STATUS_SCAN_THREAD_NUMBER, 4);
    this.daVinciPushStatusScanNoReportRetryMaxAttempt =
        props.getInt(DAVINCI_PUSH_STATUS_SCAN_NO_REPORT_RETRY_MAX_ATTEMPTS, 6);
    this.daVinciPushStatusScanMaxOfflineInstanceCount =
        props.getInt(DAVINCI_PUSH_STATUS_SCAN_MAX_OFFLINE_INSTANCE_COUNT, 10);
    this.daVinciPushStatusScanMaxOfflineInstanceRatio =
        props.getDouble(DAVINCI_PUSH_STATUS_SCAN_MAX_OFFLINE_INSTANCE_RATIO, 0.05d);

    this.zkSharedDaVinciPushStatusSystemSchemaStoreAutoCreationEnabled =
        props.getBoolean(CONTROLLER_ZK_SHARED_DAVINCI_PUSH_STATUS_SYSTEM_SCHEMA_STORE_AUTO_CREATION_ENABLED, true);
    this.systemStoreAclSynchronizationDelayMs =
        props.getLong(CONTROLLER_SYSTEM_STORE_ACL_SYNCHRONIZATION_DELAY_MS, TimeUnit.HOURS.toMillis(1));
    this.regionName = RegionUtils.getLocalRegionName(props, parent);
    LOGGER.info("Final region name for this node: {}", this.regionName);
    this.disabledRoutes = parseControllerRoutes(props, CONTROLLER_DISABLED_ROUTES, Collections.emptyList());
    this.adminTopicRemoteConsumptionEnabled = !this.parent && this.multiRegion;
    if (adminTopicRemoteConsumptionEnabled && childDataCenterKafkaUrlMap.isEmpty()) {
      throw new VeniceException("Admin topic remote consumption is enabled but Kafka url map is empty");
    }
    this.adminTopicSourceRegion = props.getString(ADMIN_TOPIC_SOURCE_REGION, "");
    this.aggregateRealTimeSourceRegion = props.getString(AGGREGATE_REAL_TIME_SOURCE_REGION, "");
    this.isAutoMaterializeMetaSystemStoreEnabled =
        props.getBoolean(CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE, false);
    this.isAutoMaterializeDaVinciPushStatusSystemStoreEnabled =
        props.getBoolean(CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE, false);
    this.usePushStatusStoreForIncrementalPushStatusReads =
        props.getBoolean(USE_PUSH_STATUS_STORE_FOR_INCREMENTAL_PUSH, false);
    this.metaStoreWriterCloseTimeoutInMS = props.getLong(META_STORE_WRITER_CLOSE_TIMEOUT_MS, 300000L);
    this.metaStoreWriterCloseConcurrency = props.getInt(META_STORE_WRITER_CLOSE_CONCURRENCY, -1);
    this.emergencySourceRegion = props.getString(EMERGENCY_SOURCE_REGION, "");
    this.allowClusterWipe = props.getBoolean(ALLOW_CLUSTER_WIPE, false);
    this.concurrentInitRoutinesEnabled = props.getBoolean(CONCURRENT_INIT_ROUTINES_ENABLED, false);
    this.controllerClusterHelixCloudEnabled = props.getBoolean(CONTROLLER_CLUSTER_HELIX_CLOUD_ENABLED, false);
    this.storageClusterHelixCloudEnabled = props.getBoolean(CONTROLLER_STORAGE_CLUSTER_HELIX_CLOUD_ENABLED, false);

    if (controllerClusterHelixCloudEnabled || storageClusterHelixCloudEnabled) {
      CloudProvider helixCloudProvider;
      String controllerCloudProvider = props.getString(CONTROLLER_HELIX_CLOUD_PROVIDER).toUpperCase();
      try {
        helixCloudProvider = CloudProvider.valueOf(controllerCloudProvider);
      } catch (IllegalArgumentException e) {
        throw new VeniceException(
            "Invalid Helix cloud provider: " + controllerCloudProvider + ". Must be one of: "
                + Arrays.toString(CloudProvider.values()));
      }

      String helixCloudId = props.getString(CONTROLLER_HELIX_CLOUD_ID, "");
      String helixCloudInfoProcessorPackage = props.getString(CONTROLLER_HELIX_CLOUD_INFO_PROCESSOR_PACKAGE, "");
      String helixCloudInfoProcessorName = props.getString(CONTROLLER_HELIX_CLOUD_INFO_PROCESSOR_NAME, "");

      List<String> helixCloudInfoSources;
      if (props.getString(CONTROLLER_HELIX_CLOUD_INFO_SOURCES, "").isEmpty()) {
        helixCloudInfoSources = Collections.emptyList();
      } else {
        helixCloudInfoSources = props.getList(CONTROLLER_HELIX_CLOUD_INFO_SOURCES);
      }

      helixCloudConfig = HelixUtils.getCloudConfig(
          helixCloudProvider,
          helixCloudId,
          helixCloudInfoSources,
          helixCloudInfoProcessorPackage,
          helixCloudInfoProcessorName);
    } else {
      helixCloudConfig = null;
    }

    this.helixRestCustomizedHealthUrl = props.getString(CONTROLLER_HELIX_REST_CUSTOMIZED_HEALTH_URL, "");

    this.unregisterMetricForDeletedStoreEnabled = props.getBoolean(UNREGISTER_METRIC_FOR_DELETED_STORE_ENABLED, false);
    this.identityParserClassName = props.getString(IDENTITY_PARSER_CLASS, DefaultIdentityParser.class.getName());
    this.storeGraveyardCleanupEnabled = props.getBoolean(CONTROLLER_STORE_GRAVEYARD_CLEANUP_ENABLED, false);
    this.storeGraveyardCleanupDelayMinutes = props.getInt(CONTROLLER_STORE_GRAVEYARD_CLEANUP_DELAY_MINUTES, 0);
    this.storeGraveyardCleanupSleepIntervalBetweenListFetchMinutes =
        props.getInt(CONTROLLER_STORE_GRAVEYARD_CLEANUP_SLEEP_INTERVAL_BETWEEN_LIST_FETCH_MINUTES, 15);
    this.parentSystemStoreRepairServiceEnabled =
        props.getBoolean(CONTROLLER_PARENT_SYSTEM_STORE_REPAIR_SERVICE_ENABLED, false);
    this.parentSystemStoreRepairCheckIntervalSeconds =
        props.getInt(CONTROLLER_PARENT_SYSTEM_STORE_REPAIR_CHECK_INTERVAL_SECONDS, 1800);
    this.parentSystemStoreHeartbeatCheckWaitTimeSeconds =
        props.getInt(CONTROLLER_PARENT_SYSTEM_STORE_HEARTBEAT_CHECK_WAIT_TIME_SECONDS, 60);
    this.parentSystemStoreRepairRetryCount = props.getInt(CONTROLLER_PARENT_SYSTEM_STORE_REPAIR_RETRY_COUNT, 1);
    this.clusterDiscoveryD2ServiceName =
        props.getString(CLUSTER_DISCOVERY_D2_SERVICE, ClientConfig.DEFAULT_CLUSTER_DISCOVERY_D2_SERVICE_NAME);
    this.parentExternalSupersetSchemaGenerationEnabled =
        props.getBoolean(CONTROLLER_PARENT_EXTERNAL_SUPERSET_SCHEMA_GENERATION_ENABLED, false);
    this.systemSchemaInitializationAtStartTimeEnabled =
        props.getBoolean(SYSTEM_SCHEMA_INITIALIZATION_AT_START_TIME_ENABLED, false);
    this.isKMERegistrationFromMessageHeaderEnabled =
        props.getBoolean(KME_REGISTRATION_FROM_MESSAGE_HEADER_ENABLED, false);
    this.enableDisabledReplicaEnabled = props.getBoolean(CONTROLLER_ENABLE_DISABLED_REPLICA_ENABLED, false);

    this.unusedValueSchemaCleanupServiceEnabled =
        props.getBoolean(CONTROLLER_UNUSED_VALUE_SCHEMA_CLEANUP_ENABLED, false);
    this.unusedSchemaCleanupIntervalSeconds = props.getInt(CONTROLLER_UNUSED_SCHEMA_CLEANUP_INTERVAL_SECONDS, 36000);
    this.minSchemaCountToKeep = props.getInt(CONTROLLER_MIN_SCHEMA_COUNT_TO_KEEP, 20);
    this.useDaVinciSpecificExecutionStatusForError =
        props.getBoolean(USE_DA_VINCI_SPECIFIC_EXECUTION_STATUS_FOR_ERROR, false);
    this.pubSubClientsFactory = new PubSubClientsFactory(props);
    this.sourceOfTruthAdminAdapterFactory = PubSubClientsFactory.createSourceOfTruthAdminFactory(props);
    this.danglingTopicCleanupIntervalSeconds = props.getLong(CONTROLLER_DANGLING_TOPIC_CLEAN_UP_INTERVAL_SECOND, -1);
    this.danglingTopicOccurrenceThresholdForCleanup =
        props.getInt(CONTROLLER_DANGLING_TOPIC_OCCURRENCE_THRESHOLD_FOR_CLEANUP, 3);
    this.serviceDiscoveryRegistrationRetryMS =
        props.getLong(SERVICE_DISCOVERY_REGISTRATION_RETRY_MS, 30L * Time.MS_PER_SECOND);
    this.pushJobUserErrorCheckpoints = parsePushJobUserErrorCheckpoints(props);
    this.isHybridStorePartitionCountUpdateEnabled =
        props.getBoolean(ConfigKeys.CONTROLLER_ENABLE_HYBRID_STORE_PARTITION_COUNT_UPDATE, false);
  }

  public VeniceProperties getProps() {
    return props;
  }

  public String getClusterName() {
    return clusterName;
  }

  public final String getZkAddress() {
    return zkAddress;
  }

  private boolean doesControllerNeedsSslConfig() {
    final boolean controllerSslEnabled = props.getBoolean(CONTROLLER_SSL_ENABLED, DEFAULT_CONTROLLER_SSL_ENABLED);
    final boolean kafkaNeedsSsl = KafkaSSLUtils.isKafkaSSLProtocol(kafkaSecurityProtocol);

    return controllerSslEnabled || kafkaNeedsSsl;
  }

  public boolean isErrorLeaderReplicaFailOverEnabled() {
    return errorLeaderReplicaFailOverEnabled;
  }

  public int getDefaultReadQuotaPerRouter() {
    return defaultReadQuotaPerRouter;
  }

  public int getDefaultMaxRecordSizeBytes() {
    return defaultMaxRecordSizeBytes;
  }

  public String getControllerName() {
    return controllerName;
  }

  public PersistenceType getPersistenceType() {
    return persistenceType;
  }

  public ReadStrategy getReadStrategy() {
    return readStrategy;
  }

  public OfflinePushStrategy getOfflinePushStrategy() {
    return offlinePushStrategy;
  }

  public RoutingStrategy getRoutingStrategy() {
    return routingStrategy;
  }

  public int getReplicationFactor() {
    return replicationFactor;
  }

  public int getMinNumberOfPartitions() {
    return minNumberOfPartitions;
  }

  public int getMinNumberOfPartitionsForHybrid() {
    return minNumberOfPartitionsForHybrid;
  }

  public int getKafkaReplicationFactor() {
    return kafkaReplicationFactor;
  }

  public int getKafkaReplicationFactorRTTopics() {
    return kafkaReplicationFactorRTTopics;
  }

  public long getPartitionSize() {
    return partitionSize;
  }

  public boolean isDisableParentRequestTopicForStreamPushes() {
    return disableParentRequestTopicForStreamPushes;
  }

  public int getMaxNumberOfPartitions() {
    return maxNumberOfPartitions;
  }

  public boolean isPartitionCountRoundUpEnabled() {
    return partitionCountRoundUpEnabled;
  }

  public int getPartitionCountRoundUpSize() {
    return partitionCountRoundUpSize;
  }

  public long getOffLineJobWaitTimeInMilliseconds() {
    return offLineJobWaitTimeInMilliseconds;
  }

  public long getDelayToRebalanceMS() {
    return delayToRebalanceMS;
  }

  /**
   * @return kafka Bootstrap Urls. If there is more than one url, they are separated by commas.
   */
  public String getKafkaBootstrapServers() {
    return kafkaBootstrapServers;
  }

  public Map<String, String> getClusterToD2Map() {
    return clusterToD2Map;
  }

  public Map<String, String> getClusterToServerD2Map() {
    return clusterToServerD2Map;
  }

  public boolean isSslToKafka() {
    return sslToKafka;
  }

  public String getSslKafkaBootstrapServers() {
    return sslKafkaBootStrapServers;
  }

  public int getHelixSendMessageTimeoutMs() {
    return helixSendMessageTimeoutMilliseconds;
  }

  public String getKafkaSecurityProtocol() {
    return kafkaSecurityProtocol;
  }

  public Optional<SSLConfig> getSslConfig() {
    return sslConfig;
  }

  public String getSslFactoryClassName() {
    return sslFactoryClassName;
  }

  public int getRefreshAttemptsForZkReconnect() {
    return refreshAttemptsForZkReconnect;
  }

  public long getRefreshIntervalForZkReconnectInMs() {
    return refreshIntervalForZkReconnectInMs;
  }

  public boolean isEnableOfflinePushSSLAllowlist() {
    return enableOfflinePushSSLAllowlist;
  }

  public List<String> getPushSSLAllowlist() {
    return pushSSLAllowlist;
  }

  public boolean isEnableNearlinePushSSLAllowlist() {
    return enableNearlinePushSSLAllowlist;
  }

  public String getHelixRebalanceAlg() {
    return helixRebalanceAlg;
  }

  public int getAdminTopicReplicationFactor() {
    return adminTopicReplicationFactor;
  }

  public Optional<Integer> getMinInSyncReplicas() {
    return minInSyncReplicas;
  }

  public Optional<Integer> getMinInSyncReplicasRealTimeTopics() {
    return minInSyncReplicasRealTimeTopics;
  }

  public Optional<Integer> getMinInSyncReplicasAdminTopics() {
    return minInSyncReplicasAdminTopics;
  }

  public boolean isKafkaLogCompactionForHybridStoresEnabled() {
    return kafkaLogCompactionForHybridStores;
  }

  public boolean isActiveActiveReplicationEnabledAsDefaultForHybrid() {
    return activeActiveReplicationEnabledAsDefaultForHybrid;
  }

  public boolean isControllerSchemaValidationEnabled() {
    return controllerSchemaValidationEnabled;
  }

  public long getLeakedPushStatusCleanUpServiceSleepIntervalInMs() {
    return leakedPushStatusCleanUpServiceSleepIntervalInMs;
  }

  public long getLeakedResourceAllowedLingerTimeInMs() {
    return leakedResourceAllowedLingerTimeInMs;
  }

  public String getNativeReplicationSourceFabricAsDefaultForBatchOnly() {
    return nativeReplicationSourceFabricAsDefaultForBatchOnly;
  }

  public String getNativeReplicationSourceFabricAsDefaultForHybrid() {
    return nativeReplicationSourceFabricAsDefaultForHybrid;
  }

  public VeniceProperties getJettyConfigOverrides() {
    return jettyConfigOverrides;
  }

  public int getReplicationMetadataVersion() {
    return replicationMetadataVersion;
  }

  public Set<String> getChildDatacenters() {
    return childDatacenters;
  }

  public boolean enabledIncrementalPushForHybridActiveActiveUserStores() {
    return enabledIncrementalPushForHybridActiveActiveUserStores;
  }

  public boolean enabledSeparateRealTimeTopicForStoreWithIncrementalPush() {
    return enabledSeparateRealTimeTopicForStoreWithIncrementalPush;
  }

  public boolean isEnablePartialUpdateForHybridActiveActiveUserStores() {
    return enablePartialUpdateForHybridActiveActiveUserStores;
  }

  public boolean isEnablePartialUpdateForHybridNonActiveActiveUserStores() {
    return enablePartialUpdateForHybridNonActiveActiveUserStores;
  }

  public int getAdminPort() {
    return adminPort;
  }

  public String getAdminHostname() {
    return adminHostname;
  }

  public int getAdminSecurePort() {
    return adminSecurePort;
  }

  public boolean adminCheckReadMethodForKafka() {
    return adminCheckReadMethodForKafka;
  }

  public int getControllerClusterReplica() {
    return controllerClusterReplica;
  }

  public String getControllerClusterName() {
    return controllerClusterName;
  }

  public String getControllerResourceInstanceGroupTag() {
    return controllerResourceInstanceGroupTag;
  }

  public List<String> getControllerInstanceTagList() {
    return controllerInstanceTagList;
  }

  public String getControllerClusterZkAddress() {
    return controllerClusterZkAddress;
  }

  public boolean isMultiRegion() {
    return multiRegion;
  }

  public boolean isParent() {
    return parent;
  }

  public ParentControllerRegionState getParentControllerRegionState() {
    return parentControllerRegionState;
  }

  public long getDeprecatedJobTopicRetentionMs() {
    return deprecatedJobTopicRetentionMs;
  }

  public long getFatalDataValidationFailureRetentionMs() {
    return fatalDataValidationFailureRetentionMs;
  }

  public long getDeprecatedJobTopicMaxRetentionMs() {
    return deprecatedJobTopicMaxRetentionMs;
  }

  public long getTopicCleanupSleepIntervalBetweenTopicListFetchMs() {
    return topicCleanupSleepIntervalBetweenTopicListFetchMs;
  }

  public long getDisabledReplicaEnablerServiceIntervalMs() {
    return disabledReplicaEnablerServiceIntervalMs;
  }

  public int getDaVinciPushStatusScanMaxOfflineInstanceCount() {
    return daVinciPushStatusScanMaxOfflineInstanceCount;
  }

  public double getDaVinciPushStatusScanMaxOfflineInstanceRatio() {
    return daVinciPushStatusScanMaxOfflineInstanceRatio;
  }

  public int getTopicCleanupDelayFactor() {
    return topicCleanupDelayFactor;
  }

  /**
   * Map where keys are logical, human-readable names for child clusters (suitable for printing in logs or other output)
   * values are a list of cluster URLs that can be used to reach that cluster with the controller client.  List provides
   * redundancy in case of hardware or other failure.  Clients of this list should be sure they use another url if the
   * first one fails.
   */
  public Map<String, String> getChildDataCenterControllerUrlMap() {
    return childDataCenterControllerUrlMap;
  }

  public String getD2ServiceName() {
    return d2ServiceName;
  }

  public String getClusterDiscoveryD2ServiceName() {
    return clusterDiscoveryD2ServiceName;
  }

  public boolean isUnusedValueSchemaCleanupServiceEnabled() {
    return unusedValueSchemaCleanupServiceEnabled;
  }

  public int getUnusedSchemaCleanupIntervalSeconds() {
    return unusedSchemaCleanupIntervalSeconds;
  }

  public int getMinSchemaCountToKeep() {
    return minSchemaCountToKeep;
  }

  public boolean useDaVinciSpecificExecutionStatusForError() {
    return useDaVinciSpecificExecutionStatusForError;
  }

  public Map<String, String> getChildDataCenterControllerD2Map() {
    return childDataCenterControllerD2Map;
  }

  public Map<String, String> getChildDataCenterKafkaUrlMap() {
    return childDataCenterKafkaUrlMap;
  }

  public List<String> getActiveActiveRealTimeSourceKafkaURLs() {
    return activeActiveRealTimeSourceKafkaURLs;
  }

  public String getNativeReplicationSourceFabric() {
    return nativeReplicationSourceFabric;
  }

  public Set<String> getParentFabrics() {
    return parentFabrics;
  }

  public int getParentControllerWaitingTimeForConsumptionMs() {
    return parentControllerWaitingTimeForConsumptionMs;
  }

  public String getBatchJobHeartbeatStoreCluster() {
    return batchJobHeartbeatStoreCluster;
  }

  public boolean getBatchJobHeartbeatEnabled() {
    return batchJobHeartbeatEnabled;
  }

  public Duration getBatchJobHeartbeatTimeout() {
    return batchJobHeartbeatTimeout;
  }

  public Duration getBatchJobHeartbeatInitialBufferTime() {
    return batchJobHeartbeatInitialBufferTime;
  }

  public boolean isEnableDisabledReplicaEnabled() {
    return enableDisabledReplicaEnabled;
  }

  public long getAdminConsumptionCycleTimeoutMs() {
    return adminConsumptionCycleTimeoutMs;
  }

  public int getAdminConsumptionMaxWorkerThreadPoolSize() {
    return adminConsumptionMaxWorkerThreadPoolSize;
  }

  static Map<String, String> parseClusterMap(VeniceProperties clusterPros, Set<String> datacenterAllowlist) {
    return parseClusterMap(clusterPros, datacenterAllowlist, false);
  }

  public double getStorageEngineOverheadRatio() {
    return storageEngineOverheadRatio;
  }

  public int getTopicManagerMetadataFetcherConsumerPoolSize() {
    return topicManagerMetadataFetcherConsumerPoolSize;
  }

  public int getTopicManagerMetadataFetcherThreadPoolSize() {
    return topicManagerMetadataFetcherThreadPoolSize;
  }

  public int getMinNumberOfUnusedKafkaTopicsToPreserve() {
    return minNumberOfUnusedKafkaTopicsToPreserve;
  }

  public int getMinNumberOfStoreVersionsToPreserve() {
    return minNumberOfStoreVersionsToPreserve;
  }

  public int getParentControllerMaxErroredTopicNumToKeep() {
    return parentControllerMaxErroredTopicNumToKeep;
  }

  public String getPushJobStatusStoreClusterName() {
    return pushJobStatusStoreClusterName;
  }

  public boolean isParticipantMessageStoreEnabled() {
    return participantMessageStoreEnabled;
  }

  public boolean isDaVinciPushStatusEnabled() {
    return true;
  }

  public String getSystemSchemaClusterName() {
    return systemSchemaClusterName;
  }

  public boolean isAdminHelixMessagingChannelEnabled() {
    return adminHelixMessagingChannelEnabled;
  }

  public boolean isControllerClusterLeaderHAAS() {
    return isControllerClusterLeaderHAAS;
  }

  public boolean isVeniceClusterLeaderHAAS() {
    return isVeniceClusterLeaderHAAS;
  }

  public String getControllerHAASSuperClusterName() {
    return controllerHAASSuperClusterName;
  }

  public boolean isEarlyDeleteBackUpEnabled() {
    return earlyDeleteBackUpEnabled;
  }

  public int getErrorPartitionAutoResetLimit() {
    return errorPartitionAutoResetLimit;
  }

  public long getErrorPartitionProcessingCycleDelay() {
    return errorPartitionProcessingCycleDelay;
  }

  public long getBackupVersionDefaultRetentionMs() {
    return backupVersionDefaultRetentionMs;
  }

  public long getBackupVersionCleanupSleepMs() {
    return backupVersionCleanupSleepMs;
  }

  public boolean isBackupVersionRetentionBasedCleanupEnabled() {
    return backupVersionRetentionBasedCleanupEnabled;
  }

  public boolean isBackupVersionMetadataFetchBasedCleanupEnabled() {
    return backupVersionMetadataFetchBasedCleanupEnabled;
  }

  public boolean isControllerEnforceSSLOnly() {
    return enforceSSLOnly;
  }

  public long getTerminalStateTopicCheckerDelayMs() {
    return terminalStateTopicCheckerDelayMs;
  }

  public boolean disableParentTopicTruncationUponCompletion() {
    return disableParentTopicTruncationUponCompletion;
  }

  public boolean isZkSharedMetaSystemSchemaStoreAutoCreationEnabled() {
    return zkSharedMetaSystemSchemaStoreAutoCreationEnabled;
  }

  public long getPushStatusStoreHeartbeatExpirationTimeInSeconds() {
    return pushStatusStoreHeartbeatExpirationTimeInSeconds;
  }

  public boolean isDaVinciPushStatusStoreEnabled() {
    return isDaVinciPushStatusStoreEnabled;
  }

  public int getDaVinciPushStatusScanIntervalInSeconds() {
    return daVinciPushStatusScanIntervalInSeconds;
  }

  public boolean isDaVinciPushStatusScanEnabled() {
    return daVinciPushStatusScanEnabled;
  }

  public int getDaVinciPushStatusScanThreadNumber() {
    return daVinciPushStatusScanThreadNumber;
  }

  public int getDaVinciPushStatusScanNoReportRetryMaxAttempt() {
    return daVinciPushStatusScanNoReportRetryMaxAttempt;
  }

  public boolean isZkSharedDaVinciPushStatusSystemSchemaStoreAutoCreationEnabled() {
    return zkSharedDaVinciPushStatusSystemSchemaStoreAutoCreationEnabled;
  }

  public long getSystemStoreAclSynchronizationDelayMs() {
    return systemStoreAclSynchronizationDelayMs;
  }

  public String getRegionName() {
    return regionName;
  }

  public List<ControllerRoute> getDisabledRoutes() {
    return disabledRoutes;
  }

  static List<ControllerRoute> parseControllerRoutes(
      VeniceProperties clusterProps,
      String property,
      List<String> defaultValue) {
    return clusterProps.getList(property, defaultValue)
        .stream()
        .map(ControllerRoute::valueOfPath)
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  public boolean isAdminTopicRemoteConsumptionEnabled() {
    return adminTopicRemoteConsumptionEnabled;
  }

  public String getAdminTopicSourceRegion() {
    return adminTopicSourceRegion;
  }

  public String getAggregateRealTimeSourceRegion() {
    return aggregateRealTimeSourceRegion;
  }

  public boolean isAutoMaterializeMetaSystemStoreEnabled() {
    return isAutoMaterializeMetaSystemStoreEnabled;
  }

  public boolean isAutoMaterializeDaVinciPushStatusSystemStoreEnabled() {
    return isAutoMaterializeDaVinciPushStatusSystemStoreEnabled;
  }

  public String getEmergencySourceRegion() {
    return emergencySourceRegion;
  }

  public String getChildControllerUrl(String fabric) {
    return getProps().getString(CHILD_CLUSTER_URL_PREFIX + fabric, "");
  }

  public String getChildControllerD2ServiceName() {
    return getProps().getString(CHILD_CLUSTER_D2_SERVICE_NAME, "");
  }

  public String getChildControllerD2ZkHost(String fabric) {
    return getProps().getString(CHILD_CLUSTER_D2_PREFIX + fabric, "");
  }

  public boolean isClusterWipeAllowed() {
    return allowClusterWipe;
  }

  public boolean isConcurrentInitRoutinesEnabled() {
    return concurrentInitRoutinesEnabled;
  }

  public boolean isControllerClusterHelixCloudEnabled() {
    return controllerClusterHelixCloudEnabled;
  }

  public boolean isStorageClusterHelixCloudEnabled() {
    return storageClusterHelixCloudEnabled;
  }

  public CloudConfig getHelixCloudConfig() {
    return helixCloudConfig;
  }

  public String getHelixRestCustomizedHealthUrl() {
    return helixRestCustomizedHealthUrl;
  }

  public boolean usePushStatusStoreForIncrementalPush() {
    return usePushStatusStoreForIncrementalPushStatusReads;
  }

  public long getMetaStoreWriterCloseTimeoutInMS() {
    return metaStoreWriterCloseTimeoutInMS;
  }

  public int getMetaStoreWriterCloseConcurrency() {
    return metaStoreWriterCloseConcurrency;
  }

  public boolean isUnregisterMetricForDeletedStoreEnabled() {
    return unregisterMetricForDeletedStoreEnabled;
  }

  public String getIdentityParserClassName() {
    return identityParserClassName;
  }

  public boolean isStoreGraveyardCleanupEnabled() {
    return storeGraveyardCleanupEnabled;
  }

  public int getStoreGraveyardCleanupDelayMinutes() {
    return storeGraveyardCleanupDelayMinutes;
  }

  public int getStoreGraveyardCleanupSleepIntervalBetweenListFetchMinutes() {
    return storeGraveyardCleanupSleepIntervalBetweenListFetchMinutes;
  }

  public boolean isParentSystemStoreRepairServiceEnabled() {
    return parentSystemStoreRepairServiceEnabled;
  }

  public int getParentSystemStoreRepairCheckIntervalSeconds() {
    return parentSystemStoreRepairCheckIntervalSeconds;
  }

  public int getParentSystemStoreHeartbeatCheckWaitTimeSeconds() {
    return parentSystemStoreHeartbeatCheckWaitTimeSeconds;
  }

  public int getParentSystemStoreRepairRetryCount() {
    return parentSystemStoreRepairRetryCount;
  }

  public boolean isParentExternalSupersetSchemaGenerationEnabled() {
    return parentExternalSupersetSchemaGenerationEnabled;
  }

  public boolean isSystemSchemaInitializationAtStartTimeEnabled() {
    return systemSchemaInitializationAtStartTimeEnabled;
  }

  public boolean isKMERegistrationFromMessageHeaderEnabled() {
    return isKMERegistrationFromMessageHeaderEnabled;
  }

  public PubSubClientsFactory getPubSubClientsFactory() {
    return pubSubClientsFactory;
  }

  public PubSubAdminAdapterFactory getSourceOfTruthAdminAdapterFactory() {
    return sourceOfTruthAdminAdapterFactory;
  }

  public long getServiceDiscoveryRegistrationRetryMS() {
    return serviceDiscoveryRegistrationRetryMS;
  }

  /**
   * The config should follow the format below:
   * CHILD_CLUSTER_URL_PREFIX.fabricName1=controllerUrls_in_fabric1
   * CHILD_CLUSTER_URL_PREFIX.fabricName2=controllerUrls_in_fabric2
   *
   * This helper function will parse the config with above format and return a Map from data center to
   * its controller urls.
   *
   * @param clusterPros list of child controller uris.
   * @param datacenterAllowlist data centers that are taken into account.
   * @param D2Routing whether it uses D2 to route or not.
   */
  static Map<String, String> parseClusterMap(
      VeniceProperties clusterPros,
      Set<String> datacenterAllowlist,
      Boolean D2Routing) {
    String propsPrefix = D2Routing ? CHILD_CLUSTER_D2_PREFIX : CHILD_CLUSTER_URL_PREFIX;
    return parseChildDataCenterToValue(propsPrefix, clusterPros, datacenterAllowlist, (m, k, v, errMsg) -> {
      m.computeIfAbsent(k, key -> {
        String[] uriList = v.split(LIST_SEPARATOR);

        if (D2Routing && uriList.length != 1) {
          throw new VeniceException(errMsg + ": can only have 1 zookeeper url");
        }

        if (!D2Routing) {
          if (uriList.length == 0) {
            throw new VeniceException(errMsg + ": urls can not be empty");
          }

          if (Arrays.stream(uriList).anyMatch(uri -> (!uri.startsWith("http://") && !uri.startsWith("https://")))) {
            throw new VeniceException(errMsg + ": urls must begin with http:// or https://");
          }
        }

        return v;
      });
    });
  }

  /**
   * The config should follow the format below:
   * $CHILD_DATA_CENTER_KAFKA_URL_PREFIX.fabricName1=kafkaBootstrapServerUrls_in_fabric1
   * $CHILD_DATA_CENTER_KAFKA_URL_PREFIX.fabricName2=kafkaBootstrapServerUrls_in_fabric2
   *
   * This helper function will parse the config with above format and return a Map from data center to
   * its Kafka bootstrap server urls.
   */
  private static Map<String, String> parseChildDataCenterKafkaUrl(
      VeniceProperties clusterPros,
      Set<String> datacenterAllowlist) {
    return parseChildDataCenterToValue(
        CHILD_DATA_CENTER_KAFKA_URL_PREFIX,
        clusterPros,
        datacenterAllowlist,
        (m, k, v, e) -> m.putIfAbsent(k, v));
  }

  private static Map<String, String> parseChildDataCenterToValue(
      String configPrefix,
      VeniceProperties clusterPros,
      Set<String> datacenterAllowlist,
      PutToMap mappingFunction) {
    Properties childDataCenterKafkaUriProps = clusterPros.clipAndFilterNamespace(configPrefix).toProperties();

    if (datacenterAllowlist == null || datacenterAllowlist.isEmpty()) {
      throw new VeniceException("child controller list must have a allowlist");
    }

    Map<String, String> outputMap = new HashMap<>();

    for (Map.Entry<Object, Object> uriEntry: childDataCenterKafkaUriProps.entrySet()) {
      String datacenter = (String) uriEntry.getKey();
      String value = (String) uriEntry.getValue();

      String errMsg = "Invalid configuration " + configPrefix + "." + datacenter;
      if (datacenter.isEmpty()) {
        throw new VeniceException(errMsg + ": data center name can't be empty for value: " + value);
      }

      if (value.isEmpty()) {
        throw new VeniceException(errMsg + ": found no value for: " + datacenter);
      }

      if (datacenterAllowlist.contains(datacenter)) {
        mappingFunction.apply(outputMap, datacenter, value, errMsg);
      }
    }

    return outputMap;
  }

  public long getDanglingTopicCleanupIntervalSeconds() {
    return danglingTopicCleanupIntervalSeconds;
  }

  public int getDanglingTopicOccurrenceThresholdForCleanup() {
    return danglingTopicOccurrenceThresholdForCleanup;
  }

  public boolean isHybridStorePartitionCountUpdateEnabled() {
    return isHybridStorePartitionCountUpdateEnabled;
  }

  /**
   * A function that would put a k/v pair into a map with some processing works.
   */
  @FunctionalInterface
  interface PutToMap {
    void apply(Map<String, String> map, String key, String value, String errorMessage);
  }

  /**
   * Parse the input to get the custom user error checkpoints for push jobs or use the default checkpoints.
   */
  static Set<PushJobCheckpoints> parsePushJobUserErrorCheckpoints(VeniceProperties props) {
    if (props.containsKey(PUSH_JOB_FAILURE_CHECKPOINTS_TO_DEFINE_USER_ERROR)) {
      String pushJobUserErrorCheckpoints = props.getString(PUSH_JOB_FAILURE_CHECKPOINTS_TO_DEFINE_USER_ERROR);
      LOGGER.info("Using configured Push job user error checkpoints: {}", pushJobUserErrorCheckpoints);
      return Utils.parseCommaSeparatedStringToSet(pushJobUserErrorCheckpoints)
          .stream()
          .map(checkpointStr -> PushJobCheckpoints.valueOf(checkpointStr))
          .collect(Collectors.toSet());
    } else {
      LOGGER.info("Using default Push job user error checkpoints: {}", DEFAULT_PUSH_JOB_USER_ERROR_CHECKPOINTS);
      return DEFAULT_PUSH_JOB_USER_ERROR_CHECKPOINTS;
    }
  }

  public Set<PushJobCheckpoints> getPushJobUserErrorCheckpoints() {
    return pushJobUserErrorCheckpoints;
  }
}
