package com.linkedin.venice.controller;

import static com.linkedin.venice.ConfigKeys.ACTIVE_ACTIVE_ENABLED_ON_CONTROLLER;
import static com.linkedin.venice.ConfigKeys.ACTIVE_ACTIVE_REAL_TIME_SOURCE_FABRIC_LIST;
import static com.linkedin.venice.ConfigKeys.ADMIN_CHECK_READ_METHOD_FOR_KAFKA;
import static com.linkedin.venice.ConfigKeys.ADMIN_CONSUMPTION_CYCLE_TIMEOUT_MS;
import static com.linkedin.venice.ConfigKeys.ADMIN_CONSUMPTION_MAX_WORKER_THREAD_POOL_SIZE;
import static com.linkedin.venice.ConfigKeys.ADMIN_CONSUMPTION_TIMEOUT_MINUTES;
import static com.linkedin.venice.ConfigKeys.ADMIN_HELIX_MESSAGING_CHANNEL_ENABLED;
import static com.linkedin.venice.ConfigKeys.ADMIN_HOSTNAME;
import static com.linkedin.venice.ConfigKeys.ADMIN_PORT;
import static com.linkedin.venice.ConfigKeys.ADMIN_SECURE_PORT;
import static com.linkedin.venice.ConfigKeys.ADMIN_TOPIC_REMOTE_CONSUMPTION_ENABLED;
import static com.linkedin.venice.ConfigKeys.ADMIN_TOPIC_SOURCE_REGION;
import static com.linkedin.venice.ConfigKeys.AGGREGATE_REAL_TIME_SOURCE_REGION;
import static com.linkedin.venice.ConfigKeys.ALLOW_CLUSTER_WIPE;
import static com.linkedin.venice.ConfigKeys.CHILD_CLUSTER_ALLOWLIST;
import static com.linkedin.venice.ConfigKeys.CHILD_CLUSTER_D2_PREFIX;
import static com.linkedin.venice.ConfigKeys.CHILD_CLUSTER_D2_SERVICE_NAME;
import static com.linkedin.venice.ConfigKeys.CHILD_CLUSTER_URL_PREFIX;
import static com.linkedin.venice.ConfigKeys.CHILD_CLUSTER_WHITELIST;
import static com.linkedin.venice.ConfigKeys.CHILD_CONTROLLER_ADMIN_TOPIC_CONSUMPTION_ENABLED;
import static com.linkedin.venice.ConfigKeys.CHILD_DATA_CENTER_KAFKA_URL_PREFIX;
import static com.linkedin.venice.ConfigKeys.CLUSTER_DISCOVERY_D2_SERVICE;
import static com.linkedin.venice.ConfigKeys.CONCURRENT_INIT_ROUTINES_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_BACKUP_VERSION_DEFAULT_RETENTION_MS;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_BACKUP_VERSION_METADATA_FETCH_BASED_CLEANUP_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_BACKUP_VERSION_RETENTION_BASED_CLEANUP_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_CLUSTER;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_CLUSTER_LEADER_HAAS;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_CLUSTER_REPLICA;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_CLUSTER_ZK_ADDRESSS;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_DISABLED_REPLICA_ENABLER_INTERVAL_MS;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_DISABLED_ROUTES;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_DISABLE_PARENT_TOPIC_TRUNCATION_UPON_COMPLETION;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_EARLY_DELETE_BACKUP_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_ENABLE_BATCH_PUSH_FROM_ADMIN_IN_CHILD;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_ENABLE_DISABLED_REPLICA_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_ENFORCE_SSL;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_HAAS_SUPER_CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_IN_AZURE_FABRIC;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_PARENT_EXTERNAL_SUPERSET_SCHEMA_GENERATION_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_PARENT_MODE;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_PARENT_SYSTEM_STORE_HEARTBEAT_CHECK_WAIT_TIME_SECONDS;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_PARENT_SYSTEM_STORE_REPAIR_CHECK_INTERVAL_SECONDS;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_PARENT_SYSTEM_STORE_REPAIR_RETRY_COUNT;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_PARENT_SYSTEM_STORE_REPAIR_SERVICE_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_STORE_GRAVEYARD_CLEANUP_DELAY_MINUTES;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_STORE_GRAVEYARD_CLEANUP_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_STORE_GRAVEYARD_CLEANUP_SLEEP_INTERVAL_BETWEEN_LIST_FETCH_MINUTES;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_SYSTEM_SCHEMA_CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_SYSTEM_STORE_ACL_SYNCHRONIZATION_DELAY_MS;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_ZK_SHARED_DAVINCI_PUSH_STATUS_SYSTEM_SCHEMA_STORE_AUTO_CREATION_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_ZK_SHARED_META_SYSTEM_SCHEMA_STORE_AUTO_CREATION_ENABLED;
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_SCAN_ENABLED;
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_SCAN_INTERVAL_IN_SECONDS;
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_SCAN_MAX_OFFLINE_INSTANCE_COUNT;
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_SCAN_MAX_OFFLINE_INSTANCE_RATIO;
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_SCAN_NO_REPORT_RETRY_MAX_ATTEMPTS;
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_SCAN_THREAD_NUMBER;
import static com.linkedin.venice.ConfigKeys.DEPRECATED_TOPIC_MAX_RETENTION_MS;
import static com.linkedin.venice.ConfigKeys.DEPRECATED_TOPIC_RETENTION_MS;
import static com.linkedin.venice.ConfigKeys.EMERGENCY_SOURCE_REGION;
import static com.linkedin.venice.ConfigKeys.ERROR_PARTITION_AUTO_RESET_LIMIT;
import static com.linkedin.venice.ConfigKeys.ERROR_PARTITION_PROCESSING_CYCLE_DELAY;
import static com.linkedin.venice.ConfigKeys.IDENTITY_PARSER_CLASS;
import static com.linkedin.venice.ConfigKeys.KAFKA_ADMIN_CLASS;
import static com.linkedin.venice.ConfigKeys.KAFKA_READ_ONLY_ADMIN_CLASS;
import static com.linkedin.venice.ConfigKeys.KAFKA_WRITE_ONLY_ADMIN_CLASS;
import static com.linkedin.venice.ConfigKeys.KME_REGISTRATION_FROM_MESSAGE_HEADER_ENABLED;
import static com.linkedin.venice.ConfigKeys.META_STORE_WRITER_CLOSE_CONCURRENCY;
import static com.linkedin.venice.ConfigKeys.META_STORE_WRITER_CLOSE_TIMEOUT_MS;
import static com.linkedin.venice.ConfigKeys.MIN_NUMBER_OF_STORE_VERSIONS_TO_PRESERVE;
import static com.linkedin.venice.ConfigKeys.MIN_NUMBER_OF_UNUSED_KAFKA_TOPICS_TO_PRESERVE;
import static com.linkedin.venice.ConfigKeys.NATIVE_REPLICATION_FABRIC_ALLOWLIST;
import static com.linkedin.venice.ConfigKeys.NATIVE_REPLICATION_FABRIC_WHITELIST;
import static com.linkedin.venice.ConfigKeys.NATIVE_REPLICATION_SOURCE_FABRIC;
import static com.linkedin.venice.ConfigKeys.PARENT_CONTROLLER_MAX_ERRORED_TOPIC_NUM_TO_KEEP;
import static com.linkedin.venice.ConfigKeys.PARENT_CONTROLLER_WAITING_TIME_FOR_CONSUMPTION_MS;
import static com.linkedin.venice.ConfigKeys.PARENT_KAFKA_CLUSTER_FABRIC_LIST;
import static com.linkedin.venice.ConfigKeys.PARTICIPANT_MESSAGE_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.PUBSUB_TOPIC_MANAGER_METADATA_FETCHER_CONSUMER_POOL_SIZE;
import static com.linkedin.venice.ConfigKeys.PUBSUB_TOPIC_MANAGER_METADATA_FETCHER_THREAD_POOL_SIZE;
import static com.linkedin.venice.ConfigKeys.PUB_SUB_ADMIN_ADAPTER_FACTORY_CLASS;
import static com.linkedin.venice.ConfigKeys.PUB_SUB_CONSUMER_ADAPTER_FACTORY_CLASS;
import static com.linkedin.venice.ConfigKeys.PUB_SUB_PRODUCER_ADAPTER_FACTORY_CLASS;
import static com.linkedin.venice.ConfigKeys.PUSH_JOB_STATUS_STORE_CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.PUSH_STATUS_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.PUSH_STATUS_STORE_HEARTBEAT_EXPIRATION_TIME_IN_SECONDS;
import static com.linkedin.venice.ConfigKeys.STORAGE_ENGINE_OVERHEAD_RATIO;
import static com.linkedin.venice.ConfigKeys.SYSTEM_SCHEMA_INITIALIZATION_AT_START_TIME_ENABLED;
import static com.linkedin.venice.ConfigKeys.TERMINAL_STATE_TOPIC_CHECK_DELAY_MS;
import static com.linkedin.venice.ConfigKeys.TOPIC_CLEANUP_DELAY_FACTOR;
import static com.linkedin.venice.ConfigKeys.TOPIC_CLEANUP_SEND_CONCURRENT_DELETES_REQUESTS;
import static com.linkedin.venice.ConfigKeys.TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS;
import static com.linkedin.venice.ConfigKeys.TOPIC_CREATION_THROTTLING_TIME_WINDOW_MS;
import static com.linkedin.venice.ConfigKeys.TOPIC_DELETION_STATUS_POLL_INTERVAL_MS;
import static com.linkedin.venice.ConfigKeys.TOPIC_MANAGER_KAFKA_OPERATION_TIMEOUT_MS;
import static com.linkedin.venice.ConfigKeys.UNREGISTER_METRIC_FOR_DELETED_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.USE_PUSH_STATUS_STORE_FOR_INCREMENTAL_PUSH;
import static com.linkedin.venice.ConfigKeys.VENICE_STORAGE_CLUSTER_LEADER_HAAS;
import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_TOPIC_DELETION_STATUS_POLL_INTERVAL_MS_DEFAULT_VALUE;
import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_TOPIC_MANAGER_METADATA_FETCHER_CONSUMER_POOL_SIZE_DEFAULT_VALUE;

import com.linkedin.venice.authorization.DefaultIdentityParser;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.controllerapi.ControllerRoute;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.PubSubAdminAdapterFactory;
import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterFactory;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.pubsub.adapter.kafka.admin.ApacheKafkaAdminAdapter;
import com.linkedin.venice.pubsub.adapter.kafka.admin.ApacheKafkaAdminAdapterFactory;
import com.linkedin.venice.pubsub.adapter.kafka.consumer.ApacheKafkaConsumerAdapterFactory;
import com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerAdapterFactory;
import com.linkedin.venice.status.BatchJobHeartbeatConfigs;
import com.linkedin.venice.utils.RegionUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Configuration which is specific to a Venice controller.
 *
 * It's quite confusing to have both {@link VeniceControllerConfig} and
 * {@link VeniceControllerClusterConfig}. TODO: remove one of them
 */
public class VeniceControllerConfig extends VeniceControllerClusterConfig {
  private static final Logger LOGGER = LogManager.getLogger(VeniceControllerConfig.class);
  private static final String LIST_SEPARATOR = ",\\s*";

  private final int adminPort;

  private final String adminHostname;
  private final int adminSecurePort;
  private final int controllerClusterReplica;
  private final String controllerClusterName;
  private final String controllerClusterZkAddress;
  private final boolean parent;
  private final List<String> childDataCenterAllowlist;
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
  private final long adminConsumptionTimeoutMinute;
  private final long adminConsumptionCycleTimeoutMs;
  private final int adminConsumptionMaxWorkerThreadPoolSize;
  private final double storageEngineOverheadRatio;
  private final long topicCreationThrottlingTimeWindowMs;
  private final long deprecatedJobTopicRetentionMs;
  private final long deprecatedJobTopicMaxRetentionMs;
  private final long topicCleanupSleepIntervalBetweenTopicListFetchMs;

  private final long disabledReplicaEnablerServiceIntervalMs;

  private final int topicCleanupDelayFactor;
  private final int topicManagerKafkaOperationTimeOutMs;
  private final int topicManagerMetadataFetcherConsumerPoolSize;
  private final int topicManagerMetadataFetcherThreadPoolSize;
  private final int minNumberOfUnusedKafkaTopicsToPreserve;
  private final int minNumberOfStoreVersionsToPreserve;
  private final int parentControllerMaxErroredTopicNumToKeep;
  private final String pushJobStatusStoreClusterName;
  private final boolean participantMessageStoreEnabled;
  private final String systemSchemaClusterName;
  private final int topicDeletionStatusPollIntervalMs;
  private final boolean adminHelixMessagingChannelEnabled;
  private final boolean isControllerClusterLeaderHAAS;
  private final boolean isVeniceClusterLeaderHAAS;
  private final String controllerHAASSuperClusterName;
  private final boolean earlyDeleteBackUpEnabled;
  private final boolean sendConcurrentTopicDeleteRequestsEnabled;
  private final boolean enableBatchPushFromAdminInChildController;
  private final boolean adminCheckReadMethodForKafka;
  private final String kafkaAdminClass;
  private final String kafkaWriteOnlyClass;
  private final String kafkaReadOnlyClass;
  private final Map<String, String> childDataCenterKafkaUrlMap;
  private final boolean activeActiveEnabledOnController;
  private final Set<String> activeActiveRealTimeSourceFabrics;
  private final String nativeReplicationSourceFabric;
  private final int errorPartitionAutoResetLimit;
  private final long errorPartitionProcessingCycleDelay;
  private final long backupVersionDefaultRetentionMs;
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

  private final boolean childControllerAdminTopicConsumptionEnabled;

  private final boolean concurrentInitRoutinesEnabled;

  private final boolean controllerInAzureFabric;

  private final boolean usePushStatusStoreForIncrementalPush;

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

  private final PubSubClientsFactory pubSubClientsFactory;

  public VeniceControllerConfig(VeniceProperties props) {
    super(props);
    this.adminPort = props.getInt(ADMIN_PORT);
    this.adminHostname = props.getString(ADMIN_HOSTNAME, () -> Utils.getHostName());
    this.adminSecurePort = props.getInt(ADMIN_SECURE_PORT);
    /**
     * Override the config to false if the "Read" method check is not working as expected.
     */
    this.adminCheckReadMethodForKafka = props.getBoolean(ADMIN_CHECK_READ_METHOD_FOR_KAFKA, true);
    this.controllerClusterName = props.getString(CONTROLLER_CLUSTER, "venice-controllers");
    this.controllerClusterReplica = props.getInt(CONTROLLER_CLUSTER_REPLICA, 3);
    this.controllerClusterZkAddress = props.getString(CONTROLLER_CLUSTER_ZK_ADDRESSS, getZkAddress());
    this.topicCreationThrottlingTimeWindowMs =
        props.getLong(TOPIC_CREATION_THROTTLING_TIME_WINDOW_MS, 10 * Time.MS_PER_SECOND);
    this.parent = props.getBoolean(CONTROLLER_PARENT_MODE, false);
    this.activeActiveEnabledOnController = props.getBoolean(ACTIVE_ACTIVE_ENABLED_ON_CONTROLLER, false);
    this.activeActiveRealTimeSourceFabrics =
        Utils.parseCommaSeparatedStringToSet(props.getString(ACTIVE_ACTIVE_REAL_TIME_SOURCE_FABRIC_LIST, ""));
    validateActiveActiveConfigs();

    // go/inclusivecode deferred(Will be replaced when clients have migrated)
    String dataCenterAllowlist = props.getStringWithAlternative(CHILD_CLUSTER_ALLOWLIST, CHILD_CLUSTER_WHITELIST);
    if (dataCenterAllowlist.isEmpty()) {
      this.childDataCenterControllerUrlMap = Collections.emptyMap();
      this.childDataCenterControllerD2Map = Collections.emptyMap();
      this.childDataCenterAllowlist = Collections.emptyList();
    } else {
      this.childDataCenterControllerUrlMap = parseClusterMap(props, dataCenterAllowlist);
      this.childDataCenterControllerD2Map = parseClusterMap(props, dataCenterAllowlist, true);
      this.childDataCenterAllowlist = Arrays.asList(dataCenterAllowlist.split(LIST_SEPARATOR));
    }
    this.d2ServiceName =
        childDataCenterControllerD2Map.isEmpty() ? null : props.getString(CHILD_CLUSTER_D2_SERVICE_NAME);
    if (this.parent) {
      if (childDataCenterControllerUrlMap.isEmpty() && childDataCenterControllerD2Map.isEmpty()) {
        throw new VeniceException("child controller list can not be empty");
      }
      String parentFabricList = props.getString(PARENT_KAFKA_CLUSTER_FABRIC_LIST, "");
      this.parentFabrics = Utils.parseCommaSeparatedStringToSet(parentFabricList);
      String nativeReplicationSourceFabricAllowlist = props.getStringWithAlternative(
          NATIVE_REPLICATION_FABRIC_ALLOWLIST,
          // go/inclusivecode deferred(will be removed once all configs have migrated)
          NATIVE_REPLICATION_FABRIC_WHITELIST,
          dataCenterAllowlist);
      this.childDataCenterKafkaUrlMap = parseChildDataCenterKafkaUrl(props, nativeReplicationSourceFabricAllowlist);
    } else {
      this.parentFabrics = Collections.emptySet();

      String nativeReplicationSourceFabricAllowlist = props.getStringWithAlternative(
          NATIVE_REPLICATION_FABRIC_ALLOWLIST,
          // go/inclusivecode deferred(will be removed once all configs have migrated)
          NATIVE_REPLICATION_FABRIC_WHITELIST,
          "");
      if (nativeReplicationSourceFabricAllowlist == null || nativeReplicationSourceFabricAllowlist.length() == 0) {
        this.childDataCenterKafkaUrlMap = Collections.emptyMap();
      } else {
        this.childDataCenterKafkaUrlMap = parseChildDataCenterKafkaUrl(props, nativeReplicationSourceFabricAllowlist);
      }
    }
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
    this.adminConsumptionTimeoutMinute = props.getLong(ADMIN_CONSUMPTION_TIMEOUT_MINUTES, TimeUnit.DAYS.toMinutes(5));
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
    this.topicCleanupSleepIntervalBetweenTopicListFetchMs =
        props.getLong(TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS, TimeUnit.SECONDS.toMillis(30)); // 30
                                                                                                                // seconds
    this.topicCleanupDelayFactor = props.getInt(TOPIC_CLEANUP_DELAY_FACTOR, 20); // thisFactor *
                                                                                 // topicCleanupSleepIntervalBetweenTopicListFetchMs
                                                                                 // = delayBeforeTopicDeletion

    this.disabledReplicaEnablerServiceIntervalMs =
        props.getLong(CONTROLLER_DISABLED_REPLICA_ENABLER_INTERVAL_MS, TimeUnit.HOURS.toMillis(16));
    this.topicManagerKafkaOperationTimeOutMs =
        props.getInt(TOPIC_MANAGER_KAFKA_OPERATION_TIMEOUT_MS, 30 * Time.MS_PER_SECOND);
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
    this.participantMessageStoreEnabled = props.getBoolean(PARTICIPANT_MESSAGE_STORE_ENABLED, false);
    this.adminHelixMessagingChannelEnabled = props.getBoolean(ADMIN_HELIX_MESSAGING_CHANNEL_ENABLED, true);
    if (!adminHelixMessagingChannelEnabled && !participantMessageStoreEnabled) {
      throw new VeniceException(
          "Cannot perform kill push job if both " + ADMIN_HELIX_MESSAGING_CHANNEL_ENABLED + " and "
              + PARTICIPANT_MESSAGE_STORE_ENABLED + " are set to false");
    }
    this.systemSchemaClusterName = props.getString(CONTROLLER_SYSTEM_SCHEMA_CLUSTER_NAME, "");
    this.earlyDeleteBackUpEnabled = props.getBoolean(CONTROLLER_EARLY_DELETE_BACKUP_ENABLED, true);
    this.topicDeletionStatusPollIntervalMs = props
        .getInt(TOPIC_DELETION_STATUS_POLL_INTERVAL_MS, PUBSUB_TOPIC_DELETION_STATUS_POLL_INTERVAL_MS_DEFAULT_VALUE); // 2s
    this.isControllerClusterLeaderHAAS = props.getBoolean(CONTROLLER_CLUSTER_LEADER_HAAS, false);
    this.isVeniceClusterLeaderHAAS = props.getBoolean(VENICE_STORAGE_CLUSTER_LEADER_HAAS, false);
    this.controllerHAASSuperClusterName = props.getString(CONTROLLER_HAAS_SUPER_CLUSTER_NAME, "");
    if ((isControllerClusterLeaderHAAS || isVeniceClusterLeaderHAAS) && controllerHAASSuperClusterName.isEmpty()) {
      throw new VeniceException(
          CONTROLLER_HAAS_SUPER_CLUSTER_NAME + " is required for " + CONTROLLER_CLUSTER_LEADER_HAAS + " or "
              + VENICE_STORAGE_CLUSTER_LEADER_HAAS + " to be set to true");
    }
    this.sendConcurrentTopicDeleteRequestsEnabled =
        props.getBoolean(TOPIC_CLEANUP_SEND_CONCURRENT_DELETES_REQUESTS, false);
    this.enableBatchPushFromAdminInChildController =
        props.getBoolean(CONTROLLER_ENABLE_BATCH_PUSH_FROM_ADMIN_IN_CHILD, true);
    this.kafkaAdminClass = props.getString(KAFKA_ADMIN_CLASS, ApacheKafkaAdminAdapter.class.getName());
    this.kafkaWriteOnlyClass = props.getString(KAFKA_WRITE_ONLY_ADMIN_CLASS, kafkaAdminClass);
    this.kafkaReadOnlyClass = props.getString(KAFKA_READ_ONLY_ADMIN_CLASS, kafkaAdminClass);
    this.errorPartitionAutoResetLimit = props.getInt(ERROR_PARTITION_AUTO_RESET_LIMIT, 0);
    this.errorPartitionProcessingCycleDelay =
        props.getLong(ERROR_PARTITION_PROCESSING_CYCLE_DELAY, 5 * Time.MS_PER_MINUTE);
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
    this.pushStatusStoreHeartbeatExpirationTimeInSeconds =
        props.getLong(PUSH_STATUS_STORE_HEARTBEAT_EXPIRATION_TIME_IN_SECONDS, TimeUnit.MINUTES.toSeconds(10));
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
    this.adminTopicRemoteConsumptionEnabled = props.getBoolean(ADMIN_TOPIC_REMOTE_CONSUMPTION_ENABLED, false);
    if (adminTopicRemoteConsumptionEnabled && childDataCenterKafkaUrlMap.isEmpty()) {
      throw new VeniceException("Admin topic remote consumption is enabled but Kafka url map is empty");
    }
    this.adminTopicSourceRegion = props.getString(ADMIN_TOPIC_SOURCE_REGION, "");
    this.aggregateRealTimeSourceRegion = props.getString(AGGREGATE_REAL_TIME_SOURCE_REGION, "");
    this.isAutoMaterializeMetaSystemStoreEnabled =
        props.getBoolean(CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE, false);
    this.isAutoMaterializeDaVinciPushStatusSystemStoreEnabled =
        props.getBoolean(CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE, false);
    this.usePushStatusStoreForIncrementalPush = props.getBoolean(USE_PUSH_STATUS_STORE_FOR_INCREMENTAL_PUSH, false);
    this.metaStoreWriterCloseTimeoutInMS = props.getLong(META_STORE_WRITER_CLOSE_TIMEOUT_MS, 300000L);
    this.metaStoreWriterCloseConcurrency = props.getInt(META_STORE_WRITER_CLOSE_CONCURRENCY, -1);
    this.emergencySourceRegion = props.getString(EMERGENCY_SOURCE_REGION, "");
    this.allowClusterWipe = props.getBoolean(ALLOW_CLUSTER_WIPE, false);
    this.childControllerAdminTopicConsumptionEnabled =
        props.getBoolean(CHILD_CONTROLLER_ADMIN_TOPIC_CONSUMPTION_ENABLED, true);
    this.concurrentInitRoutinesEnabled = props.getBoolean(CONCURRENT_INIT_ROUTINES_ENABLED, false);
    this.controllerInAzureFabric = props.getBoolean(CONTROLLER_IN_AZURE_FABRIC, false);
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

    try {
      String producerFactoryClassName =
          props.getString(PUB_SUB_PRODUCER_ADAPTER_FACTORY_CLASS, ApacheKafkaProducerAdapterFactory.class.getName());
      PubSubProducerAdapterFactory pubSubProducerAdapterFactory =
          (PubSubProducerAdapterFactory) Class.forName(producerFactoryClassName).newInstance();
      String consumerFactoryClassName =
          props.getString(PUB_SUB_CONSUMER_ADAPTER_FACTORY_CLASS, ApacheKafkaConsumerAdapterFactory.class.getName());
      PubSubConsumerAdapterFactory pubSubConsumerAdapterFactory =
          (PubSubConsumerAdapterFactory) Class.forName(consumerFactoryClassName).newInstance();
      String adminFactoryClassName =
          props.getString(PUB_SUB_ADMIN_ADAPTER_FACTORY_CLASS, ApacheKafkaAdminAdapterFactory.class.getName());
      PubSubAdminAdapterFactory pubSubAdminAdapterFactory =
          (PubSubAdminAdapterFactory) Class.forName(adminFactoryClassName).newInstance();

      pubSubClientsFactory = new PubSubClientsFactory(
          pubSubProducerAdapterFactory,
          pubSubConsumerAdapterFactory,
          pubSubAdminAdapterFactory);
    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
      LOGGER.error("Failed to create an instance of pub sub clients factory", e);
      throw new VeniceException(e);
    }

  }

  private void validateActiveActiveConfigs() {
    if (this.activeActiveEnabledOnController && this.activeActiveRealTimeSourceFabrics.isEmpty()) {
      throw new VeniceException(
          String.format(
              "The config %s cannot be empty when the child controller has A/A enabled " + "(%s == true).",
              ACTIVE_ACTIVE_REAL_TIME_SOURCE_FABRIC_LIST,
              ACTIVE_ACTIVE_ENABLED_ON_CONTROLLER));

    } else if (this.activeActiveEnabledOnController) {
      LOGGER.info(
          "A/A is enabled on a child controller and {} == {}",
          ACTIVE_ACTIVE_REAL_TIME_SOURCE_FABRIC_LIST,
          this.activeActiveRealTimeSourceFabrics);
    } else {
      LOGGER.info(
          "A/A is not enabled on child controller. {}",
          !this.activeActiveRealTimeSourceFabrics.isEmpty()
              ? String.format(
                  " But %s is still set to %s.",
                  ACTIVE_ACTIVE_REAL_TIME_SOURCE_FABRIC_LIST,
                  this.activeActiveRealTimeSourceFabrics)
              : "");
    }
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

  public String getControllerClusterZkAddress() {
    return controllerClusterZkAddress;
  }

  public boolean isParent() {
    return parent;
  }

  public long getTopicCreationThrottlingTimeWindowMs() {
    return topicCreationThrottlingTimeWindowMs;
  }

  public long getDeprecatedJobTopicRetentionMs() {
    return deprecatedJobTopicRetentionMs;
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

  public Map<String, String> getChildDataCenterControllerD2Map() {
    return childDataCenterControllerD2Map;
  }

  public Map<String, String> getChildDataCenterKafkaUrlMap() {
    return childDataCenterKafkaUrlMap;
  }

  public List<String> getChildDataCenterAllowlist() {
    return childDataCenterAllowlist;
  }

  public Set<String> getActiveActiveRealTimeSourceFabrics() {
    return activeActiveRealTimeSourceFabrics;
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

  public long getAdminConsumptionTimeoutMinutes() {
    return adminConsumptionTimeoutMinute;
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

  public static Map<String, String> parseClusterMap(VeniceProperties clusterPros, String datacenterAllowlist) {
    return parseClusterMap(clusterPros, datacenterAllowlist, false);
  }

  public double getStorageEngineOverheadRatio() {
    return storageEngineOverheadRatio;
  }

  public int getTopicManagerKafkaOperationTimeOutMs() {
    return topicManagerKafkaOperationTimeOutMs;
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

  public int getTopicDeletionStatusPollIntervalMs() {
    return topicDeletionStatusPollIntervalMs;
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

  public boolean isConcurrentTopicDeleteRequestsEnabled() {
    return sendConcurrentTopicDeleteRequestsEnabled;
  }

  public boolean isEnableBatchPushFromAdminInChildController() {
    return enableBatchPushFromAdminInChildController;
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

  public int getErrorPartitionAutoResetLimit() {
    return errorPartitionAutoResetLimit;
  }

  public long getErrorPartitionProcessingCycleDelay() {
    return errorPartitionProcessingCycleDelay;
  }

  public long getBackupVersionDefaultRetentionMs() {
    return backupVersionDefaultRetentionMs;
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

  public boolean isChildControllerAdminTopicConsumptionEnabled() {
    return childControllerAdminTopicConsumptionEnabled;
  }

  public boolean isConcurrentInitRoutinesEnabled() {
    return concurrentInitRoutinesEnabled;
  }

  public boolean isControllerInAzureFabric() {
    return controllerInAzureFabric;
  }

  public boolean usePushStatusStoreForIncrementalPush() {
    return usePushStatusStoreForIncrementalPush;
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
  public static Map<String, String> parseClusterMap(
      VeniceProperties clusterPros,
      String datacenterAllowlist,
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
      String datacenterAllowlist) {
    return parseChildDataCenterToValue(
        CHILD_DATA_CENTER_KAFKA_URL_PREFIX,
        clusterPros,
        datacenterAllowlist,
        (m, k, v, e) -> m.putIfAbsent(k, v));
  }

  private static Map<String, String> parseChildDataCenterToValue(
      String configPrefix,
      VeniceProperties clusterPros,
      String datacenterAllowlist,
      PutToMap mappingFunction) {
    Properties childDataCenterKafkaUriProps = clusterPros.clipAndFilterNamespace(configPrefix).toProperties();

    if (StringUtils.isEmpty(datacenterAllowlist)) {
      throw new VeniceException("child controller list must have a allowlist");
    }

    Map<String, String> outputMap = new HashMap<>();
    List<String> allowlist = Arrays.asList(datacenterAllowlist.split(LIST_SEPARATOR));

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

      if (allowlist.contains(datacenter)) {
        mappingFunction.apply(outputMap, datacenter, value, errMsg);
      }
    }

    return outputMap;
  }

  /**
   * A function that would put a k/v pair into a map with some processing works.
   */
  @FunctionalInterface
  interface PutToMap {
    void apply(Map<String, String> map, String key, String value, String errorMessage);
  }
}
