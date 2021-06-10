package com.linkedin.venice.controller;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.controllerapi.ControllerRoute;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.admin.ScalaAdminUtils;
import com.linkedin.venice.status.BatchJobHeartbeatConfigs;
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
import org.apache.log4j.Logger;

import static com.linkedin.venice.ConfigConstants.*;
import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.VeniceConstants.*;


/**
 * Configuration which is specific to a Venice controller.
 *
 * It's quite confusing to have both {@link VeniceControllerConfig} and
 * {@link VeniceControllerClusterConfig}. TODO: remove one of them
 */
public class VeniceControllerConfig extends VeniceControllerClusterConfig {
  private static final Logger LOGGER = Logger.getLogger(VeniceControllerConfig.class);

  private final int adminPort;
  private final int adminSecurePort;
  private final int controllerClusterReplica;
  private final String controllerClusterName;
  private final String controllerClusterZkAddress;
  private final boolean parent;
  private final boolean enableTopicReplicator;
  private final Map<String, String> childDataCenterControllerUrlMap;
  private final String d2ServiceName;
  private final Map<String, String> childDataCenterControllerD2Map;
  private final int parentControllerWaitingTimeForConsumptionMs;
  private final boolean batchJobHeartbeatEnabled;
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
  private final int topicCleanupDelayFactor;
  private final int topicManagerKafkaOperationTimeOutMs;
  private final boolean enableTopicReplicatorSSL;
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
  private final Map<String, String> childDataCenterKafkaUrlMap;
  private final Map<String, String> childDataCenterKafkaZkMap;
  private final String nativeReplicationSourceFabric;
  private final int errorPartitionAutoResetLimit;
  private final long errorPartitionProcessingCycleDelay;
  private final long backupVersionDefaultRetentionMs;
  private final boolean backupVersionRetentionBasedCleanupEnabled;
  private final boolean enforceSSLOnly;
  private final long terminalStateTopicCheckerDelayMs;
  private final boolean isMetadataSystemStoreAutoMaterializeEnabled;
  private final List<ControllerRoute> disabledRoutes;
  /**
   * Test only config used to disable parent topic truncation upon job completion. This is needed because kafka cluster
   * in test environment is shared between parent and child controllers. Truncating topic upon completion will confuse
   * child controllers in certain scenarios.
   */
  private final boolean disableParentTopicTruncationUponCompletion;
  private final Set<String> parentFabrics;
  private final boolean zkSharedMetadataSystemSchemaStoreAutoCreationEnabled;
  /**
   * To decide whether to initialize push status store related components.
   */
  private final boolean isDaVinciPushStatusStoreEnabled;

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
   * Automatically perform new version creation for corresponding meta system store upon new user store creation.
   */
  private final boolean isAutoMaterializeMetaSystemStoreEnabled;

  public VeniceControllerConfig(VeniceProperties props) {
    super(props);
    this.adminPort = props.getInt(ADMIN_PORT);
    this.adminSecurePort = props.getInt(ADMIN_SECURE_PORT);
    /**
     * Override the config to false if the "Read" method check is not working as expected.
     */
    this.adminCheckReadMethodForKafka = props.getBoolean(ADMIN_CHECK_READ_METHOD_FOR_KAFKA, true);
    this.controllerClusterName = props.getString(CONTROLLER_CLUSTER, "venice-controllers");
    this.controllerClusterReplica = props.getInt(CONTROLLER_CLUSTER_REPLICA, 3);
    this.controllerClusterZkAddress = props.getString(CONTROLLER_CLUSTER_ZK_ADDRESSS, getZkAddress());
    this.topicCreationThrottlingTimeWindowMs = props.getLong(TOPIC_CREATION_THROTTLING_TIME_WINDOW_MS, 10 * Time.MS_PER_SECOND);
    this.parent = props.getBoolean(ConfigKeys.CONTROLLER_PARENT_MODE, false);
    if (this.parent) {
      String dataCenterWhitelist = props.getString(CHILD_CLUSTER_WHITELIST);
      this.childDataCenterControllerUrlMap = parseClusterMap(props, dataCenterWhitelist);
      this.childDataCenterControllerD2Map = parseClusterMap(props, dataCenterWhitelist, true);
      this.d2ServiceName = childDataCenterControllerD2Map.isEmpty() ? null : props.getString(CHILD_CLUSTER_D2_SERVICE_NAME);

      if (childDataCenterControllerUrlMap.isEmpty() && childDataCenterControllerD2Map.isEmpty()) {
        throw new VeniceException("child controller list can not be empty");
      }
      String parentFabricList = props.getString(PARENT_KAFKA_CLUSTER_FABRIC_LIST, "");
      this.parentFabrics = Utils.parseCommaSeparatedStringToSet(parentFabricList);
      String nativeReplicationSourceFabricWhitelist = props.getString(NATIVE_REPLICATION_FABRIC_WHITELIST, dataCenterWhitelist);
      this.childDataCenterKafkaUrlMap = parseChildDataCenterKafkaUrl(props, nativeReplicationSourceFabricWhitelist);
      this.childDataCenterKafkaZkMap = parseChildDataCenterKafkaZk(props, nativeReplicationSourceFabricWhitelist);
    } else {
      this.childDataCenterControllerUrlMap = Collections.emptyMap();
      this.childDataCenterControllerD2Map = Collections.emptyMap();
      this.d2ServiceName = null;
      this.parentFabrics = Collections.emptySet();
      String nativeReplicationSourceFabricWhitelist = props.getString(NATIVE_REPLICATION_FABRIC_WHITELIST, "");
      if (nativeReplicationSourceFabricWhitelist == null || nativeReplicationSourceFabricWhitelist.length() == 0) {
        this.childDataCenterKafkaUrlMap = Collections.emptyMap();
        this.childDataCenterKafkaZkMap = Collections.emptyMap();
      } else {
        this.childDataCenterKafkaUrlMap = parseChildDataCenterKafkaUrl(props, nativeReplicationSourceFabricWhitelist);
        this.childDataCenterKafkaZkMap = parseChildDataCenterKafkaZk(props, nativeReplicationSourceFabricWhitelist);
      }
    }
    this.nativeReplicationSourceFabric = props.getString(NATIVE_REPLICATION_SOURCE_FABRIC, "");
    this.parentControllerWaitingTimeForConsumptionMs = props.getInt(ConfigKeys.PARENT_CONTROLLER_WAITING_TIME_FOR_CONSUMPTION_MS, 30 * Time.MS_PER_SECOND);
    this.batchJobHeartbeatEnabled = props.getBoolean(BatchJobHeartbeatConfigs.HEARTBEAT_ENABLED_CONFIG.getConfigName(), false);
    this.batchJobHeartbeatTimeout = Duration.ofMillis(props.getLong(
        BatchJobHeartbeatConfigs.HEARTBEAT_CONTROLLER_TIMEOUT_CONFIG.getConfigName(),
        BatchJobHeartbeatConfigs.HEARTBEAT_CONTROLLER_TIMEOUT_CONFIG.getDefaultValue()
    ));
    this.batchJobHeartbeatInitialBufferTime = Duration.ofMillis(props.getLong(
        BatchJobHeartbeatConfigs.HEARTBEAT_CONTROLLER_INITIAL_DELAY_CONFIG.getConfigName(),
        BatchJobHeartbeatConfigs.HEARTBEAT_CONTROLLER_INITIAL_DELAY_CONFIG.getDefaultValue()
    ));
    this.adminConsumptionTimeoutMinute = props.getLong(ADMIN_CONSUMPTION_TIMEOUT_MINUTES, TimeUnit.DAYS.toMinutes(5));
    this.adminConsumptionCycleTimeoutMs = props.getLong(ConfigKeys.ADMIN_CONSUMPTION_CYCLE_TIMEOUT_MS, TimeUnit.MINUTES.toMillis(30));
    this.adminConsumptionMaxWorkerThreadPoolSize = props.getInt(ConfigKeys.ADMIN_CONSUMPTION_MAX_WORKER_THREAD_POOL_SIZE, 1);

    this.enableTopicReplicator = props.getBoolean(ENABLE_TOPIC_REPLICATOR, true);
    this.enableTopicReplicatorSSL = props.getBoolean(ENABLE_TOPIC_REPLICATOR_SSL, false);
    this.storageEngineOverheadRatio = props.getDouble(STORAGE_ENGINE_OVERHEAD_RATIO, 0.85d);

    // The default retention will allow Kafka remove as much data as possible.
    this.deprecatedJobTopicRetentionMs = props.getLong(DEPRECATED_TOPIC_RETENTION_MS, TimeUnit.SECONDS.toMillis(5)); // 5 seconds
    this.deprecatedJobTopicMaxRetentionMs = props.getLong(DEPRECATED_TOPIC_MAX_RETENTION_MS, TimeUnit.SECONDS.toMillis(60)); // 1 min
    if (this.deprecatedJobTopicMaxRetentionMs < this.deprecatedJobTopicRetentionMs) {
      throw new VeniceException("Config: " + DEPRECATED_TOPIC_MAX_RETENTION_MS + " with value: " + this.deprecatedJobTopicMaxRetentionMs +
          " should be larger than config: " + DEPRECATED_TOPIC_RETENTION_MS + " with value: " + this.deprecatedJobTopicRetentionMs);
    }
    this.topicCleanupSleepIntervalBetweenTopicListFetchMs = props.getLong(TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS, TimeUnit.SECONDS.toMillis(30)); // 30 seconds
    this.topicCleanupDelayFactor = props.getInt(TOPIC_CLEANUP_DELAY_FACTOR, 2); // thisFactor * topicCleanupSleepIntervalBetweenTopicListFetchMs = delayBeforeTopicDeletion

    this.topicManagerKafkaOperationTimeOutMs = props.getInt(TOPIC_MANAGER_KAFKA_OPERATION_TIMEOUT_MS, 30 * Time.MS_PER_SECOND);

    this.minNumberOfUnusedKafkaTopicsToPreserve = props.getInt(MIN_NUMBER_OF_UNUSED_KAFKA_TOPICS_TO_PRESERVE, 2);
    this.minNumberOfStoreVersionsToPreserve = props.getInt(MIN_NUMBER_OF_STORE_VERSIONS_TO_PRESERVE, 2);
    if (minNumberOfStoreVersionsToPreserve < 1) {
      throw new VeniceException("The minimal acceptable value for '" + MIN_NUMBER_OF_STORE_VERSIONS_TO_PRESERVE + "' is 1.");
    }
    // By default, keep 0 errored topics per store in parent controller
    this.parentControllerMaxErroredTopicNumToKeep = props.getInt(PARENT_CONTROLLER_MAX_ERRORED_TOPIC_NUM_TO_KEEP, 0);

    this.pushJobStatusStoreClusterName = props.getString(PUSH_JOB_STATUS_STORE_CLUSTER_NAME, "");
    this.participantMessageStoreEnabled = props.getBoolean(PARTICIPANT_MESSAGE_STORE_ENABLED, false);
    this.adminHelixMessagingChannelEnabled = props.getBoolean(ADMIN_HELIX_MESSAGING_CHANNEL_ENABLED, true);
    if (!adminHelixMessagingChannelEnabled && !participantMessageStoreEnabled) {
      throw new VeniceException("Cannot perform kill push job if both " + ADMIN_HELIX_MESSAGING_CHANNEL_ENABLED
          + " and " + PARTICIPANT_MESSAGE_STORE_ENABLED + " are set to false");
    }
    this.systemSchemaClusterName = props.getString(CONTROLLER_SYSTEM_SCHEMA_CLUSTER_NAME, "");
    this.earlyDeleteBackUpEnabled = props.getBoolean(CONTROLLER_EARLY_DELETE_BACKUP_ENABLED, true);
    this.topicDeletionStatusPollIntervalMs = props.getInt(TOPIC_DELETION_STATUS_POLL_INTERVAL_MS, DEFAULT_TOPIC_DELETION_STATUS_POLL_INTERVAL_MS); // 2s
    this.isControllerClusterLeaderHAAS = props.getBoolean(CONTROLLER_CLUSTER_LEADER_HAAS, false);
    this.isVeniceClusterLeaderHAAS = props.getBoolean(VENICE_STORAGE_CLUSTER_LEADER_HAAS, false);
    this.controllerHAASSuperClusterName = props.getString(CONTROLLER_HAAS_SUPER_CLUSTER_NAME, "");
    if ((isControllerClusterLeaderHAAS || isVeniceClusterLeaderHAAS) && controllerHAASSuperClusterName.isEmpty()) {
      throw new VeniceException(CONTROLLER_HAAS_SUPER_CLUSTER_NAME + " is required for "
          + CONTROLLER_CLUSTER_LEADER_HAAS + " or " + VENICE_STORAGE_CLUSTER_LEADER_HAAS + " to be set to true");
    }
    this.sendConcurrentTopicDeleteRequestsEnabled = props.getBoolean(TOPIC_CLEANUP_SEND_CONCURRENT_DELETES_REQUESTS, false);
    this.enableBatchPushFromAdminInChildController = props.getBoolean(CONTROLLER_ENABLE_BATCH_PUSH_FROM_ADMIN_IN_CHILD, true);
    this.kafkaAdminClass = props.getString(KAFKA_ADMIN_CLASS, ScalaAdminUtils.class.getName());
    this.errorPartitionAutoResetLimit = props.getInt(ERROR_PARTITION_AUTO_RESET_LIMIT, 0);
    this.errorPartitionProcessingCycleDelay = props.getLong(ERROR_PARTITION_PROCESSING_CYCLE_DELAY, 5 * Time.MS_PER_MINUTE);
    this.backupVersionDefaultRetentionMs = props.getLong(CONTROLLER_BACKUP_VERSION_DEFAULT_RETENTION_MS, TimeUnit.DAYS.toMillis(7)); // 1 week
    this.backupVersionRetentionBasedCleanupEnabled = props.getBoolean(CONTROLLER_BACKUP_VERSION_RETENTION_BASED_CLEANUP_ENABLED, false);
    this.enforceSSLOnly = props.getBoolean(CONTROLLER_ENFORCE_SSL, false); // By default, allow both secure and insecure routes
    this.terminalStateTopicCheckerDelayMs = props.getLong(TERMINAL_STATE_TOPIC_CHECK_DELAY_MS, TimeUnit.MINUTES.toMillis(10));
    this.disableParentTopicTruncationUponCompletion = props.getBoolean(CONTROLLER_DISABLE_PARENT_TOPIC_TRUNCATION_UPON_COMPLETION, false);
    /**
     * Disable the zk shared metadata system schema store by default until the schema is fully finalized.
     */
    this.zkSharedMetadataSystemSchemaStoreAutoCreationEnabled = props.getBoolean(CONTROLLER_ZK_SHARED_METADATA_SYSTEM_SCHEMA_STORE_AUTO_CREATION_ENABLED, false);
    this.isMetadataSystemStoreAutoMaterializeEnabled = props.getBoolean(
        CONTROLLER_AUTO_MATERIALIZE_METADATA_SYSTEM_STORE_ENABLED, false);
    this.pushStatusStoreHeartbeatExpirationTimeInSeconds = props.getLong(PUSH_STATUS_STORE_HEARTBEAT_EXPIRATION_TIME_IN_SECONDS, TimeUnit.MINUTES.toSeconds(10));
    this.isDaVinciPushStatusStoreEnabled =  props.getBoolean(PUSH_STATUS_STORE_ENABLED, false);
    this.zkSharedDaVinciPushStatusSystemSchemaStoreAutoCreationEnabled = props.getBoolean(CONTROLLER_ZK_SHARED_DAVINCI_PUSH_STATUS_SYSTEM_SCHEMA_STORE_AUTO_CREATION_ENABLED, false);
    this.systemStoreAclSynchronizationDelayMs = props.getLong(CONTROLLER_SYSTEM_STORE_ACL_SYNCHRONIZATION_DELAY_MS, TimeUnit.HOURS.toMillis(1));
    String regionNameFromConfig = props.getString(LOCAL_REGION_NAME, "");
    if (!Utils.isNullOrEmpty(regionNameFromConfig)) {
      this.regionName = regionNameFromConfig + (parent ? ".parent" : "");
    } else {
      String regionNameFromEnv = null;
      try {
        regionNameFromEnv = System.getenv(ENVIRONMENT_CONFIG_KEY_FOR_REGION_NAME);
        LOGGER.info("Region name from environment config: " + regionNameFromEnv);
        if (regionNameFromEnv == null) {
          regionNameFromEnv = System.getProperty(SYSTEM_PROPERTY_FOR_APP_RUNNING_REGION);
          LOGGER.info("Region name from System property: " + regionNameFromEnv);
        }
      } catch (Exception e) {
        LOGGER.warn("Error when trying to retrieve environment variable for region name; will use default value instead.", e);
      }
      this.regionName = Utils.isNullOrEmpty(regionNameFromEnv)
                        ? ""
                        : regionNameFromEnv + (parent ? ".parent" : "");
    }
    LOGGER.info("Final region name for this node: " + this.regionName);
    this.disabledRoutes = parseControllerRoutes(props, CONTROLLER_DISABLED_ROUTES, Collections.emptyList());
    this.adminTopicRemoteConsumptionEnabled = props.getBoolean(ADMIN_TOPIC_REMOTE_CONSUMPTION_ENABLED, false);
    if (adminTopicRemoteConsumptionEnabled && (childDataCenterKafkaUrlMap == null || childDataCenterKafkaUrlMap.isEmpty())) {
      throw new VeniceException("Admin topic remote consumption is enabled but Kafka url map is empty");
    }
    this.adminTopicSourceRegion = props.getString(ADMIN_TOPIC_SOURCE_REGION, "");
    this.aggregateRealTimeSourceRegion = props.getString(AGGREGATE_REAL_TIME_SOURCE_REGION, "");
    this.isAutoMaterializeMetaSystemStoreEnabled = props.getBoolean(CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE, false);
  }

  public int getAdminPort() {
    return adminPort;
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

  public int getTopicCleanupDelayFactor() {
    return topicCleanupDelayFactor;
  }

  /**
   * Map where keys are logical, human-readable names for child clusters (suitable for printing in logs or other output)
   * values are a list of cluster URLs that can be used to reach that cluster with the controller client.  List provides
   * redundancy in case of hardware or other failure.  Clients of this list should be sure they use another url if the
   * first one fails.
   *
   * @return
   */
  public Map<String, String> getChildDataCenterControllerUrlMap(){
    return childDataCenterControllerUrlMap;
  }

  public String getD2ServiceName() {
    return d2ServiceName;
  }

  public Map<String, String> getChildDataCenterControllerD2Map() {
    return childDataCenterControllerD2Map;
  }

  public Map<String, String> getChildDataCenterKafkaUrlMap() {
    return childDataCenterKafkaUrlMap;
  }

  public Map<String, String> getChildDataCenterKafkaZkMap() {
    return childDataCenterKafkaZkMap;
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

  public boolean getBatchJobHeartbeatEnabled() {
    return batchJobHeartbeatEnabled;
  }

  public Duration getBatchJobHeartbeatTimeout() {
    return batchJobHeartbeatTimeout;
  }

  public Duration getBatchJobHeartbeatInitialBufferTime() {
    return batchJobHeartbeatInitialBufferTime;
  }

  public long getAdminConsumptionTimeoutMinutes(){
    return adminConsumptionTimeoutMinute;
  }

  public long getAdminConsumptionCycleTimeoutMs() { return adminConsumptionCycleTimeoutMs; }

  public int getAdminConsumptionMaxWorkerThreadPoolSize() { return adminConsumptionMaxWorkerThreadPoolSize; }

  public boolean isEnableTopicReplicator() {
    return enableTopicReplicator;
  }

  public static Map<String, String> parseClusterMap(VeniceProperties clusterPros, String datacenterWhitelist) {
    return parseClusterMap(clusterPros, datacenterWhitelist, false);
  }

  public double getStorageEngineOverheadRatio() {
    return storageEngineOverheadRatio;
  }

  public int getTopicManagerKafkaOperationTimeOutMs() {
    return topicManagerKafkaOperationTimeOutMs;
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

  public String getPushJobStatusStoreClusterName() { return pushJobStatusStoreClusterName; }

  public boolean isParticipantMessageStoreEnabled() { return participantMessageStoreEnabled; }

  public boolean isDaVinciPushStatusEnabled() { return true; }

  public String getSystemSchemaClusterName() { return systemSchemaClusterName; }

  public int getTopicDeletionStatusPollIntervalMs() {
    return topicDeletionStatusPollIntervalMs;
  }

  public boolean isAdminHelixMessagingChannelEnabled() { return  adminHelixMessagingChannelEnabled; }

  public boolean isControllerClusterLeaderHAAS() { return isControllerClusterLeaderHAAS; }

  public boolean isVeniceClusterLeaderHAAS() { return isVeniceClusterLeaderHAAS; }

  public String getControllerHAASSuperClusterName() { return controllerHAASSuperClusterName; }

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

  public boolean isControllerEnforceSSLOnly() {
    return enforceSSLOnly;
  }

  public long getTerminalStateTopicCheckerDelayMs() {
    return terminalStateTopicCheckerDelayMs;
  }

  public boolean disableParentTopicTruncationUponCompletion() {
    return disableParentTopicTruncationUponCompletion;
  }

  public boolean isZkSharedMetadataSystemSchemaStoreAutoCreationEnabled() {
    return zkSharedMetadataSystemSchemaStoreAutoCreationEnabled;
  }

  public boolean isMetadataSystemStoreAutoMaterializeEnabled() {
    return isMetadataSystemStoreAutoMaterializeEnabled;
  }

  public long getPushStatusStoreHeartbeatExpirationTimeInSeconds() {
    return pushStatusStoreHeartbeatExpirationTimeInSeconds;
  }

  public boolean isDaVinciPushStatusStoreEnabled() {
    return isDaVinciPushStatusStoreEnabled;
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

  public static List<ControllerRoute> parseControllerRoutes(VeniceProperties clusterProps, String property, List<String> defaultValue) {
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

  /**
   * The config should follow the format below:
   * CHILD_CLUSTER_URL_PREFIX.fabricName1=controllerUrls_in_fabric1
   * CHILD_CLUSTER_URL_PREFIX.fabricName2=controllerUrls_in_fabric2
   *
   * This helper function will parse the config with above format and return a Map from data center to
   * its controller urls.
   *
   * @param clusterPros list of child controller uris
   * @param datacenterWhitelist data centers that are taken into account
   * @param D2Routing whether uses D2 to route or not
   * @return
   */
  public static Map<String, String> parseClusterMap(VeniceProperties clusterPros, String datacenterWhitelist, Boolean D2Routing) {
    String propsPrefix =  D2Routing ? CHILD_CLUSTER_D2_PREFIX : CHILD_CLUSTER_URL_PREFIX;
    return parseChildDataCenterToValue(propsPrefix, clusterPros, datacenterWhitelist,
        (m, k, v, errMsg) -> {
          m.computeIfAbsent(k, key -> {
            String[] uriList = v.split(",\\s*");

            if (D2Routing && uriList.length != 1) {
              throw new VeniceException(errMsg + ": can only have 1 zookeeper url");
            }

            if (!D2Routing) {
              if (uriList.length == 0) {
                throw new VeniceException(errMsg + ": urls can not be empty");
              }

              if (Arrays.stream(uriList).anyMatch(uri -> !uri.startsWith("http://"))) {
                throw new VeniceException(errMsg + ": urls must begin with http://");
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
  private static Map<String, String> parseChildDataCenterKafkaUrl(VeniceProperties clusterPros, String datacenterWhitelist) {
    return parseChildDataCenterToValue(CHILD_DATA_CENTER_KAFKA_URL_PREFIX, clusterPros, datacenterWhitelist,
        (m, k, v, e) -> {
          m.putIfAbsent(k, v);
        });
  }

  /**
   * The config should follow the format below:
   * $CHILD_DATA_CENTER_KAFKA_ZK_PREFIX.fabricName1=kafkaZk_in_fabric1
   * $CHILD_DATA_CENTER_KAFKA_ZK_PREFIX.fabricName2=kafkaZk_in_fabric2
   *
   * This helper function will parse the config with above format and return a Map from data center to
   * its Kafka zk address.
   */
  private static Map<String, String> parseChildDataCenterKafkaZk(VeniceProperties clusterPros, String datacenterWhitelist) {
    return parseChildDataCenterToValue(CHILD_DATA_CENTER_KAFKA_ZK_PREFIX, clusterPros, datacenterWhitelist,
        (m, k, v, e) -> {
          m.putIfAbsent(k, v);
        });
  }

  private static Map<String, String> parseChildDataCenterToValue(String configPrefix, VeniceProperties clusterPros,
      String datacenterWhitelist, PutToMap mappingFunction) {
    Properties childDataCenterKafkaUriProps = clusterPros.clipAndFilterNamespace(configPrefix).toProperties();

    if (Utils.isNullOrEmpty(datacenterWhitelist)) {
      throw new VeniceException("child controller list must have a whitelist");
    }

    Map<String, String> outputMap = new HashMap<>();
    List<String> whitelist = Arrays.asList(datacenterWhitelist.split(",\\s*"));

    for (Map.Entry<Object, Object> uriEntry : childDataCenterKafkaUriProps.entrySet()) {
      String datacenter = (String) uriEntry.getKey();
      String value = (String) uriEntry.getValue();

      String errMsg = "Invalid configuration " + configPrefix + "." + datacenter;
      if (datacenter.isEmpty()) {
        throw new VeniceException(errMsg + ": data center name can't be empty for value: " + value);
      }

      if (value.isEmpty()) {
        throw new VeniceException(errMsg + ": found no value for: " + datacenter);
      }

      if (whitelist.contains(datacenter)) {
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
