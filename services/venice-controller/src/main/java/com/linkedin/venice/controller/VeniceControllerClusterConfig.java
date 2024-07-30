package com.linkedin.venice.controller;

import static com.linkedin.venice.CommonConfigKeys.SSL_FACTORY_CLASS_NAME;
import static com.linkedin.venice.ConfigKeys.ADMIN_TOPIC_REPLICATION_FACTOR;
import static com.linkedin.venice.ConfigKeys.CHILD_CLUSTER_ALLOWLIST;
import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.CLUSTER_TO_D2;
import static com.linkedin.venice.ConfigKeys.CLUSTER_TO_SERVER_D2;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_DEFAULT_READ_QUOTA_PER_ROUTER;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_DISABLE_PARENT_REQUEST_TOPIC_FOR_STREAM_PUSHES;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_JETTY_CONFIG_OVERRIDE_PREFIX;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_NAME;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_SCHEMA_VALIDATION_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_SSL_ENABLED;
import static com.linkedin.venice.ConfigKeys.DEFAULT_MAX_NUMBER_OF_PARTITIONS;
import static com.linkedin.venice.ConfigKeys.DEFAULT_NUMBER_OF_PARTITION;
import static com.linkedin.venice.ConfigKeys.DEFAULT_NUMBER_OF_PARTITION_FOR_HYBRID;
import static com.linkedin.venice.ConfigKeys.DEFAULT_OFFLINE_PUSH_STRATEGY;
import static com.linkedin.venice.ConfigKeys.DEFAULT_PARTITION_SIZE;
import static com.linkedin.venice.ConfigKeys.DEFAULT_READ_STRATEGY;
import static com.linkedin.venice.ConfigKeys.DEFAULT_REPLICA_FACTOR;
import static com.linkedin.venice.ConfigKeys.DEFAULT_ROUTING_STRATEGY;
import static com.linkedin.venice.ConfigKeys.DELAY_TO_REBALANCE_MS;
import static com.linkedin.venice.ConfigKeys.ENABLE_ACTIVE_ACTIVE_REPLICATION_AS_DEFAULT_FOR_BATCH_ONLY_STORE;
import static com.linkedin.venice.ConfigKeys.ENABLE_ACTIVE_ACTIVE_REPLICATION_AS_DEFAULT_FOR_HYBRID_STORE;
import static com.linkedin.venice.ConfigKeys.ENABLE_HYBRID_PUSH_SSL_ALLOWLIST;
import static com.linkedin.venice.ConfigKeys.ENABLE_HYBRID_PUSH_SSL_WHITELIST;
import static com.linkedin.venice.ConfigKeys.ENABLE_INCREMENTAL_PUSH_FOR_HYBRID_ACTIVE_ACTIVE_USER_STORES;
import static com.linkedin.venice.ConfigKeys.ENABLE_NATIVE_REPLICATION_AS_DEFAULT_FOR_BATCH_ONLY;
import static com.linkedin.venice.ConfigKeys.ENABLE_NATIVE_REPLICATION_AS_DEFAULT_FOR_HYBRID;
import static com.linkedin.venice.ConfigKeys.ENABLE_NATIVE_REPLICATION_FOR_BATCH_ONLY;
import static com.linkedin.venice.ConfigKeys.ENABLE_NATIVE_REPLICATION_FOR_HYBRID;
import static com.linkedin.venice.ConfigKeys.ENABLE_OFFLINE_PUSH_SSL_ALLOWLIST;
import static com.linkedin.venice.ConfigKeys.ENABLE_OFFLINE_PUSH_SSL_WHITELIST;
import static com.linkedin.venice.ConfigKeys.ENABLE_PARTIAL_UPDATE_FOR_HYBRID_ACTIVE_ACTIVE_USER_STORES;
import static com.linkedin.venice.ConfigKeys.ENABLE_PARTIAL_UPDATE_FOR_HYBRID_NON_ACTIVE_ACTIVE_USER_STORES;
import static com.linkedin.venice.ConfigKeys.ENABLE_PARTITION_COUNT_ROUND_UP;
import static com.linkedin.venice.ConfigKeys.FORCE_LEADER_ERROR_REPLICA_FAIL_OVER_ENABLED;
import static com.linkedin.venice.ConfigKeys.HELIX_REBALANCE_ALG;
import static com.linkedin.venice.ConfigKeys.HELIX_SEND_MESSAGE_TIMEOUT_MS;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.KAFKA_LOG_COMPACTION_FOR_HYBRID_STORES;
import static com.linkedin.venice.ConfigKeys.KAFKA_MIN_IN_SYNC_REPLICAS;
import static com.linkedin.venice.ConfigKeys.KAFKA_MIN_IN_SYNC_REPLICAS_ADMIN_TOPICS;
import static com.linkedin.venice.ConfigKeys.KAFKA_MIN_IN_SYNC_REPLICAS_RT_TOPICS;
import static com.linkedin.venice.ConfigKeys.KAFKA_MIN_LOG_COMPACTION_LAG_MS;
import static com.linkedin.venice.ConfigKeys.KAFKA_OVER_SSL;
import static com.linkedin.venice.ConfigKeys.KAFKA_REPLICATION_FACTOR;
import static com.linkedin.venice.ConfigKeys.KAFKA_REPLICATION_FACTOR_RT_TOPICS;
import static com.linkedin.venice.ConfigKeys.KAFKA_SECURITY_PROTOCOL;
import static com.linkedin.venice.ConfigKeys.LEAKED_PUSH_STATUS_CLEAN_UP_SERVICE_SLEEP_INTERVAL_MS;
import static com.linkedin.venice.ConfigKeys.LEAKED_RESOURCE_ALLOWED_LINGER_TIME_MS;
import static com.linkedin.venice.ConfigKeys.MIN_ACTIVE_REPLICA;
import static com.linkedin.venice.ConfigKeys.NATIVE_REPLICATION_SOURCE_FABRIC_AS_DEFAULT_FOR_BATCH_ONLY_STORES;
import static com.linkedin.venice.ConfigKeys.NATIVE_REPLICATION_SOURCE_FABRIC_AS_DEFAULT_FOR_HYBRID_STORES;
import static com.linkedin.venice.ConfigKeys.OFFLINE_JOB_START_TIMEOUT_MS;
import static com.linkedin.venice.ConfigKeys.PARTITION_COUNT_ROUND_UP_SIZE;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.PUSH_MONITOR_TYPE;
import static com.linkedin.venice.ConfigKeys.PUSH_SSL_ALLOWLIST;
import static com.linkedin.venice.ConfigKeys.PUSH_SSL_WHITELIST;
import static com.linkedin.venice.ConfigKeys.REFRESH_ATTEMPTS_FOR_ZK_RECONNECT;
import static com.linkedin.venice.ConfigKeys.REFRESH_INTERVAL_FOR_ZK_RECONNECT_MS;
import static com.linkedin.venice.ConfigKeys.REPLICATION_METADATA_VERSION;
import static com.linkedin.venice.ConfigKeys.SSL_KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.SSL_TO_KAFKA_LEGACY;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;
import static com.linkedin.venice.SSLConfig.DEFAULT_CONTROLLER_SSL_ENABLED;
import static com.linkedin.venice.VeniceConstants.DEFAULT_PER_ROUTER_READ_QUOTA;
import static com.linkedin.venice.VeniceConstants.DEFAULT_SSL_FACTORY_CLASS_NAME;
import static com.linkedin.venice.pubsub.PubSubConstants.DEFAULT_KAFKA_MIN_LOG_COMPACTION_LAG_MS;
import static com.linkedin.venice.pubsub.PubSubConstants.DEFAULT_KAFKA_REPLICATION_FACTOR;

import com.linkedin.venice.SSLConfig;
import com.linkedin.venice.exceptions.ConfigurationException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.pushmonitor.LeakedPushStatusCleanUpService;
import com.linkedin.venice.pushmonitor.PushMonitorType;
import com.linkedin.venice.utils.KafkaSSLUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.helix.controller.rebalancer.strategy.CrushRebalanceStrategy;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Configuration which is specific to a Venice cluster used by Venice controller.
 */
public class VeniceControllerClusterConfig {
  private static final Logger LOGGER = LogManager.getLogger(VeniceControllerClusterConfig.class);

  private final VeniceProperties props;
  private String clusterName;
  private String zkAddress;
  private String controllerName;
  private PersistenceType persistenceType;
  private ReadStrategy readStrategy;
  private OfflinePushStrategy offlinePushStrategy;
  private RoutingStrategy routingStrategy;
  private int replicationFactor;
  private int minNumberOfPartitions;
  private int minNumberOfPartitionsForHybrid;
  private int maxNumberOfPartitions;
  private long partitionSize;
  private boolean partitionCountRoundUpEnabled;
  private int partitionCountRoundUpSize;
  private long offLineJobWaitTimeInMilliseconds;
  private Map<String, String> clusterToD2Map;
  private Map<String, String> clusterToServerD2Map;
  private boolean sslToKafka;
  private int helixSendMessageTimeoutMilliseconds;
  private int adminTopicReplicationFactor;
  private PushMonitorType pushMonitorType;

  private String kafkaSecurityProtocol;
  // SSL related config
  Optional<SSLConfig> sslConfig;
  private String sslFactoryClassName;
  private int refreshAttemptsForZkReconnect;
  private long refreshIntervalForZkReconnectInMs;
  private boolean enableOfflinePushSSLAllowlist;
  private boolean enableNearlinePushSSLAllowlist;
  private List<String> pushSSLAllowlist;

  /**
   * TODO: the follower 3 cluster level configs remains in the code base in case the new cluster level configs are not
   *       working as expected. Once the new cluster level configs for native replication have been tested in prod, retire
   *       the following configs.
   */
  /**
   * When this option is enabled, all new batch-only store versions created will have native replication enabled so long
   * as the store has leader follower also enabled.
   */
  private boolean nativeReplicationEnabledForBatchOnly;

  /**
   * When this option is enabled, all new hybrid store versions created will have native replication enabled so long
   * as the store has leader follower also enabled.
   */
  private boolean nativeReplicationEnabledForHybrid;

  /**
   * When this option is enabled, all new batch-only stores will have native replication enabled in store config so long
   * as the store has leader follower also enabled.
   */
  private boolean nativeReplicationEnabledAsDefaultForBatchOnly;

  /**
   * When this option is enabled, all new hybrid stores will have native replication enabled in store config so long
   * as the store has leader follower also enabled.
   */
  private boolean nativeReplicationEnabledAsDefaultForHybrid;

  private String nativeReplicationSourceFabricAsDefaultForBatchOnly;
  private String nativeReplicationSourceFabricAsDefaultForHybrid;

  /**
   * When the following option is enabled, active-active enabled new user hybrid store will automatically
   * have incremental push enabled.
   */
  private boolean enabledIncrementalPushForHybridActiveActiveUserStores;

  private boolean enablePartialUpdateForHybridActiveActiveUserStores;
  private boolean enablePartialUpdateForHybridNonActiveActiveUserStores;

  /**
   * When this option is enabled, all new batch-only stores will have active-active replication enabled in store config so long
   * as the store has leader follower also enabled.
   */
  private boolean activeActiveReplicationEnabledAsDefaultForBatchOnly;

  /**
   * When this option is enabled, all new hybrid stores will have active-active replication enabled in store config so long
   * as the store has leader follower also enabled.
   */
  private boolean activeActiveReplicationEnabledAsDefaultForHybrid;

  /**
   * When this option is enabled, new schema registration will validate the schema against all existing store value schemas.
   */
  private boolean controllerSchemaValidationEnabled;

  /**
   * After server disconnecting for delayToRebalanceMS, helix would trigger the re-balance immediately.
   */
  private long delayToRebalanceMS;
  /**
   * If the replica count smaller than minActiveReplica, helix would trigger the re-balance immediately.
   * This config is deprecated. Replication factor config is moved to store/version-level config
   */
  @Deprecated
  private int minActiveReplica;

  /**
   * kafka Bootstrap Urls . IF there is more than one url, they are separated by commas
   */
  private String kafkaBootstrapServers;

  private String sslKafkaBootStrapServers;

  /**
   * Number of replicas for each kafka topic. It can be different from the Venice Storage Node replication factor,
   * defined by {@value com.linkedin.venice.ConfigKeys#DEFAULT_REPLICA_FACTOR}.
   */
  private int kafkaReplicationFactor;
  private int kafkaReplicationFactorRTTopics;
  private Optional<Integer> minInSyncReplicas;
  private Optional<Integer> minInSyncReplicasRealTimeTopics;
  private Optional<Integer> minInSyncReplicasAdminTopics;
  private boolean kafkaLogCompactionForHybridStores;
  private long kafkaMinLogCompactionLagInMs;

  /**
   * Alg used by helix to decide the mapping between replicas and nodes.
   */
  private String helixRebalanceAlg;

  /**
   * Sleep interval inside {@link LeakedPushStatusCleanUpService}
   */
  private long leakedPushStatusCleanUpServiceSleepIntervalInMs;

  /**
   * The amount of time a leaked resource is allowed to linger before it is cleaned up.
   *
   * A leaked resource per store is allowed to linger for some time in order to gather troubleshooting information.
   */
  private long leakedResourceAllowedLingerTimeInMs;

  /**
   * Jetty config overrides for Spark server
   */
  private VeniceProperties jettyConfigOverrides;

  /**
   * Config which disables request_topic calls to the parent controller for stream pushes.  This is meant to discourage
   * the use of the parent colo for aggregating pushed data, users should instead push to their local colo and allow
   * Venice AA to aggregate the data.
   */
  private boolean disableParentRequestTopicForStreamPushes;

  private int defaultReadQuotaPerRouter;
  private int replicationMetadataVersion;

  private boolean errorLeaderReplicaFailOverEnabled;

  private String childDatacenters;

  public VeniceControllerClusterConfig(VeniceProperties props) {
    try {
      this.props = props;
      initFieldsWithProperties(props);
      LOGGER.info("Loaded configuration");
    } catch (Exception e) {
      String errorMessage = "Can not load properties.";
      LOGGER.error(errorMessage);
      throw new VeniceException(errorMessage, e);
    }
  }

  private void initFieldsWithProperties(VeniceProperties props) {
    clusterName = props.getString(CLUSTER_NAME);
    zkAddress = props.getString(ZOOKEEPER_ADDRESS);
    controllerName = props.getString(CONTROLLER_NAME);
    kafkaReplicationFactor = props.getInt(KAFKA_REPLICATION_FACTOR, DEFAULT_KAFKA_REPLICATION_FACTOR);
    kafkaReplicationFactorRTTopics = props.getInt(KAFKA_REPLICATION_FACTOR_RT_TOPICS, kafkaReplicationFactor);
    minInSyncReplicas = props.getOptionalInt(KAFKA_MIN_IN_SYNC_REPLICAS);
    minInSyncReplicasRealTimeTopics = props.getOptionalInt(KAFKA_MIN_IN_SYNC_REPLICAS_RT_TOPICS);
    minInSyncReplicasAdminTopics = props.getOptionalInt(KAFKA_MIN_IN_SYNC_REPLICAS_ADMIN_TOPICS);
    kafkaLogCompactionForHybridStores = props.getBoolean(KAFKA_LOG_COMPACTION_FOR_HYBRID_STORES, true);
    kafkaMinLogCompactionLagInMs =
        props.getLong(KAFKA_MIN_LOG_COMPACTION_LAG_MS, DEFAULT_KAFKA_MIN_LOG_COMPACTION_LAG_MS);
    replicationFactor = props.getInt(DEFAULT_REPLICA_FACTOR);
    minNumberOfPartitions = props.getInt(DEFAULT_NUMBER_OF_PARTITION);
    minNumberOfPartitionsForHybrid = props.getInt(DEFAULT_NUMBER_OF_PARTITION_FOR_HYBRID, minNumberOfPartitions);
    kafkaBootstrapServers = props.getString(KAFKA_BOOTSTRAP_SERVERS);
    partitionSize = props.getSizeInBytes(DEFAULT_PARTITION_SIZE);
    maxNumberOfPartitions = props.getInt(DEFAULT_MAX_NUMBER_OF_PARTITIONS);
    partitionCountRoundUpEnabled = props.getBoolean(ENABLE_PARTITION_COUNT_ROUND_UP, false);
    partitionCountRoundUpSize = props.getInt(PARTITION_COUNT_ROUND_UP_SIZE, 1);
    // If the timeout is longer than 3min, we need to update controller client's timeout as well, otherwise creating
    // version would fail.
    offLineJobWaitTimeInMilliseconds = props.getLong(OFFLINE_JOB_START_TIMEOUT_MS, 120000);
    // By default, delayed rebalance is disabled.
    delayToRebalanceMS = props.getLong(DELAY_TO_REBALANCE_MS, 0);
    // By default, the min active replica is replica factor minus one, which means if more than one server failed,
    // helix would trigger re-balance immediately.
    minActiveReplica = props.getInt(MIN_ACTIVE_REPLICA, replicationFactor - 1);
    if (props.containsKey(PERSISTENCE_TYPE)) {
      persistenceType = PersistenceType.valueOf(props.getString(PERSISTENCE_TYPE));
    } else {
      persistenceType = PersistenceType.IN_MEMORY;
    }
    if (props.containsKey(DEFAULT_READ_STRATEGY)) {
      readStrategy = ReadStrategy.valueOf(props.getString(DEFAULT_READ_STRATEGY));
    } else {
      readStrategy = ReadStrategy.ANY_OF_ONLINE;
    }
    if (props.containsKey(DEFAULT_OFFLINE_PUSH_STRATEGY)) {
      offlinePushStrategy = OfflinePushStrategy.valueOf(props.getString(DEFAULT_OFFLINE_PUSH_STRATEGY));
    } else {
      offlinePushStrategy = OfflinePushStrategy.WAIT_ALL_REPLICAS;
    }
    if (props.containsKey(DEFAULT_ROUTING_STRATEGY)) {
      routingStrategy = RoutingStrategy.valueOf(props.getString(DEFAULT_ROUTING_STRATEGY));
    } else {
      routingStrategy = RoutingStrategy.CONSISTENT_HASH;
    }

    nativeReplicationEnabledForBatchOnly = props.getBoolean(ENABLE_NATIVE_REPLICATION_FOR_BATCH_ONLY, false);
    nativeReplicationEnabledAsDefaultForBatchOnly =
        props.getBoolean(ENABLE_NATIVE_REPLICATION_AS_DEFAULT_FOR_BATCH_ONLY, false);
    nativeReplicationEnabledForHybrid = props.getBoolean(ENABLE_NATIVE_REPLICATION_FOR_HYBRID, false);
    nativeReplicationEnabledAsDefaultForHybrid =
        props.getBoolean(ENABLE_NATIVE_REPLICATION_AS_DEFAULT_FOR_HYBRID, false);
    nativeReplicationSourceFabricAsDefaultForBatchOnly =
        props.getString(NATIVE_REPLICATION_SOURCE_FABRIC_AS_DEFAULT_FOR_BATCH_ONLY_STORES, "");
    nativeReplicationSourceFabricAsDefaultForHybrid =
        props.getString(NATIVE_REPLICATION_SOURCE_FABRIC_AS_DEFAULT_FOR_HYBRID_STORES, "");
    activeActiveReplicationEnabledAsDefaultForBatchOnly =
        props.getBoolean(ENABLE_ACTIVE_ACTIVE_REPLICATION_AS_DEFAULT_FOR_BATCH_ONLY_STORE, false);
    activeActiveReplicationEnabledAsDefaultForHybrid =
        props.getBoolean(ENABLE_ACTIVE_ACTIVE_REPLICATION_AS_DEFAULT_FOR_HYBRID_STORE, false);
    controllerSchemaValidationEnabled = props.getBoolean(CONTROLLER_SCHEMA_VALIDATION_ENABLED, true);
    enabledIncrementalPushForHybridActiveActiveUserStores =
        props.getBoolean(ENABLE_INCREMENTAL_PUSH_FOR_HYBRID_ACTIVE_ACTIVE_USER_STORES, false);
    enablePartialUpdateForHybridActiveActiveUserStores =
        props.getBoolean(ENABLE_PARTIAL_UPDATE_FOR_HYBRID_ACTIVE_ACTIVE_USER_STORES, false);
    enablePartialUpdateForHybridNonActiveActiveUserStores =
        props.getBoolean(ENABLE_PARTIAL_UPDATE_FOR_HYBRID_NON_ACTIVE_ACTIVE_USER_STORES, false);

    clusterToD2Map = props.getMap(CLUSTER_TO_D2);
    clusterToServerD2Map = props.getMap(CLUSTER_TO_SERVER_D2, Collections.emptyMap());
    this.sslToKafka = props.getBooleanWithAlternative(KAFKA_OVER_SSL, SSL_TO_KAFKA_LEGACY, false);
    // Enable ssl to kafka
    if (sslToKafka) {
      // In that case , ssl kafka broker list is an mandatory field
      sslKafkaBootStrapServers = props.getString(SSL_KAFKA_BOOTSTRAP_SERVERS);
    }
    helixSendMessageTimeoutMilliseconds = props.getInt(HELIX_SEND_MESSAGE_TIMEOUT_MS, 10000);

    kafkaSecurityProtocol = props.getString(KAFKA_SECURITY_PROTOCOL, SecurityProtocol.PLAINTEXT.name());
    if (!KafkaSSLUtils.isKafkaProtocolValid(kafkaSecurityProtocol)) {
      throw new ConfigurationException("Invalid kafka security protocol: " + kafkaSecurityProtocol);
    }
    if (doesControllerNeedsSslConfig()) {
      sslConfig = Optional.of(new SSLConfig(props));
    } else {
      sslConfig = Optional.empty();
    }
    sslFactoryClassName = props.getString(SSL_FACTORY_CLASS_NAME, DEFAULT_SSL_FACTORY_CLASS_NAME);
    refreshAttemptsForZkReconnect = props.getInt(REFRESH_ATTEMPTS_FOR_ZK_RECONNECT, 3);
    refreshIntervalForZkReconnectInMs =
        props.getLong(REFRESH_INTERVAL_FOR_ZK_RECONNECT_MS, java.util.concurrent.TimeUnit.SECONDS.toMillis(10));
    enableOfflinePushSSLAllowlist = props.getBooleanWithAlternative(
        ENABLE_OFFLINE_PUSH_SSL_ALLOWLIST,
        // go/inclusivecode deferred(Reference will be removed when clients have migrated)
        ENABLE_OFFLINE_PUSH_SSL_WHITELIST,
        true);
    enableNearlinePushSSLAllowlist = props.getBooleanWithAlternative(
        ENABLE_HYBRID_PUSH_SSL_ALLOWLIST,
        // go/inclusivecode deferred(Reference will be removed when clients have migrated)
        ENABLE_HYBRID_PUSH_SSL_WHITELIST,
        true);
    pushSSLAllowlist = props.getListWithAlternative(PUSH_SSL_ALLOWLIST, PUSH_SSL_WHITELIST, new ArrayList<>());
    helixRebalanceAlg = props.getString(HELIX_REBALANCE_ALG, CrushRebalanceStrategy.class.getName());
    adminTopicReplicationFactor = props.getInt(ADMIN_TOPIC_REPLICATION_FACTOR, 3);
    this.pushMonitorType =
        PushMonitorType.valueOf(props.getString(PUSH_MONITOR_TYPE, PushMonitorType.WRITE_COMPUTE_STORE.name()));
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
    this.replicationMetadataVersion = props.getInt(REPLICATION_METADATA_VERSION, 1);
    this.childDatacenters = props.getString(CHILD_CLUSTER_ALLOWLIST);
    this.errorLeaderReplicaFailOverEnabled = props.getBoolean(FORCE_LEADER_ERROR_REPLICA_FAIL_OVER_ENABLED, true);
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

  public VeniceProperties getProps() {
    return props;
  }

  public String getClusterName() {
    return clusterName;
  }

  public final String getZkAddress() {
    return zkAddress;
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

  @Deprecated
  public int getMinActiveReplica() {
    return minActiveReplica;
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

  public PushMonitorType getPushMonitorType() {
    return pushMonitorType;
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

  public long getKafkaMinLogCompactionLagInMs() {
    return kafkaMinLogCompactionLagInMs;
  }

  public boolean isNativeReplicationEnabledForBatchOnly() {
    return nativeReplicationEnabledForBatchOnly;
  }

  public boolean isNativeReplicationEnabledAsDefaultForBatchOnly() {
    return nativeReplicationEnabledAsDefaultForBatchOnly;
  }

  public boolean isNativeReplicationEnabledForHybrid() {
    return nativeReplicationEnabledForHybrid;
  }

  public boolean isNativeReplicationEnabledAsDefaultForHybrid() {
    return nativeReplicationEnabledAsDefaultForHybrid;
  }

  public boolean isActiveActiveReplicationEnabledAsDefaultForBatchOnly() {
    return activeActiveReplicationEnabledAsDefaultForBatchOnly;
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

  public String getChildDatacenters() {
    return childDatacenters;
  }

  public boolean enabledIncrementalPushForHybridActiveActiveUserStores() {
    return enabledIncrementalPushForHybridActiveActiveUserStores;
  }

  public boolean isEnablePartialUpdateForHybridActiveActiveUserStores() {
    return enablePartialUpdateForHybridActiveActiveUserStores;
  }

  public boolean isEnablePartialUpdateForHybridNonActiveActiveUserStores() {
    return enablePartialUpdateForHybridNonActiveActiveUserStores;
  }
}
