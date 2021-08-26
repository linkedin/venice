package com.linkedin.venice.controller;

import com.linkedin.venice.SSLConfig;
import com.linkedin.venice.exceptions.ConfigurationException;
import com.linkedin.venice.exceptions.UndefinedPropertyException;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.helix.controller.rebalancer.strategy.CrushRebalanceStrategy;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.log4j.Logger;

import static com.linkedin.venice.CommonConfigKeys.*;
import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.VeniceConstants.*;
import static com.linkedin.venice.kafka.TopicManager.*;


/**
 * Configuration which is specific to a Venice cluster used by Venice controller.
 */
public class VeniceControllerClusterConfig {
  private static final Logger logger = Logger.getLogger(VeniceControllerClusterConfig.class);

  private final VeniceProperties props;
  private String clusterName;
  private String zkAddress;
  private String controllerName;
  private PersistenceType persistenceType;
  private ReadStrategy readStrategy;
  private OfflinePushStrategy offlinePushStrategy;
  private RoutingStrategy routingStrategy;
  private int replicationFactor;
  private int numberOfPartition;
  private int maxNumberOfPartition;
  private long partitionSize;
  private long offLineJobWaitTimeInMilliseconds;
  private Map<String, String> clusterToD2Map;
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
  private boolean enableOfflinePushSSLWhitelist;
  private boolean enableNearlinePushSSLWhitelist;
  private List<String> pushSSLWhitelist;

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
   * When this option is enabled, all new incremental push enabled store versions created will have native replication
   * enabled so long as the store has leader follower also enabled.
   */
  private boolean nativeReplicationEnabledForIncremental;

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
   * When this option is enabled, all new incremental push enabled stores will have native replication enabled in store
   * config so long as the store has leader follower also enabled.
   */
  private boolean nativeReplicationEnabledAsDefaultForIncremental;

  /**
   * When this option is enabled, all new hybrid stores will have native replication enabled in store config so long
   * as the store has leader follower also enabled.
   */
  private boolean nativeReplicationEnabledAsDefaultForHybrid;

  private String nativeReplicationSourceFabricAsDefaultForBatchOnly;
  private String nativeReplicationSourceFabricAsDefaultForHybrid;
  private String nativeReplicationSourceFabricAsDefaultForIncremental;

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
   * When this option is enabled, all new incremental push enabled stores will have active-active replication enabled in store
   * config so long as the store has leader follower also enabled.
   */
  private boolean activeActiveReplicationEnabledAsDefaultForIncremental;

  /**
   * When this option is set to true, we will not check if leader follower mode has been enabled at cluster level or store level.
   * Just enabling native replication should be good enough to turn a cluster or store NR enabled. This should be set to false
   * at child controller and true in parent controller.
   */
  private boolean lfModelDependencyCheckDisabled;

  /**
   * When this option is enabled, all new hybrid stores will have leader follower enabled.
   */
  private boolean leaderFollowerEnabledForHybridStores;

  /**
   * When this option is enabled, all new incremental push stores will have leader follower enabled.
   */
  private boolean leaderFollowerEnabledForIncrementalPushStores;

  /**
   * When this option is enabled, all new batch-only stores will have leader/follower state model enabled.
   */
  private boolean leaderFollowerEnabledForBatchOnlyStores;

  /**
   * When this option is enabled, all new stores will have leader follower enabled.
   */
  private boolean leaderFollowerEnabledForAllStores;

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
  private Optional<Integer> minIsr;
  private boolean kafkaLogCompactionForHybridStores;
  private boolean kafkaLogCompactionForIncrementalPushStores;
  private long kafkaMinLogCompactionLagInMs;

  /**
   * Address of zookeeper that kafka used. It may be different from what Helix used.
   */
  private String kafkaZkAddress;

  /**
   * Alg used by helix to decide the mapping between replicas and nodes.
   */
  private String helixRebalanceAlg;

  /**
   * Sleep interval inside {@link LeakedPushStatusCleanUpService}
   */
  private long leakedPushStatusCleanUpServiceSleepIntervalInMs;

  /**
   * Jetty config overrides for Spark server
   */
  private VeniceProperties jettyConfigOverrides;

  public VeniceControllerClusterConfig(VeniceProperties props) {
    try {
      this.props = props;
      initFieldsWithProperties(props);
      logger.info("Loaded configuration");
    } catch (Exception e) {
      String errorMessage = "Can not load properties.";
      logger.error(errorMessage);
      throw new VeniceException(errorMessage, e);
    }
  }

  private void initFieldsWithProperties(VeniceProperties props) {
    clusterName = props.getString(CLUSTER_NAME);
    zkAddress = props.getString(ZOOKEEPER_ADDRESS);
    controllerName = props.getString(CONTROLLER_NAME);
    kafkaZkAddress = props.getString(KAFKA_ZK_ADDRESS);
    try {
      kafkaReplicationFactor = props.getInt(KAFKA_REPLICATION_FACTOR);
    } catch (UndefinedPropertyException e) {
      kafkaReplicationFactor = props.getInt(KAFKA_REPLICATION_FACTOR_LEGACY_SPELLING);
    }
    minIsr = props.getOptionalInt(KAFKA_MIN_ISR);
    kafkaLogCompactionForHybridStores = props.getBoolean(KAFKA_LOG_COMPACTION_FOR_HYBRID_STORES, true);
    kafkaLogCompactionForIncrementalPushStores = props.getBoolean(KAFKA_LOG_COMPACTION_FOR_INCREMENTAL_PUSH_STORES, true);
    kafkaMinLogCompactionLagInMs = props.getLong(KAFKA_MIN_LOG_COMPACTION_LAG_MS, DEFAULT_KAFKA_MIN_LOG_COMPACTION_LAG_MS);
    replicationFactor = props.getInt(DEFAULT_REPLICA_FACTOR);
    numberOfPartition = props.getInt(DEFAULT_NUMBER_OF_PARTITION);
    kafkaBootstrapServers = props.getString(KAFKA_BOOTSTRAP_SERVERS);
    partitionSize = props.getSizeInBytes(DEFAULT_PARTITION_SIZE);
    maxNumberOfPartition = props.getInt(DEFAULT_MAX_NUMBER_OF_PARTITIONS);
    // If the timeout is longer than 3min, we need to update controller client's timeout as well, otherwise creating version would fail.
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
    nativeReplicationEnabledAsDefaultForBatchOnly = props.getBoolean(ENABLE_NATIVE_REPLICATION_AS_DEFAULT_FOR_BATCH_ONLY, false);
    nativeReplicationEnabledForIncremental = props.getBoolean(ENABLE_NATIVE_REPLICATION_FOR_INCREMENTAL_PUSH, false);
    nativeReplicationEnabledAsDefaultForIncremental = props.getBoolean(ENABLE_NATIVE_REPLICATION_AS_DEFAULT_FOR_INCREMENTAL_PUSH, false);
    nativeReplicationEnabledForHybrid = props.getBoolean(ENABLE_NATIVE_REPLICATION_FOR_HYBRID, false);
    nativeReplicationEnabledAsDefaultForHybrid = props.getBoolean(ENABLE_NATIVE_REPLICATION_AS_DEFAULT_FOR_HYBRID, false);
    nativeReplicationSourceFabricAsDefaultForBatchOnly = props.getString(NATIVE_REPLICATION_SOURCE_FABRIC_AS_DEFAULT_FOR_BATCH_ONLY_STORES, "");
    nativeReplicationSourceFabricAsDefaultForHybrid = props.getString(NATIVE_REPLICATION_SOURCE_FABRIC_AS_DEFAULT_FOR_HYBRID_STORES, "");
    nativeReplicationSourceFabricAsDefaultForIncremental = props.getString(NATIVE_REPLICATION_SOURCE_FABRIC_AS_DEFAULT_FOR_INCREMENTAL_PUSH_STORES, "");
    activeActiveReplicationEnabledAsDefaultForBatchOnly = props.getBoolean(ENABLE_ACTIVE_ACTIVE_REPLICATION_AS_DEFAULT_FOR_BATCH_ONLY_STORE, false);
    activeActiveReplicationEnabledAsDefaultForHybrid = props.getBoolean(ENABLE_ACTIVE_ACTIVE_REPLICATION_AS_DEFAULT_FOR_HYBRID_STORE, false);
    activeActiveReplicationEnabledAsDefaultForIncremental = props.getBoolean(ENABLE_ACTIVE_ACTIVE_REPLICATION_AS_DEFAULT_FOR_INCREMENTAL_PUSH_STORE, false);
    leaderFollowerEnabledForHybridStores = props.getBoolean(ENABLE_LEADER_FOLLOWER_AS_DEFAULT_FOR_HYBRID_STORES, false);
    leaderFollowerEnabledForIncrementalPushStores =
        props.getBoolean(ENABLE_LEADER_FOLLOWER_AS_DEFAULT_FOR_INCREMENTAL_PUSH_STORES, false);
    leaderFollowerEnabledForBatchOnlyStores = props.getBoolean(ENABLE_LEADER_FOLLOWER_AS_DEFAULT_FOR_BATCH_ONLY_STORES, false);
    leaderFollowerEnabledForAllStores = props.getBoolean(ENABLE_LEADER_FOLLOWER_AS_DEFAULT_FOR_ALL_STORES, false);
    controllerSchemaValidationEnabled = props.getBoolean(CONTROLLER_SCHEMA_VALIDATION_ENABLED, true);
    lfModelDependencyCheckDisabled = props.getBoolean(LF_MODEL_DEPENDENCY_CHECK_DISABLED, false);

    if (!leaderFollowerEnabledForAllStores && !lfModelDependencyCheckDisabled
        && (nativeReplicationEnabledAsDefaultForBatchOnly || nativeReplicationEnabledAsDefaultForIncremental || nativeReplicationEnabledAsDefaultForHybrid)) {
      logger.error("Cannot enable native replication when leader follower is not enabled for all stores. Will revert "
          + "the cluster-level native replication flags to false");
      nativeReplicationEnabledForBatchOnly = false;
      nativeReplicationEnabledAsDefaultForBatchOnly = false;
      nativeReplicationEnabledForIncremental = false;
      nativeReplicationEnabledAsDefaultForIncremental = false;
      nativeReplicationEnabledForHybrid = false;
      nativeReplicationEnabledAsDefaultForHybrid = false;
    }

    if (!leaderFollowerEnabledForAllStores && !lfModelDependencyCheckDisabled
        && (activeActiveReplicationEnabledAsDefaultForHybrid || activeActiveReplicationEnabledAsDefaultForIncremental || activeActiveReplicationEnabledAsDefaultForBatchOnly)) {
      logger.error("Cannot enable active-active replication when leader follower is not enabled for all stores. Will revert "
          + "the cluster-level active-active replication flags to false");
      activeActiveReplicationEnabledAsDefaultForHybrid = false;
      activeActiveReplicationEnabledAsDefaultForIncremental = false;
      activeActiveReplicationEnabledAsDefaultForBatchOnly = false;
    }

    clusterToD2Map = props.getMap(CLUSTER_TO_D2);
    this.sslToKafka = props.getBoolean(SSL_TO_KAFKA, false);
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
    if (KafkaSSLUtils.isKafkaSSLProtocol(kafkaSecurityProtocol)) {
      sslConfig = Optional.of(new SSLConfig(props));
    } else {
      sslConfig = Optional.empty();
    }
    sslFactoryClassName = props.getString(SSL_FACTORY_CLASS_NAME, DEFAULT_SSL_FACTORY_CLASS_NAME);
    refreshAttemptsForZkReconnect = props.getInt(REFRESH_ATTEMPTS_FOR_ZK_RECONNECT, 3);
    refreshIntervalForZkReconnectInMs =
        props.getLong(REFRESH_INTERVAL_FOR_ZK_RECONNECT_MS, java.util.concurrent.TimeUnit.SECONDS.toMillis(10));
    enableOfflinePushSSLWhitelist = props.getBoolean(ENABLE_OFFLINE_PUSH_SSL_WHITELIST, true);
    enableNearlinePushSSLWhitelist = props.getBoolean(ENABLE_HYBRID_PUSH_SSL_WHITELIST, true);
    pushSSLWhitelist = props.getList(PUSH_SSL_WHITELIST, new ArrayList<>());
    helixRebalanceAlg = props.getString(HELIX_REBALANCE_ALG, CrushRebalanceStrategy.class.getName());
    adminTopicReplicationFactor = props.getInt(ADMIN_TOPIC_REPLICATION_FACTOR, 3);
    this.pushMonitorType = PushMonitorType
        .valueOf(props.getString(PUSH_MONITOR_TYPE, PushMonitorType.WRITE_COMPUTE_STORE.name()));
    if (adminTopicReplicationFactor < 1) {
      throw new ConfigurationException(ADMIN_TOPIC_REPLICATION_FACTOR + " cannot be less than 1.");
    }
    this.leakedPushStatusCleanUpServiceSleepIntervalInMs = props.getLong(LEAKED_PUSH_STATUS_CLEAN_UP_SERVICE_SLEEP_INTERVAL_MS, TimeUnit.MINUTES.toMillis(15));
    this.jettyConfigOverrides = props.clipAndFilterNamespace(CONTROLLER_JETTY_CONFIG_OVERRIDE_PREFIX);
  }

  public VeniceProperties getProps() {
    return props;
  }

  public String getClusterName() {
    return clusterName;
  }

  public String getZkAddress() {
    return zkAddress;
  }

  public String getControllerName() {
    return controllerName;
  }

  public String getKafkaZkAddress() {
    return kafkaZkAddress;
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

  public int getNumberOfPartition() {
    return numberOfPartition;
  }

  public int getKafkaReplicationFactor() {
    return kafkaReplicationFactor;
  }

  public long getPartitionSize() {
    return partitionSize;
  }

  public int getMaxNumberOfPartition() {
    return maxNumberOfPartition;
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
   * returns kafka Bootstrap Urls . IF there is more than one url, they are separated by commas.
   */
  public String getKafkaBootstrapServers() { return kafkaBootstrapServers; }

  public Map<String, String> getClusterToD2Map() {
    return clusterToD2Map;
  }

  public boolean isSslToKafka() {
    return sslToKafka;
  }

  public String getSslKafkaBootStrapServers() {
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

  public boolean isEnableOfflinePushSSLWhitelist() {
    return enableOfflinePushSSLWhitelist;
  }

  public List<String> getPushSSLWhitelist() {
    return pushSSLWhitelist;
  }

  public boolean isEnableNearlinePushSSLWhitelist() {
    return enableNearlinePushSSLWhitelist;
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

  public Optional<Integer> getMinIsr() {
    return minIsr;
  }

  public boolean isKafkaLogCompactionForHybridStoresEnabled() {
    return kafkaLogCompactionForHybridStores;
  }

  public boolean isKafkaLogCompactionForIncrementalPushStoresEnabled() {
    return kafkaLogCompactionForIncrementalPushStores;
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

  public boolean isNativeReplicationEnabledForIncremental() {
    return nativeReplicationEnabledForIncremental;
  }

  public boolean isNativeReplicationEnabledAsDefaultForIncremental() {
    return nativeReplicationEnabledAsDefaultForIncremental;
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

  public boolean isActiveActiveReplicationEnabledAsDefaultForIncremental() {
    return activeActiveReplicationEnabledAsDefaultForIncremental;
  }

  public boolean isLfModelDependencyCheckDisabled() {
    return lfModelDependencyCheckDisabled;
  }

  public boolean isLeaderFollowerEnabledForHybridStores() {
    return leaderFollowerEnabledForHybridStores || leaderFollowerEnabledForAllStores;
  }

  public boolean isLeaderFollowerEnabledForIncrementalPushStores() {
    return leaderFollowerEnabledForIncrementalPushStores || leaderFollowerEnabledForAllStores;
  }

  public boolean isLeaderFollowerEnabledForBatchOnlyStores() {
    return leaderFollowerEnabledForBatchOnlyStores || leaderFollowerEnabledForAllStores;
  }

  public boolean isLeaderFollowerEnabledForAllStores() {
    return leaderFollowerEnabledForAllStores;
  }

  public boolean isControllerSchemaValidationEnabled() {
    return controllerSchemaValidationEnabled;
  }

  public long getLeakedPushStatusCleanUpServiceSleepIntervalInMs() {
    return leakedPushStatusCleanUpServiceSleepIntervalInMs;
  }

  public String getNativeReplicationSourceFabricAsDefaultForBatchOnly() {
    return nativeReplicationSourceFabricAsDefaultForBatchOnly;
  }

  public String getNativeReplicationSourceFabricAsDefaultForHybrid() {
    return nativeReplicationSourceFabricAsDefaultForHybrid;
  }

  public String getNativeReplicationSourceFabricAsDefaultForIncremental() {
    return nativeReplicationSourceFabricAsDefaultForIncremental;
  }

  public VeniceProperties getJettyConfigOverrides() {
    return jettyConfigOverrides;
  }
}
