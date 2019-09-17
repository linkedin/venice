package com.linkedin.venice.controller;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.linkedin.venice.ConfigConstants.*;
import static com.linkedin.venice.ConfigKeys.*;


/**
 * Configuration which is specific to a Venice controller.
 *
 * It's quite confusing to have both {@link VeniceControllerConfig} and
 * {@link VeniceControllerClusterConfig}. TODO: remove one of them
 */
public class VeniceControllerConfig extends VeniceControllerClusterConfig {

  private final int adminPort;
  private final int controllerClusterReplica;
  private final String controllerClusterName;
  private final String controllerClusterZkAddresss;
  private final int topicMonitorPollIntervalMs;
  private final boolean parent;
  private final boolean enableTopicReplicator;
  private Map<String, String> childClusterMap = null;
  private String d2ServiceName;
  private Map<String, String> childClusterD2Map = null;
  private final int parentControllerWaitingTimeForConsumptionMs;
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
  private int minNumberOfUnusedKafkaTopicsToPreserve;
  private int minNumberOfStoreVersionsToPreserve;
  private final int parentControllerMaxErroredTopicNumToKeep;
  private final String pushJobStatusStoreName;
  private final String pushJobStatusStoreClusterName;
  private final int pushJobStatusValueSchemaId;
  private final boolean addVersionViaAdminProtocol;
  private final boolean addVersionViaTopicMonitor;
  private final boolean participantMessageStoreEnabled;
  private final String systemSchemaClusterName;
  private final int topicDeletionStatusPollIntervalMs;
  private final boolean adminHelixMessagingChannelEnabled;
  private final boolean isControllerClusterLeaderHAAS;

  public VeniceControllerConfig(VeniceProperties props) {
    super(props);
    this.adminPort = props.getInt(ADMIN_PORT);
    this.controllerClusterName = props.getString(CONTROLLER_CLUSTER, "venice-controllers");
    this.controllerClusterReplica = props.getInt(CONTROLLER_CLUSTER_REPLICA, 3);
    this.controllerClusterZkAddresss = props.getString(CONTROLLER_CLUSTER_ZK_ADDRESSS, getZkAddress());
    this.topicMonitorPollIntervalMs = props.getInt(TOPIC_MONITOR_POLL_INTERVAL_MS, 10 * Time.MS_PER_SECOND); // By default, time window used to throttle topic creation is 10sec.
    this.topicCreationThrottlingTimeWindowMs = props.getLong(TOPIC_CREATION_THROTTLING_TIME_WINDOW_MS, 10 * Time.MS_PER_SECOND);
    this.parent = props.getBoolean(ConfigKeys.CONTROLLER_PARENT_MODE, false);
    if (this.parent) {
      String clusterWhitelist = props.getString(CHILD_CLUSTER_WHITELIST);
      this.childClusterMap = parseClusterMap(props, clusterWhitelist);
      this.childClusterD2Map = parseClusterMap(props, clusterWhitelist, true);
      if (!childClusterD2Map.isEmpty()) {
        this.d2ServiceName = props.getString(CHILD_CLUSTER_D2_SERVICE_NAME);
      }

      if (childClusterMap.isEmpty() && childClusterD2Map.isEmpty()) {
        throw new VeniceException("child controller list can not be empty");
      }
    }
    this.parentControllerWaitingTimeForConsumptionMs = props.getInt(ConfigKeys.PARENT_CONTROLLER_WAITING_TIME_FOR_CONSUMPTION_MS, 30 * Time.MS_PER_SECOND);
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

    this.pushJobStatusStoreName = props.getString(PUSH_JOB_STATUS_STORE_NAME, "");
    this.pushJobStatusStoreClusterName = props.getString(PUSH_JOB_STATUS_STORE_CLUSTER_NAME, "");
    this.pushJobStatusValueSchemaId = props.getInt(PUSH_JOB_STATUS_VALUE_SCHEMA_ID, 1);
    this.addVersionViaAdminProtocol = props.getBoolean(CONTROLLER_ADD_VERSION_VIA_ADMIN_PROTOCOL, false);
    this.addVersionViaTopicMonitor = props.getBoolean(CONTROLLER_ADD_VERSION_VIA_TOPIC_MONITOR, true);
    this.participantMessageStoreEnabled = props.getBoolean(PARTICIPANT_MESSAGE_STORE_ENABLED, false);
    this.adminHelixMessagingChannelEnabled = props.getBoolean(ADMIN_HELIX_MESSAGING_CHANNEL_ENABLED, true);
    if (!adminHelixMessagingChannelEnabled && !participantMessageStoreEnabled) {
      throw new VeniceException("Cannot perform kill push job if both " + ADMIN_HELIX_MESSAGING_CHANNEL_ENABLED
          + " and " + PARTICIPANT_MESSAGE_STORE_ENABLED + " are set to false");
    }
    this.systemSchemaClusterName = props.getString(CONTROLLER_SYSTEM_SCHEMA_CLUSTER_NAME, "");
    this.topicDeletionStatusPollIntervalMs = props.getInt(TOPIC_DELETION_STATUS_POLL_INTERVAL_MS, DEFAULT_TOPIC_DELETION_STATUS_POLL_INTERVAL_MS); // 2s
    this.isControllerClusterLeaderHAAS = props.getBoolean(CONTROLLER_CLUSTER_LEADER_HAAS, false);
  }

  public int getAdminPort() {
    return adminPort;
  }

  public int getControllerClusterReplica() {
    return controllerClusterReplica;
  }

  public String getControllerClusterName() {
    return controllerClusterName;
  }

  public String getControllerClusterZkAddresss() {
    return controllerClusterZkAddresss;
  }

  public int getTopicMonitorPollIntervalMs() { return topicMonitorPollIntervalMs; }

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
  public Map<String, String> getChildClusterMap(){
    return childClusterMap;
  }

  public String getD2ServiceName() {
    return d2ServiceName;
  }

  public Map<String, String> getChildClusterD2Map() {
    return childClusterD2Map;
  }

  public int getParentControllerWaitingTimeForConsumptionMs() {
    return parentControllerWaitingTimeForConsumptionMs;
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

  public String getPushJobStatusStoreName() { return pushJobStatusStoreName; }

  public String getPushJobStatusStoreClusterName() { return pushJobStatusStoreClusterName; }

  public int getPushJobStatusValueSchemaId() {return pushJobStatusValueSchemaId; }

  public boolean isAddVersionViaAdminProtocolEnabled() { return addVersionViaAdminProtocol; }

  public boolean isAddVersionViaTopicMonitorEnabled() { return addVersionViaTopicMonitor; }

  public boolean isParticipantMessageStoreEnabled() { return participantMessageStoreEnabled; }

  public String getSystemSchemaClusterName() { return systemSchemaClusterName; }

  public int getTopicDeletionStatusPollIntervalMs() {
    return topicDeletionStatusPollIntervalMs;
  }

  public boolean isAdminHelixMessagingChannelEnabled() { return  adminHelixMessagingChannelEnabled; }

  public boolean isControllerClusterLeaderHAAS() { return isControllerClusterLeaderHAAS; }

  /**
   * @param clusterPros list of child controller uris
   * @param datacenterWhitelist data centers that are taken into account
   * @param D2Routing whether uses D2 to route or not
   * @return
   */
  public static Map<String, String> parseClusterMap(VeniceProperties clusterPros, String datacenterWhitelist, Boolean D2Routing) {
    Properties childClusterUriProps;
    String propsPrefix =  D2Routing ? CHILD_CLUSTER_D2_PREFIX : CHILD_CLUSTER_URL_PREFIX;

    childClusterUriProps = clusterPros.clipAndFilterNamespace(propsPrefix).toProperties();

    if (Utils.isNullOrEmpty(datacenterWhitelist)) {
      throw new VeniceException("child controller list must have a whitelist");
    }

    Map<String, String> outputMap = new HashMap<>();
    List<String> whitelist = Arrays.asList(datacenterWhitelist.split(",\\s*"));

    for (Map.Entry<Object, Object> uriEntry : childClusterUriProps.entrySet()) {
      String datacenter = (String) uriEntry.getKey();
      String uris = (String) uriEntry.getValue();

      String errmsg = "Invalid configuration " + propsPrefix + "." + datacenter;
      if (datacenter.isEmpty()) {
        throw new VeniceException(errmsg + ": cluster name can't be empty. " + uris);
      }

      if (uris.isEmpty()) {
        throw new VeniceException(errmsg + ": found no urls for: " + datacenter);
      }

      if (whitelist.contains(datacenter)) {
        outputMap.computeIfAbsent(datacenter, k -> {
          String[] uriList = uris.split(",\\s*");

          if (D2Routing && uriList.length != 1) {
            throw new VeniceException(errmsg + ": can only have 1 zookeeper url");
          }

          if (!D2Routing) {
            if (uriList.length == 0) {
              throw new VeniceException(errmsg + ": urls can not be empty");
            }

            if (Arrays.stream(uriList).anyMatch(uri -> !uri.startsWith("http://"))) {
              throw new VeniceException(errmsg + ": urls must begin with http://");
            }
          }

          return uris;
        });
      }
    }

    return outputMap;
  }
}
