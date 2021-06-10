package com.linkedin.venice.controller;

import com.linkedin.venice.SSLConfig;
import com.linkedin.venice.controllerapi.ControllerRoute;
import com.linkedin.venice.exceptions.VeniceNoClusterException;
import com.linkedin.venice.utils.VeniceProperties;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;


public class VeniceControllerMultiClusterConfig {

  private final Map<String, VeniceControllerConfig> clusterToConfigMap;

  public VeniceControllerMultiClusterConfig(Collection<VeniceProperties> propertiesCollection) {
    clusterToConfigMap = new HashMap<>(propertiesCollection.size());
    for (VeniceProperties properties : propertiesCollection) {
      VeniceControllerConfig controllerConfig = new VeniceControllerConfig(properties);
      clusterToConfigMap.put(controllerConfig.getClusterName(), controllerConfig);
    }
  }

  public VeniceControllerMultiClusterConfig(Map<String, VeniceControllerConfig> configMap){
    clusterToConfigMap = new HashMap<>(configMap);
  }

  public void addClusterConfig(VeniceControllerConfig clusterConfig) {
    clusterToConfigMap.put(clusterConfig.getClusterName(), clusterConfig);
  }

  public VeniceControllerConfig getConfigForCluster(String clusterName) {
    if (clusterToConfigMap.containsKey(clusterName)) {
      return clusterToConfigMap.get(clusterName);
    } else {
      throw new VeniceNoClusterException(clusterName);
    }
  }

  public int getAdminPort() {
    return getCommonConfig().getAdminPort();
  }

  public int getAdminSecurePort() {
    return getCommonConfig().getAdminSecurePort();
  }

  public boolean adminCheckReadMethodForKafka() {
    return getCommonConfig().adminCheckReadMethodForKafka();
  }

  public boolean isParent() {
    return getCommonConfig().isParent();
  }

  public String getControllerName() {
    return getCommonConfig().getControllerName();
  }

  public String getZkAddress() {
    return getCommonConfig().getZkAddress();
  }

  public String getControllerClusterName() {
    return getCommonConfig().getControllerClusterName();
  }

  public String getControllerHAASSuperClusterName() {
    return getCommonConfig().getControllerHAASSuperClusterName();
  }

  public int getControllerClusterReplica() {
    return getCommonConfig().getControllerClusterReplica();
  }

  public String getKafkaBootstrapServers() {
    return getCommonConfig().getKafkaBootstrapServers();
  }

  public String getSslKafkaBootstrapServers() {
    return getCommonConfig().getSslKafkaBootStrapServers();
  }

  public boolean isSslToKafka() {
    return getCommonConfig().isSslToKafka();
  }

  public Optional<SSLConfig> getSslConfig() {
    return getCommonConfig().getSslConfig();
  }

  public String getSslFactoryClassName() {
    return getCommonConfig().getSslFactoryClassName();
  }

  public String getKafkaZkAddress() {
    return getCommonConfig().getKafkaZkAddress();
  }

  public long getDeprecatedJobTopicRetentionMs() {
    return getCommonConfig().getDeprecatedJobTopicRetentionMs();
  }

  public long getDeprecatedJobTopicMaxRetentionMs() {
    return getCommonConfig().getDeprecatedJobTopicMaxRetentionMs();
  }

  public long getTopicCleanupSleepIntervalBetweenTopicListFetchMs() {
    return getCommonConfig().getTopicCleanupSleepIntervalBetweenTopicListFetchMs();
  }

  public int getTopicCleanupDelayFactor() {
    return getCommonConfig().getTopicCleanupDelayFactor();
  }

  public String getControllerClusterZkAddress() {
    return getCommonConfig().getControllerClusterZkAddress();
  }

  public int getParentControllerWaitingTimeForConsumptionMs() {
    return getCommonConfig().getParentControllerWaitingTimeForConsumptionMs();
  }

  public boolean getBatchJobHeartbeatEnabled() {
    return getCommonConfig().getBatchJobHeartbeatEnabled();
  }

  public Duration getBatchJobHeartbeatTimeout() {
    return getCommonConfig().getBatchJobHeartbeatTimeout();
  }

  public Duration getBatchJobHeartbeatInitialBufferTime() {
    return getCommonConfig().getBatchJobHeartbeatInitialBufferTime();
  }

  public int getKafkaReplicaFactor() {
    return getCommonConfig().getKafkaReplicationFactor();
  }

  public long getTopicCreationThrottlingTimeWindowMs() {
    return getCommonConfig().getTopicCreationThrottlingTimeWindowMs();
  }

  public Map<String,String> getClusterToD2Map() {
    return getCommonConfig().getClusterToD2Map();
  }

  public int getTopicManagerKafkaOperationTimeOutMs() {
    return getCommonConfig().getTopicManagerKafkaOperationTimeOutMs();
  }

  public int getTopicDeletionStatusPollIntervalMs() {
    return getCommonConfig().getTopicDeletionStatusPollIntervalMs();
  }

  public boolean isConcurrentTopicDeleteRequestsEnabled() {
    return getCommonConfig().isConcurrentTopicDeleteRequestsEnabled();
  }

  public long getKafkaMinLogCompactionLagInMs() {
    return getCommonConfig().getKafkaMinLogCompactionLagInMs();
  }

  public int getMinNumberOfUnusedKafkaTopicsToPreserve() {
    return getCommonConfig().getMinNumberOfUnusedKafkaTopicsToPreserve();
  }

  public int getMinNumberOfStoreVersionsToPreserve() {
    return getCommonConfig().getMinNumberOfStoreVersionsToPreserve();
  }

  public int getParentControllerMaxErroredTopicNumToKeep() {
    return getCommonConfig().getParentControllerMaxErroredTopicNumToKeep();
  }

  public VeniceControllerConfig getCommonConfig() {
    return clusterToConfigMap.values().iterator().next();
  }

  public List<ControllerRoute> getDisabledRoutes() {
    return getCommonConfig().getDisabledRoutes();
  }

  public Set<String> getClusters() {
    return clusterToConfigMap.keySet();
  }

  public String getPushJobStatusStoreClusterName() {
    return getCommonConfig().getPushJobStatusStoreClusterName();
  }

  public String getSystemSchemaClusterName() {
    return getCommonConfig().getSystemSchemaClusterName();
  }

  public boolean isEnableBatchPushFromAdminInChildController() {
    return getCommonConfig().isEnableBatchPushFromAdminInChildController();
  }

  public long getBackupVersionDefaultRetentionMs() {
    return getCommonConfig().getBackupVersionDefaultRetentionMs();
  }

  public boolean isBackupVersionRetentionBasedCleanupEnabled() {
    return getCommonConfig().isBackupVersionRetentionBasedCleanupEnabled();
  }

  public boolean isControllerEnforceSSLOnly() {
    return getCommonConfig().isControllerEnforceSSLOnly();
  }

  public long getTerminalStateTopicCheckerDelayMs() {
    return getCommonConfig().getTerminalStateTopicCheckerDelayMs();
  }

  public Map<String, String> getChildDataCenterKafkaUrlMap() {
    return getCommonConfig().getChildDataCenterKafkaUrlMap();
  }

  public Map<String, String> getChildDataCenterKafkaZkMap() {
    return getCommonConfig().getChildDataCenterKafkaZkMap();
  }

  public Set<String> getParentFabrics() {
    return getCommonConfig().getParentFabrics();
  }

  public boolean isZkSharedMetadataSystemSchemaStoreAutoCreationEnabled() {
    return getCommonConfig().isZkSharedMetadataSystemSchemaStoreAutoCreationEnabled();
  }

  public boolean isZkSharedDaVinciPushStatusSystemSchemaStoreAutoCreationEnabled() {
    return getCommonConfig().isZkSharedDaVinciPushStatusSystemSchemaStoreAutoCreationEnabled();
  }

  public long getSystemStoreAclSynchronizationDelayMs() {
    return getCommonConfig().getSystemStoreAclSynchronizationDelayMs();
  }

  public String getRegionName() {
    return getCommonConfig().getRegionName();
  }
}
