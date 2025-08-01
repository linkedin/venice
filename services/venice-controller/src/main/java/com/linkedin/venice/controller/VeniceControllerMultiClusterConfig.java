package com.linkedin.venice.controller;

import com.linkedin.venice.SSLConfig;
import com.linkedin.venice.controllerapi.ControllerRoute;
import com.linkedin.venice.exceptions.VeniceNoClusterException;
import com.linkedin.venice.pubsub.PubSubAdminAdapterFactory;
import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.PubSubPositionTypeRegistry;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.utils.LogContext;
import com.linkedin.venice.utils.VeniceProperties;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;


public class VeniceControllerMultiClusterConfig {
  private final Map<String, VeniceControllerClusterConfig> clusterToControllerConfigMap;
  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  public VeniceControllerMultiClusterConfig(Collection<VeniceProperties> controllerClusterProperties) {
    clusterToControllerConfigMap = new HashMap<>();
    for (VeniceProperties properties: controllerClusterProperties) {
      final VeniceControllerClusterConfig controllerConfig = new VeniceControllerClusterConfig(properties);
      clusterToControllerConfigMap.put(controllerConfig.getClusterName(), controllerConfig);
    }
  }

  // This constructor is used for testing.
  public VeniceControllerMultiClusterConfig(Map<String, VeniceControllerClusterConfig> clusterToControllerConfigMap) {
    this.clusterToControllerConfigMap = new HashMap<>(clusterToControllerConfigMap);
  }

  public void addClusterConfig(VeniceControllerClusterConfig controllerConfig) {
    clusterToControllerConfigMap.put(controllerConfig.getClusterName(), controllerConfig);
  }

  public VeniceControllerClusterConfig getControllerConfig(String clusterName) {
    if (clusterToControllerConfigMap.containsKey(clusterName)) {
      return clusterToControllerConfigMap.get(clusterName);
    } else {
      throw new VeniceNoClusterException(clusterName);
    }
  }

  public int getAdminPort() {
    return getCommonConfig().getAdminPort();
  }

  public String getAdminHostname() {
    return getCommonConfig().getAdminHostname();
  }

  public int getAdminSecurePort() {
    return getCommonConfig().getAdminSecurePort();
  }

  public int getAdminGrpcPort() {
    return getCommonConfig().getAdminGrpcPort();
  }

  public int getAdminSecureGrpcPort() {
    return getCommonConfig().getAdminSecureGrpcPort();
  }

  public boolean adminCheckReadMethodForKafka() {
    return getCommonConfig().adminCheckReadMethodForKafka();
  }

  public boolean isMultiRegion() {
    return getCommonConfig().isMultiRegion();
  }

  public boolean isParent() {
    return getCommonConfig().isParent();
  }

  public ParentControllerRegionState getParentControllerRegionState() {
    return getCommonConfig().getParentControllerRegionState();
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
    return getCommonConfig().getSslKafkaBootstrapServers();
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

  public long getDeprecatedJobTopicRetentionMs() {
    return getCommonConfig().getDeprecatedJobTopicRetentionMs();
  }

  public long getFatalDataValidationFailureRetentionMs() {
    return getCommonConfig().getFatalDataValidationFailureRetentionMs();
  }

  public long getDeprecatedJobTopicMaxRetentionMs() {
    return getCommonConfig().getDeprecatedJobTopicMaxRetentionMs();
  }

  public long getTopicCleanupSleepIntervalBetweenTopicListFetchMs() {
    return getCommonConfig().getTopicCleanupSleepIntervalBetweenTopicListFetchMs();
  }

  public long getDisabledReplicaEnablerServiceIntervalMs() {
    return getCommonConfig().getDisabledReplicaEnablerServiceIntervalMs();
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

  public String getBatchJobHeartbeatStoreCluster() {
    return getCommonConfig().getBatchJobHeartbeatStoreCluster();
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

  public Map<String, String> getClusterToD2Map() {
    return getCommonConfig().getClusterToD2Map();
  }

  public Map<String, String> getClusterToServerD2Map() {
    return getCommonConfig().getClusterToServerD2Map();
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

  public VeniceControllerClusterConfig getCommonConfig() {
    return clusterToControllerConfigMap.values().iterator().next();
  }

  public List<ControllerRoute> getDisabledRoutes() {
    return getCommonConfig().getDisabledRoutes();
  }

  public int getUnusedSchemaCleanupIntervalSeconds() {
    return getCommonConfig().getUnusedSchemaCleanupIntervalSeconds();
  }

  public int getMinSchemaCountToKeep() {
    return getCommonConfig().getMinSchemaCountToKeep();
  }

  public Set<String> getClusters() {
    return clusterToControllerConfigMap.keySet();
  }

  public String getPushJobStatusStoreClusterName() {
    return getCommonConfig().getPushJobStatusStoreClusterName();
  }

  public String getSystemSchemaClusterName() {
    return getCommonConfig().getSystemSchemaClusterName();
  }

  public long getBackupVersionDefaultRetentionMs() {
    return getCommonConfig().getBackupVersionDefaultRetentionMs();
  }

  public long getBackupVersionCleanupSleepMs() {
    return getCommonConfig().getBackupVersionCleanupSleepMs();
  }

  public long getDeferredVersionSwapSleepMs() {
    return getCommonConfig().getDeferredVersionSwapSleepMs();
  }

  public double getDeferredVersionSwapBufferTime() {
    return getCommonConfig().getDeferredVersionSwapBufferTime();
  }

  public boolean isDeferredVersionSwapServiceEnabled() {
    return getCommonConfig().isDeferredVersionSwapServiceEnabled();
  }

  public boolean isSkipDeferredVersionSwapForDVCEnabled() {
    return getCommonConfig().isSkipDeferredVersionSwapForDVCEnabled();
  }

  public boolean isControllerEnforceSSLOnly() {
    return getCommonConfig().isControllerEnforceSSLOnly();
  }

  public boolean isGrpcServerEnabled() {
    return getCommonConfig().isGrpcServerEnabled();
  }

  public int getGrpcServerThreadCount() {
    return getCommonConfig().getGrpcServerThreadCount();
  }

  public long getTerminalStateTopicCheckerDelayMs() {
    return getCommonConfig().getTerminalStateTopicCheckerDelayMs();
  }

  public Map<String, String> getChildDataCenterKafkaUrlMap() {
    return getCommonConfig().getChildDataCenterKafkaUrlMap();
  }

  public Set<String> getParentFabrics() {
    return getCommonConfig().getParentFabrics();
  }

  public boolean isZkSharedMetaSystemSchemaStoreAutoCreationEnabled() {
    return getCommonConfig().isZkSharedMetaSystemSchemaStoreAutoCreationEnabled();
  }

  public boolean isZkSharedDaVinciPushStatusSystemSchemaStoreAutoCreationEnabled() {
    return getCommonConfig().isZkSharedDaVinciPushStatusSystemSchemaStoreAutoCreationEnabled();
  }

  public boolean isParticipantMessageStoreEnabled() {
    return getCommonConfig().isParticipantMessageStoreEnabled();
  }

  public long getSystemStoreAclSynchronizationDelayMs() {
    return getCommonConfig().getSystemStoreAclSynchronizationDelayMs();
  }

  public String getRegionName() {
    return getCommonConfig().getRegionName();
  }

  public String getEmergencySourceRegion(String clusterName) {
    return getControllerConfig(clusterName).getEmergencySourceRegion();
  }

  public int getGraveyardCleanupSleepIntervalBetweenListFetchMinutes() {
    return getCommonConfig().getStoreGraveyardCleanupSleepIntervalBetweenListFetchMinutes();
  }

  public PubSubClientsFactory getPubSubClientsFactory() {
    return getCommonConfig().getPubSubClientsFactory();
  }

  public PubSubPositionTypeRegistry getPubSubPositionTypeRegistry() {
    return getCommonConfig().getPubSubPositionTypeRegistry();
  }

  public PubSubAdminAdapterFactory getSourceOfTruthAdminAdapterFactory() {
    return getCommonConfig().getSourceOfTruthAdminAdapterFactory();
  }

  public long getDanglingTopicCleanupIntervalSeconds() {
    return getCommonConfig().getDanglingTopicCleanupIntervalSeconds();
  }

  public int getDanglingTopicOccurrenceThresholdForCleanup() {
    return getCommonConfig().getDanglingTopicOccurrenceThresholdForCleanup();
  }

  public int getDefaultMaxRecordSizeBytes() {
    return getCommonConfig().getDefaultMaxRecordSizeBytes();
  }

  public long getServiceDiscoveryRegistrationRetryMS() {
    return getCommonConfig().getServiceDiscoveryRegistrationRetryMS();
  }

  public List<String> getControllerInstanceTagList() {
    return getCommonConfig().getControllerInstanceTagList();
  }

  public String getRepushOrchestratorClassName() {
    return getCommonConfig().getRepushOrchestratorClassName();
  }

  public VeniceProperties getRepushOrchestratorConfigs() {
    return getCommonConfig().getRepushOrchestratorConfigs();
  }

  public boolean isLogCompactionEnabled() {
    return getCommonConfig().isLogCompactionEnabled();
  }

  public boolean isLogCompactionSchedulingEnabled() {
    return getCommonConfig().isLogCompactionSchedulingEnabled();
  }

  public int getLogCompactionThreadCount() {
    return getCommonConfig().getLogCompactionThreadCount();
  }

  public long getLogCompactionIntervalMS() {
    return getCommonConfig().getLogCompactionIntervalMS();
  }

  public long getLogCompactionThresholdMS() {
    return getCommonConfig().getLogCompactionThresholdMS();
  }

  public boolean isRealTimeTopicVersioningEnabled() {
    return getCommonConfig().getRealTimeTopicVersioningEnabled();
  }

  public LogContext getLogContext() {
    return getCommonConfig().getLogContext();
  }

  public PubSubTopicRepository getPubSubTopicRepository() {
    return pubSubTopicRepository;
  }
}
