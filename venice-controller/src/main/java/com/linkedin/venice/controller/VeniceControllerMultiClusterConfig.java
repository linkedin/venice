package com.linkedin.venice.controller;

import com.linkedin.venice.exceptions.VeniceNoClusterException;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


public class VeniceControllerMultiClusterConfig {

  private final Map<String, VeniceControllerConfig> clusterToConfigMap;

  public VeniceControllerMultiClusterConfig(Collection<VeniceProperties> propertiesCollection) {
    clusterToConfigMap = new HashMap<>();
    for (VeniceProperties properties : propertiesCollection) {
      VeniceControllerConfig controllerConfig = new VeniceControllerConfig(properties);
      clusterToConfigMap.put(controllerConfig.getClusterName(), controllerConfig);
    }
  }

  public VeniceControllerMultiClusterConfig(Map<String, VeniceControllerConfig> configMap){
    clusterToConfigMap = new HashMap<String,VeniceControllerConfig>(configMap);
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

  public boolean isParent() {
    return getCommonConfig().isParent();
  }

  public int getTopicMonitorPollIntervalMs() {
    return getCommonConfig().getTopicMonitorPollIntervalMs();
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

  public int getControllerClusterReplica() {
    return getCommonConfig().getControllerClusterReplica();
  }

  public String getKafkaBootstrapServers() {
    return getCommonConfig().getKafkaBootstrapServers();
  }

  public String getSslKafkaBootstrapServers() {
    return getCommonConfig().getSslKafkaBootStrapServers();
  }

  public boolean isSslToKafka(){
    return getCommonConfig().isSslToKafka();
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

  public String getControllerClusterZkAddresss() {
    return getCommonConfig().getControllerClusterZkAddresss();
  }

  public int getParentControllerWaitingTimeForConsumptionMs(){
    return getCommonConfig().getParentControllerWaitingTimeForConsumptionMs();
  }

  public int getKafkaReplicaFactor(){
    return getCommonConfig().getKafkaReplicaFactor();
  }

  public long getTopicCreationThrottlingTimeWindowMs(){
    return getCommonConfig().getTopicCreationThrottlingTimeWindowMs();
  }

  public Map<String,String> getClusterToD2Map(){
    return getCommonConfig().getClusterToD2Map();
  }

  public int getTopicManagerKafkaOperationTimeOutMs() {
    return getCommonConfig().getTopicManagerKafkaOperationTimeOutMs();
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

  public Set<String> getClusters() {
    return clusterToConfigMap.keySet();
  }
}
