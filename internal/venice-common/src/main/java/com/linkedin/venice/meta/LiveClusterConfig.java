package com.linkedin.venice.meta;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.utils.Utils;
import java.util.HashMap;
import java.util.Map;


public class LiveClusterConfig {
  @JsonProperty(ConfigKeys.SERVER_KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND)
  private Map<String, Integer> serverKafkaFetchQuotaRecordsPerSecond;

  @JsonProperty(ConfigKeys.ALLOW_STORE_MIGRATION)
  private boolean storeMigrationAllowed = true;

  @JsonProperty(ConfigKeys.CHILD_CONTROLLER_ADMIN_TOPIC_CONSUMPTION_ENABLED)
  private boolean childControllerAdminTopicConsumptionEnabled = true;

  public LiveClusterConfig() {
  }

  public LiveClusterConfig(LiveClusterConfig clone) {
    if (clone.getServerKafkaFetchQuotaRecordsPerSecond() != null) {
      serverKafkaFetchQuotaRecordsPerSecond = new HashMap<>(clone.getServerKafkaFetchQuotaRecordsPerSecond());
    }
    storeMigrationAllowed = clone.storeMigrationAllowed;
    childControllerAdminTopicConsumptionEnabled = clone.childControllerAdminTopicConsumptionEnabled;
  }

  // ------------------------ Getter/Setter for Jackson to ser/de LiveClusterConfig ------------------------

  public Map<String, Integer> getServerKafkaFetchQuotaRecordsPerSecond() {
    return serverKafkaFetchQuotaRecordsPerSecond;
  }

  public void setServerKafkaFetchQuotaRecordsPerSecond(Map<String, Integer> serverKafkaFetchQuotaRecordsPerSecond) {
    this.serverKafkaFetchQuotaRecordsPerSecond = serverKafkaFetchQuotaRecordsPerSecond;
  }

  public boolean isStoreMigrationAllowed() {
    return storeMigrationAllowed;
  }

  public void setStoreMigrationAllowed(boolean storeMigrationAllowed) {
    this.storeMigrationAllowed = storeMigrationAllowed;
  }

  public boolean isChildControllerAdminTopicConsumptionEnabled() {
    return childControllerAdminTopicConsumptionEnabled;
  }

  public void setChildControllerAdminTopicConsumptionEnabled(boolean childControllerAdminTopicConsumptionEnabled) {
    this.childControllerAdminTopicConsumptionEnabled = childControllerAdminTopicConsumptionEnabled;
  }

  // -------------------------------------- Default Values for config --------------------------------------

  public static final int DEFAULT_SERVER_KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND = -1;

  // --------------------------- Helper accessor/modifiers for LiveClusterConfig ---------------------------

  @JsonIgnore
  public int getServerKafkaFetchQuotaRecordsPerSecondForRegion(String regionName) {
    if (serverKafkaFetchQuotaRecordsPerSecond == null) {
      return DEFAULT_SERVER_KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND;
    }

    return serverKafkaFetchQuotaRecordsPerSecond
        .getOrDefault(regionName, DEFAULT_SERVER_KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND);
  }

  @JsonIgnore
  public void setServerKafkaFetchQuotaRecordsPerSecondForRegion(
      String regionName,
      int kafkaFetchQuotaRecordsPerSecond) {
    if (serverKafkaFetchQuotaRecordsPerSecond == null) {
      serverKafkaFetchQuotaRecordsPerSecond = new HashMap<>();
    }

    serverKafkaFetchQuotaRecordsPerSecond.put(regionName, kafkaFetchQuotaRecordsPerSecond);
  }

  public String toString() {
    return new StringBuilder().append(ConfigKeys.SERVER_KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND)
        .append('=')
        .append(serverKafkaFetchQuotaRecordsPerSecond)
        .append(Utils.NEW_LINE_CHAR)
        .append(ConfigKeys.ALLOW_STORE_MIGRATION)
        .append('=')
        .append(storeMigrationAllowed)
        .append(Utils.NEW_LINE_CHAR)
        .append(ConfigKeys.CHILD_CONTROLLER_ADMIN_TOPIC_CONSUMPTION_ENABLED)
        .append('=')
        .append(childControllerAdminTopicConsumptionEnabled)
        .append(Utils.NEW_LINE_CHAR)
        .toString();
  }
}
