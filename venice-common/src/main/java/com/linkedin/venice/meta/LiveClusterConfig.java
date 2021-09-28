package com.linkedin.venice.meta;

import com.linkedin.venice.ConfigKeys;
import java.util.HashMap;
import java.util.Map;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;


public class LiveClusterConfig {
  @JsonProperty(ConfigKeys.SERVER_KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND)
  private Map<String, Integer> serverKafkaFetchQuotaRecordsPerSecond;

  public LiveClusterConfig() {}

  public LiveClusterConfig(LiveClusterConfig clone) {
    if (clone.getServerKafkaFetchQuotaRecordsPerSecond() != null) {
      serverKafkaFetchQuotaRecordsPerSecond = new HashMap<>(clone.getServerKafkaFetchQuotaRecordsPerSecond());
    }
  }

  // ------------------------ Getter/Setter for Jackson to ser/de LiveClusterConfig ------------------------

  public Map<String, Integer> getServerKafkaFetchQuotaRecordsPerSecond() {
    return serverKafkaFetchQuotaRecordsPerSecond;
  }

  public void setServerKafkaFetchQuotaRecordsPerSecond(Map<String, Integer> serverKafkaFetchQuotaRecordsPerSecond) {
    this.serverKafkaFetchQuotaRecordsPerSecond = serverKafkaFetchQuotaRecordsPerSecond;
  }

  // -------------------------------------- Default Values for config --------------------------------------

  public static final int DEFAULT_SERVER_KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND = -1;

  // --------------------------- Helper accessor/modifiers for LiveClusterConfig ---------------------------

  @JsonIgnore
  public int getServerKafkaFetchQuotaRecordsPerSecondForRegion(String regionName) {
    if (serverKafkaFetchQuotaRecordsPerSecond == null) {
      return DEFAULT_SERVER_KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND;
    }

    return serverKafkaFetchQuotaRecordsPerSecond.getOrDefault(regionName, DEFAULT_SERVER_KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND);
  }

  @JsonIgnore
  public void setServerKafkaFetchQuotaRecordsPerSecondForRegion(String regionName, int kafkaFetchQuotaRecordsPerSecond) {
    if (serverKafkaFetchQuotaRecordsPerSecond == null) {
      serverKafkaFetchQuotaRecordsPerSecond = new HashMap<>();
    }

    serverKafkaFetchQuotaRecordsPerSecond.put(regionName, kafkaFetchQuotaRecordsPerSecond);
  }

  public String toString() {
    return new StringBuilder()
        .append(ConfigKeys.SERVER_KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND)
        .append('=').append(serverKafkaFetchQuotaRecordsPerSecond).append('\n')
        .toString();
  }
}
