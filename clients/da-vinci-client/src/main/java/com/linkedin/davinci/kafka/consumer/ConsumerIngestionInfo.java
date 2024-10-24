package com.linkedin.davinci.kafka.consumer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;


public class ConsumerIngestionInfo {
  private String consumerName;
  private Map<String, TopicPartitionIngestionInfo> consumerIngestionInfo;

  @JsonCreator
  public ConsumerIngestionInfo(
      @JsonProperty("consumerName") String consumerName,
      @JsonProperty("consumerIngestionInfo") Map<String, TopicPartitionIngestionInfo> consumerIngestionInfo) {
    this.consumerIngestionInfo = consumerIngestionInfo;
    this.consumerName = consumerName;
  }

  public String getConsumerName() {
    return consumerName;
  }

  public void setConsumerName(String consumerName) {
    this.consumerName = consumerName;
  }

  public Map<String, TopicPartitionIngestionInfo> getConsumerIngestionInfo() {
    return consumerIngestionInfo;
  }

  public void setConsumerIngestionInfo(Map<String, TopicPartitionIngestionInfo> consumerIngestionInfo) {
    this.consumerIngestionInfo = consumerIngestionInfo;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ConsumerIngestionInfo consumerIngestionInfo = (ConsumerIngestionInfo) o;
    return this.consumerName.equals(consumerIngestionInfo.getConsumerName())
        && this.consumerIngestionInfo.equals(consumerIngestionInfo.getConsumerIngestionInfo());
  }

  @Override
  public int hashCode() {
    int result = consumerName.hashCode();
    result = 31 * result + consumerIngestionInfo.hashCode();
    return result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{");

    for (Map.Entry<String, TopicPartitionIngestionInfo> entry: consumerIngestionInfo.entrySet()) {
      sb.append("\"").append(entry.getKey()).append("\"").append("=").append(entry.getValue().toString()).append(", ");
    }

    // Remove trailing comma and space, if any
    if (!consumerIngestionInfo.isEmpty()) {
      sb.setLength(sb.length() - 2);
    }

    sb.append("}");
    return sb.toString();
  }
}
