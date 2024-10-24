package com.linkedin.davinci.kafka.consumer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;


public class ConsumerServiceIngestionInfo {
  private String consumerServiceName;
  private Map<String, ConsumerIngestionInfo> consumerServiceIngestionInfo;

  @JsonCreator
  public ConsumerServiceIngestionInfo(
      @JsonProperty("consumerServiceName") String consumerServiceName,
      @JsonProperty("consumerServiceIngestionInfo") Map<String, ConsumerIngestionInfo> consumerServiceIngestionInfo) {
    this.consumerServiceIngestionInfo = consumerServiceIngestionInfo;
    this.consumerServiceName = consumerServiceName;
  }

  public void setConsumerServiceName(String consumerServiceName) {
    this.consumerServiceName = consumerServiceName;
  }

  public String getConsumerServiceName() {
    return consumerServiceName;
  }

  public void setConsumerServiceIngestionInfo(Map<String, ConsumerIngestionInfo> consumerServiceIngestionInfo) {
    this.consumerServiceIngestionInfo = consumerServiceIngestionInfo;
  }

  public Map<String, ConsumerIngestionInfo> getConsumerServiceIngestionInfo() {
    return consumerServiceIngestionInfo;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ConsumerServiceIngestionInfo consumerServiceIngestionInfo = (ConsumerServiceIngestionInfo) o;
    return this.consumerServiceName.equals(consumerServiceIngestionInfo.getConsumerServiceName())
        && this.consumerServiceIngestionInfo.equals(consumerServiceIngestionInfo.getConsumerServiceIngestionInfo());
  }

  @Override
  public int hashCode() {
    int result = consumerServiceName.hashCode();
    result = 31 * result + consumerServiceIngestionInfo.hashCode();
    return result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{");

    for (Map.Entry<String, ConsumerIngestionInfo> entry: consumerServiceIngestionInfo.entrySet()) {
      sb.append("\"").append(entry.getKey()).append("\"").append("=").append(entry.getValue().toString()).append(", ");
    }

    // Remove trailing comma and space, if any
    if (!consumerServiceIngestionInfo.isEmpty()) {
      sb.setLength(sb.length() - 2);
    }

    sb.append("}");
    return sb.toString();
  }
}
