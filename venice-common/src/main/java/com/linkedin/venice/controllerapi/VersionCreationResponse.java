package com.linkedin.venice.controllerapi;

import org.codehaus.jackson.annotate.JsonIgnore;

public class VersionCreationResponse extends VersionResponse {
  /* Uses Json Reflective Serializer, get without set may break things */
  private int partitions = 0;
  private int replicas = 0;
  private String kafkaTopic = null;
  private String kafkaBootstrapServers = null;
  // As controller client will ignore the unknown field in json so we could add a field here without break the backward compatibility.
  private boolean enableSSL = false;

  public void setPartitions(int partitions) {
    this.partitions = partitions;
  }

  public void setReplicas(int replicas) {
    this.replicas = replicas;
  }

  public void setKafkaTopic(String kafkaTopic) {
    this.kafkaTopic = kafkaTopic;
  }

  public void setKafkaBootstrapServers(String kafkaBootstrapServers) {
    this.kafkaBootstrapServers = kafkaBootstrapServers;
  }

  public int getPartitions() {
    return partitions;
  }

  public int getReplicas() {
    return replicas;
  }

  public String getKafkaTopic() {
    return kafkaTopic;
  }

  public String getKafkaBootstrapServers() {
    return kafkaBootstrapServers;
  }

  public boolean isEnableSSL() {
    return enableSSL;
  }

  public void setEnableSSL(boolean enableSSL) {
    this.enableSSL = enableSSL;
  }

  @JsonIgnore
  public String toString() {
    return VersionCreationResponse.class.getSimpleName() + "(partitions: " + partitions +
        ", replicas: " + replicas +
        ", kafkaTopic: " + kafkaTopic +
        ", kafkaBootstrapServers: " + kafkaBootstrapServers +
        ", enableSSL: " + enableSSL +
        ", super: " + super.toString() + ")";
  }
}
