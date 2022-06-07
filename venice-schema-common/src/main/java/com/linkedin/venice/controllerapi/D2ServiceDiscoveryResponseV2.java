package com.linkedin.venice.controllerapi;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class D2ServiceDiscoveryResponseV2 extends D2ServiceDiscoveryResponse {
  public static final String D2_SERVICE_DISCOVERY_RESPONSE_V2_ENABLED = "d2.service.discovery.response.v2.enabled";

  String zkAddress;
  String kafkaZkAddress;
  String kafkaBootstrapServers;

  public String getZkAddress() {
    return zkAddress;
  }

  public void setZkAddress(String zkAddress) {
    this.zkAddress = zkAddress;
  }

  public String getKafkaZkAddress() {
    return kafkaZkAddress;
  }

  public void setKafkaZkAddress(String kafkaZkAddress) {
    this.kafkaZkAddress = kafkaZkAddress;
  }

  public String getKafkaBootstrapServers() {
    return kafkaBootstrapServers;
  }

  public void setKafkaBootstrapServers(String kafkaBootstrapServers) {
    this.kafkaBootstrapServers = kafkaBootstrapServers;
  }

  @JsonIgnore
  public String toString() {
    return D2ServiceDiscoveryResponseV2.class.getSimpleName() +
        "(zkAddress: " + zkAddress + ", kafkaZkAddress: " + kafkaZkAddress +
        ", kafkaBootstrapServers: " + kafkaBootstrapServers + ", super: " + super.toString() + ")";
  }
}
