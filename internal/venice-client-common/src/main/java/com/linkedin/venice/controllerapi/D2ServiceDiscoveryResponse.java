package com.linkedin.venice.controllerapi;

import com.fasterxml.jackson.annotation.JsonIgnore;


public class D2ServiceDiscoveryResponse extends ControllerResponse {
  String d2Service;
  String serverD2Service;
  String zkAddress;
  String kafkaBootstrapServers;

  public String getD2Service() {
    return d2Service;
  }

  public void setD2Service(String d2Service) {
    this.d2Service = d2Service;
  }

  public String getServerD2Service() {
    return serverD2Service;
  }

  public void setServerD2Service(String serverD2Service) {
    this.serverD2Service = serverD2Service;
  }

  public String getZkAddress() {
    return zkAddress;
  }

  public void setZkAddress(String zkAddress) {
    this.zkAddress = zkAddress;
  }

  public String getKafkaBootstrapServers() {
    return kafkaBootstrapServers;
  }

  public void setKafkaBootstrapServers(String kafkaBootstrapServers) {
    this.kafkaBootstrapServers = kafkaBootstrapServers;
  }

  @JsonIgnore
  public String toString() {
    return D2ServiceDiscoveryResponse.class.getSimpleName() + "(d2Service: " + d2Service + ", serverD2Service: "
        + serverD2Service + ", zkAddress: " + zkAddress + ", kafkaBootstrapServers: " + kafkaBootstrapServers
        + ", super: " + super.toString() + ")";
  }
}
