package com.linkedin.venice.controllerapi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;


public class AggregatedHealthStatusRequest {
  List<String> instances;
  List<String> to_be_stopped_instances;
  String cluster_id;

  @JsonCreator
  public AggregatedHealthStatusRequest(
      @JsonProperty("instances") List<String> instances,
      @JsonProperty("to_be_stopped_instances") List<String> to_be_stopped_instances,
      @JsonProperty("cluster_id") String cluster_id) {
    this.instances = instances;
    this.to_be_stopped_instances = to_be_stopped_instances;
    this.cluster_id = cluster_id;
  }

  @JsonProperty("cluster_id")
  public void setClusterId(String cluster_id) {
    this.cluster_id = cluster_id;
  }

  @JsonProperty("cluster_id")
  public String getClusterId() {
    return cluster_id;
  }

  @JsonProperty("instances")
  public List<String> getInstances() {
    return instances;
  }

  @JsonProperty("instances")
  public void setInstances(List<String> instances) {
    this.instances = instances;
  }

  @JsonProperty("to_be_stopped_instances")
  public List<String> getToBeStoppedInstances() {
    return to_be_stopped_instances;
  }

  @JsonProperty("to_be_stopped_instances")
  public void setToBeStoppedInstances(List<String> to_be_stopped_instances) {
    this.to_be_stopped_instances = to_be_stopped_instances;
  }
}
