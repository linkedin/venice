package com.linkedin.venice.controller.server;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;


public class AggregratedHealthStatusRequest {
  @JsonFormat(with = JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
  List<String> instances;

  @JsonFormat(with = JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
  List<String> to_be_stopped_instances;
  String cluster_id;

  public AggregratedHealthStatusRequest(
      List<String> instances,
      List<String> to_be_stopped_instances,
      String cluster_id) {
    this.instances = instances;
    this.to_be_stopped_instances = to_be_stopped_instances;
    this.cluster_id = cluster_id;
  }

  public AggregratedHealthStatusRequest() {
  }

  @JsonProperty("cluster_id")
  public void setClusterId(String cluster_id) {
    this.cluster_id = cluster_id;
  }

  @JsonProperty("cluster_id")
  public String getClusterId() {
    return cluster_id;
  }

  public List<String> getInstances() {
    return instances;
  }

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
