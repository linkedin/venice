package com.linkedin.venice.controllerapi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collections;
import java.util.List;


public class AggregatedHealthStatusRequest {
  private final String cluster_id;
  private final List<String> instances;
  private final List<String> to_be_stopped_instances;

  @JsonCreator
  public AggregatedHealthStatusRequest(
      @JsonProperty("cluster_id") String cluster_id,
      @JsonProperty("instances") List<String> instances,
      @JsonProperty("to_be_stopped_instances") List<String> to_be_stopped_instances) {
    this.cluster_id = cluster_id;
    this.instances = instances;
    if (to_be_stopped_instances == null) {
      this.to_be_stopped_instances = Collections.emptyList();
    } else {
      this.to_be_stopped_instances = to_be_stopped_instances;
    }
  }

  @JsonProperty("cluster_id")
  public String getClusterId() {
    return cluster_id;
  }

  @JsonProperty("instances")
  public List<String> getInstances() {
    return instances;
  }

  @JsonProperty("to_be_stopped_instances")
  public List<String> getToBeStoppedInstances() {
    return to_be_stopped_instances;
  }
}
