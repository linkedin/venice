package com.linkedin.venice.controllerapi;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER_ID;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.INSTANCES;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.TO_BE_STOPPED_INSTANCES;

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
      @JsonProperty(CLUSTER_ID) String cluster_id,
      @JsonProperty(INSTANCES) List<String> instances,
      @JsonProperty(TO_BE_STOPPED_INSTANCES) List<String> to_be_stopped_instances) {
    if (cluster_id == null) {
      throw new IllegalArgumentException("'" + CLUSTER_ID + "' is required");
    }
    this.cluster_id = cluster_id;

    if (instances == null) {
      throw new IllegalArgumentException("'" + INSTANCES + "' is required");
    }
    this.instances = instances;

    if (to_be_stopped_instances == null) {
      this.to_be_stopped_instances = Collections.emptyList();
    } else {
      this.to_be_stopped_instances = to_be_stopped_instances;
    }
  }

  @JsonProperty(CLUSTER_ID)
  public String getClusterId() {
    return cluster_id;
  }

  @JsonProperty(INSTANCES)
  public List<String> getInstances() {
    return instances;
  }

  @JsonProperty(TO_BE_STOPPED_INSTANCES)
  public List<String> getToBeStoppedInstances() {
    return to_be_stopped_instances;
  }
}
