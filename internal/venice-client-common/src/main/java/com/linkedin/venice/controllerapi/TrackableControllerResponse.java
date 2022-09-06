package com.linkedin.venice.controllerapi;

import com.fasterxml.jackson.annotation.JsonIgnore;


/**
 * Extend this class to create response objects for async command. Execution id is added to track the execution status
 * of async command.
 */
public class TrackableControllerResponse extends ControllerResponse {
  private long executionId;

  public long getExecutionId() {
    return executionId;
  }

  public void setExecutionId(long executionId) {
    this.executionId = executionId;
  }

  @JsonIgnore
  public String toString() {
    return ControllerResponse.class.getSimpleName() + "(cluster: " + this.getCluster() + ", name: " + this.getName()
        + ", error: " + this.getError() + ", executionId: " + executionId + ")";
  }
}
