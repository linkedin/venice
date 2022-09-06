package com.linkedin.venice.routerapi;

import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.pushmonitor.ExecutionStatus;


/**
 * Push job status response for a resource; this is a response
 * that will be returned by Router.
 */
public class PushStatusResponse extends ControllerResponse {
  private ExecutionStatus executionStatus = ExecutionStatus.UNKNOWN;

  public ExecutionStatus getExecutionStatus() {
    return executionStatus;
  }

  public void setExecutionStatus(ExecutionStatus executionStatus) {
    this.executionStatus = executionStatus;
  }
}
