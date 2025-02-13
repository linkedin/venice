package com.linkedin.venice.controller.repush;

import com.linkedin.venice.controllerapi.ControllerResponse;


/**
 * Data model of response from a repush job trigger request for a store
 */
public class RepushJobResponse extends ControllerResponse {
  private final String executionId;

  public RepushJobResponse(String executionId) {
    this.executionId = executionId;
  }

  public String getExecutionId() {
    return executionId;
  }
}
