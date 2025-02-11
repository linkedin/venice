package com.linkedin.venice.controller.repush;

import com.linkedin.venice.controllerapi.ControllerResponse;


/**
 * Data model of response from a repush job trigger request for a store
 */
public class RepushJobResponse extends ControllerResponse {
  private final String executionId;
  private final String executionUrl;

  public RepushJobResponse(String executionId, String executionUrl) {
    this.executionId = executionId;
    this.executionUrl = executionUrl;
  }

  public String getExecutionId() {
    return executionId;
  }

  public String getExecutionUrl() {
    return executionUrl;
  }
}
