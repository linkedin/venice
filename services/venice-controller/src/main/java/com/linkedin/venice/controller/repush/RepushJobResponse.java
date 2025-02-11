package com.linkedin.venice.controller.repush;

import com.linkedin.venice.controllerapi.ControllerResponse;


/**
 * Data model of response from a repush job trigger request for a store
 */
public class RepushJobResponse extends ControllerResponse {
  private final String executionRunId;
  private final String executionUrl;

  public RepushJobResponse(String executionRunId, String executionUrl) {
    this.executionRunId = executionRunId;
    this.executionUrl = executionUrl;
  }

  public String getExecutionRunId() {
    return executionRunId;
  }

  public String getExecutionUrl() {
    return executionUrl;
  }
}
