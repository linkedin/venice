package com.linkedin.venice.controllerapi;

/**
 * Data model of response from a repush job trigger request for a store
 */
public class RepushJobResponse extends ControllerResponse {
  private String executionId;

  public RepushJobResponse() {
    // TODO: ControlleResponse types need to have a default constructor which takes no arguments,
    // we either need to refactor that or make it so this class can populate executionId in a different way
    this.executionId = "-1";
  }

  public RepushJobResponse(String executionId) {
    this.executionId = executionId;
  }

  public String getExecutionId() {
    return executionId;
  }
}
