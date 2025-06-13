package com.linkedin.venice.controllerapi;

/**
 * Data model of response from a repush job trigger request for a store
 */
public class RepushJobResponse extends ControllerResponse {
  private String executionId;
  public static final String DEFAULT_EXECUTION_ID = "-1";

  public RepushJobResponse() {
    // TODO: ControllerResponse types need to have a default constructor which takes no arguments,
    // we either need to refactor that or make it so this class can populate executionId in a different way
    this.executionId = DEFAULT_EXECUTION_ID;
  }

  public RepushJobResponse(String storeName, String executionId) {
    this.setName(storeName);
    this.executionId = executionId;
  }

  public String getExecutionId() {
    return executionId;
  }

  /** This method copies the values of another RepushJobInstance. This is to
   * 1. put up with StoresRoutes::repushStore() and VeniceRouteHandler::internalHandle receiving the response through a
   * response instance passed into the method rather than a return value
   * 2. centralise the object field value duplication in this class to ensure all information are copied */
  public void copyValueOf(RepushJobResponse response) {
    super.copyValueOf(response);
    this.executionId = response.getExecutionId();
  }
}
