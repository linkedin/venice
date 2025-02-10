package com.linkedin.venice.controller.repush;

import com.linkedin.venice.controllerapi.ControllerResponse;


/**
 * Data model of response from a repush job trigger request for a store
 */
public class RepushJobResponse extends ControllerResponse {
  private final String execId;
  private final String execUrl;

  public RepushJobResponse(String execId, String execUrl) {
    this.execId = execId;
    this.execUrl = execUrl;
  }

  public String getExecId() {
    return execId;
  }

  public String getExecUrl() {
    return execUrl;
  }

}
