package com.linkedin.venice;

import com.linkedin.venice.controllerapi.ControllerResponse;


public class LastSucceedExecutionIdResponse extends ControllerResponse {
  private long lastSucceedExecutionId;

  public long getLastSucceedExecutionId() {
    return lastSucceedExecutionId;
  }

  public void setLastSucceedExecutionId(long lastSucceedExecutionId) {
    this.lastSucceedExecutionId = lastSucceedExecutionId;
  }
}
