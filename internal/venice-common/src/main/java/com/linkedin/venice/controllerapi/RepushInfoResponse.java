package com.linkedin.venice.controllerapi;

public class RepushInfoResponse extends ControllerResponse {
  private RepushInfo repushInfo;

  public RepushInfo getRepushInfo() {
    return repushInfo;
  }

  public void setRepushInfo(RepushInfo repushInfo) {
    this.repushInfo = repushInfo;
  }
}
