package com.linkedin.venice.controllerapi;

public class LocalAdminOperationProtocolVersionResponse extends ControllerResponse {
  private long adminOperationProtocolVersion = -1;

  public long getAdminOperationProtocolVersion() {
    return adminOperationProtocolVersion;
  }

  public void setAdminOperationProtocolVersion(long adminOperationProtocolVersion) {
    this.adminOperationProtocolVersion = adminOperationProtocolVersion;
  }
}
