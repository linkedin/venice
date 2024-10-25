package com.linkedin.venice.controllerapi.request;

public class AdminCommandExecutionStatusRequest extends ControllerRequest {
  private long adminCommandExecutionId;

  public AdminCommandExecutionStatusRequest(String clusterName, long adminCommandExecutionId) {
    super(clusterName);
    this.adminCommandExecutionId = adminCommandExecutionId;
  }

  public long getAdminCommandExecutionId() {
    return adminCommandExecutionId;
  }
}
