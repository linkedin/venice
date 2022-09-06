package com.linkedin.venice.controllerapi;

public class AclResponse extends ControllerResponse {
  private String accessPermissions;

  public String getAccessPermissions() {
    return accessPermissions;
  }

  public void setAccessPermissions(String accessPermissions) {
    this.accessPermissions = accessPermissions;
  }
}
