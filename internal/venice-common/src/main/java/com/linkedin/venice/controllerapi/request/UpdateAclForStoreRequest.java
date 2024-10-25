package com.linkedin.venice.controllerapi.request;

public class UpdateAclForStoreRequest extends ControllerRequest {
  private final String accessPermissions;

  public UpdateAclForStoreRequest(String clusterName, String storeName, String accessPermissions) {
    super(clusterName, storeName);
    this.accessPermissions = validateParam(accessPermissions, "Access permissions");
  }

  public String getAccessPermissions() {
    return accessPermissions;
  }
}
