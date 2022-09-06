package com.linkedin.venice.controllerapi;

public class StoreMigrationResponse extends ControllerResponse {
  private String srcClusterName;

  private boolean storeMigrationAllowed = true;

  public String getSrcClusterName() {
    return srcClusterName;
  }

  public void setSrcClusterName(String srcClusterName) {
    this.srcClusterName = srcClusterName;
  }

  public boolean isStoreMigrationAllowed() {
    return storeMigrationAllowed;
  }

  public void setStoreMigrationAllowed(boolean storeMigrationAllowed) {
    this.storeMigrationAllowed = storeMigrationAllowed;
  }

  @Override
  public String toString() {
    return StoreMigrationResponse.class.getSimpleName() + "(src cluster: " + srcClusterName + ", super: "
        + super.toString() + ")";
  }
}
