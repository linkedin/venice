package com.linkedin.venice.controllerapi;


public class StoreMigrationResponse extends ControllerResponse {
  private String srcClusterName;

  public String getSrcClusterName() {
    return srcClusterName;
  }

  public void setSrcClusterName(String srcClusterName) {
    this.srcClusterName = srcClusterName;
  }


  @Override
  public String toString() {
      return StoreMigrationResponse.class.getSimpleName() + "(src cluster: " + srcClusterName + ", super: "
          + super.toString() + ")";
  }
}
