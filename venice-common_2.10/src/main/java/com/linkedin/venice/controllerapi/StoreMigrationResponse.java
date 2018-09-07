package com.linkedin.venice.controllerapi;

import java.util.List;


public class StoreMigrationResponse extends ControllerResponse {
  private String srcClusterName;
  private List<String> childControllerUrls;

  public String getSrcClusterName() {
    return srcClusterName;
  }

  public void setSrcClusterName(String srcClusterName) {
    this.srcClusterName = srcClusterName;
  }

  public List<String> getChildControllerUrls() {
    return childControllerUrls;
  }

  public void setChildControllerUrls(List<String> childControllerUrls) {
    this.childControllerUrls = childControllerUrls;
  }


  @Override
  public String toString() {
    if (childControllerUrls == null) {
      return StoreMigrationResponse.class.getSimpleName() + "(src cluster: " + srcClusterName + ", super: "
          + super.toString() + ")";
    } else {
      return StoreMigrationResponse.class.getSimpleName() + "(src cluster: " + srcClusterName + ", super: "
          + super.toString() + ", childControllers: " + childControllerUrls + ")";
    }
  }
}
