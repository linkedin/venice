package com.linkedin.venice.controllerapi;

import java.util.Map;


public class ChildAwareResponse extends ControllerResponse {
  private Map<String, String> childClusterMap;

  public Map<String, String> getChildClusterMap() {
    return childClusterMap;
  }

  public void setChildClusterMap(Map<String, String> childClusterMap) {
    this.childClusterMap = childClusterMap;
  }


  @Override
  public String toString() {
    if (childClusterMap == null) {
      return super.toString();
    } else {
      return ChildAwareResponse.class.getSimpleName() + "(childControllers: " + childClusterMap + ", super: "
          + super.toString() + ")";
    }
  }
}
