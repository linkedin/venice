package com.linkedin.venice.controllerapi;

import java.util.Map;


public class ChildAwareResponse extends ControllerResponse {
  private Map<String, String> childDataCenterControllerUrlMap;
  private Map<String, String> childDataCenterControllerD2Map;
  String d2ServiceName;

  public Map<String, String> getChildDataCenterControllerUrlMap() {
    return childDataCenterControllerUrlMap;
  }

  public void setChildDataCenterControllerUrlMap(Map<String, String> childDataCenterControllerUrlMap) {
    this.childDataCenterControllerUrlMap = childDataCenterControllerUrlMap;
  }

  public Map<String, String> getChildDataCenterControllerD2Map() {
    return childDataCenterControllerD2Map;
  }

  public void setChildDataCenterControllerD2Map(Map<String, String> childDataCenterControllerD2Map) {
    this.childDataCenterControllerD2Map = childDataCenterControllerD2Map;
  }

  public String getD2ServiceName() {
    return d2ServiceName;
  }

  public void setD2ServiceName(String d2ServiceName) {
    this.d2ServiceName = d2ServiceName;
  }

  @Override
  public String toString() {
    if (childDataCenterControllerUrlMap == null && childDataCenterControllerD2Map == null) {
      return super.toString();
    } else {
      return ChildAwareResponse.class.getSimpleName() + "(childDataCenterControllerUrlMap: "
          + childDataCenterControllerUrlMap + ", childDataCenterControllerD2Map: " + childDataCenterControllerD2Map
          + ", d2ServiceName: " + d2ServiceName + ", super: " + super.toString() + ")";
    }
  }
}
