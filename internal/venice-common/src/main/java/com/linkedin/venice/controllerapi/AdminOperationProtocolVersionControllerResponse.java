package com.linkedin.venice.controllerapi;

import java.util.HashMap;
import java.util.Map;


public class AdminOperationProtocolVersionControllerResponse extends ControllerResponse {
  private long localAdminOperationProtocolVersion = -1;
  private String localControllerName = "";
  private Map<String, Long> controllerNameToVersionMap = new HashMap<>();

  public void setLocalAdminOperationProtocolVersion(long adminOperationProtocolVersion) {
    this.localAdminOperationProtocolVersion = adminOperationProtocolVersion;
  }

  public long getLocalAdminOperationProtocolVersion() {
    return localAdminOperationProtocolVersion;
  }

  public void setLocalControllerName(String controllerName) {
    this.localControllerName = controllerName;
  }

  public String getLocalControllerName() {
    return localControllerName;
  }

  public void setControllerNameToVersionMap(Map<String, Long> controllerNameToVersionMap) {
    this.controllerNameToVersionMap = controllerNameToVersionMap;
  }

  public Map<String, Long> getControllerNameToVersionMap() {
    return controllerNameToVersionMap;
  }
}
