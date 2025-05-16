package com.linkedin.venice.controllerapi;

import com.fasterxml.jackson.annotation.JsonIgnore;
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

  /*
   * This method is used to convert the object to a string representation.
   * AdminOperationProtocolVersionControllerResponse {"localAdminOperationProtocolVersion": 4, "cluster": "venice0,
   * "requestUrl": "host7", "controllerUrlToVersionMap": {host7=4, host9=6, host8=5}}
   */
  @JsonIgnore
  public String toString() {
    return new StringBuilder().append(this.getClass().getSimpleName())
        .append(" {")
        .append("\"localAdminOperationProtocolVersion\": ")
        .append(localAdminOperationProtocolVersion)
        .append(", \"cluster\": \"")
        .append(getCluster())
        .append(", \"localControllerName\": \"")
        .append(localControllerName)
        .append("\", \"controllerNameToVersionMap\": ")
        .append(controllerNameToVersionMap.toString())
        .append("}")
        .toString();
  }
}
