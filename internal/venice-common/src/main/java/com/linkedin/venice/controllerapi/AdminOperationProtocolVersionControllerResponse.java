package com.linkedin.venice.controllerapi;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.HashMap;
import java.util.Map;


public class AdminOperationProtocolVersionControllerResponse extends ControllerResponse {
  private long localAdminOperationProtocolVersion = -1;
  private String requestUrl = "";
  private Map<String, Long> controllerUrlToVersionMap = new HashMap<>();

  public void setLocalAdminOperationProtocolVersion(long adminOperationProtocolVersion) {
    this.localAdminOperationProtocolVersion = adminOperationProtocolVersion;
  }

  public long getLocalAdminOperationProtocolVersion() {
    return localAdminOperationProtocolVersion;
  }

  public void setRequestUrl(String url) {
    this.requestUrl = url;
  }

  public String getRequestUrl() {
    return requestUrl;
  }

  public void setControllerUrlToVersionMap(Map<String, Long> urlToVersionMap) {
    this.controllerUrlToVersionMap = urlToVersionMap;
  }

  public Map<String, Long> getControllerUrlToVersionMap() {
    return controllerUrlToVersionMap;
  }

  @JsonIgnore
  public String toString() {
    return new StringBuilder().append(this.getClass().getSimpleName())
        .append(" {\n")
        .append("  \"localAdminOperationProtocolVersion\": ")
        .append(localAdminOperationProtocolVersion)
        .append(",\n")
        .append("  \"cluster\": \"")
        .append(getCluster())
        .append("\",\n")
        .append("  \"requestUrl\": \"")
        .append(requestUrl)
        .append("\",\n")
        .append("  \"controllerUrlToVersionMap\": ")
        .append(controllerUrlToVersionMap.toString())
        .append("\n")
        .append("}")
        .toString();
  }
}
