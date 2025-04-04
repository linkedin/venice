package com.linkedin.venice.controllerapi;

import java.util.HashMap;
import java.util.Map;


public class AdminOperationProtocolVersionControllerResponse extends ControllerResponse {
  private long localAdminOperationProtocolVersion = -1;
  private String requestUrl = "";
  private Map<String, Long> urlToVersionMap = new HashMap<>();

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

  public void setUrlToVersionMap(Map<String, Long> urlToVersionMap) {
    this.urlToVersionMap = urlToVersionMap;
  }

  public Map<String, Long> getUrlToVersionMap() {
    return urlToVersionMap;
  }
}
