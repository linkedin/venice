package com.linkedin.venice.controllerapi;

import java.util.Map;


public class MultiVersionStatusResponse extends ControllerResponse {
  private Map<String, String> versionStatusMap;

  public Map<String, String> getVersionStatusMap() {
    return versionStatusMap;
  }

  public void setVersionStatusMap(Map<String, String> versionStatusMap) {
    this.versionStatusMap = versionStatusMap;
  }
}
