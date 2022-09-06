package com.linkedin.venice.controllerapi;

import java.util.Map;


public class MultiNodesStatusResponse extends ControllerResponse {
  private Map<String, String> instancesStatusMap;

  public Map<String, String> getInstancesStatusMap() {
    return instancesStatusMap;
  }

  public void setInstancesStatusMap(Map<String, String> instancesStatusMap) {
    this.instancesStatusMap = instancesStatusMap;
  }
}
