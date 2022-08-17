package com.linkedin.venice.controllerapi;

import java.util.Map;


public class MultiStoreStatusResponse extends ControllerResponse {
  private Map<String, String> storeStatusMap;

  public Map<String, String> getStoreStatusMap() {
    return storeStatusMap;
  }

  public void setStoreStatusMap(Map<String, String> storeStatusMap) {
    this.storeStatusMap = storeStatusMap;
  }
}
