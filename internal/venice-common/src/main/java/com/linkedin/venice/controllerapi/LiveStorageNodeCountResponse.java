package com.linkedin.venice.controllerapi;

import java.util.Map;


public class LiveStorageNodeCountResponse extends ControllerResponse {
  private Map<String, Integer> liveStorageNodeCountPerFabric;

  public Map<String, Integer> getLiveStorageNodeCountPerFabric() {
    return liveStorageNodeCountPerFabric;
  }

  public void setLiveStorageNodeCountPerFabric(Map<String, Integer> liveStorageNodeCountPerFabric) {
    this.liveStorageNodeCountPerFabric = liveStorageNodeCountPerFabric;
  }
}
