package com.linkedin.venice.controllerapi.request;

public class ClusterDiscoveryRequest extends ControllerRequest {
  public ClusterDiscoveryRequest(String storeName) {
    super("-", storeName);
  }
}
