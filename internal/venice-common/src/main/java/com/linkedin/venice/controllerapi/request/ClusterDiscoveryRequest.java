package com.linkedin.venice.controllerapi.request;

public class ClusterDiscoveryRequest extends ControllerRequest {
  private static final String CLUSTER_NAME_PLACEHOLDER = "UNKNOWN";

  public ClusterDiscoveryRequest(String storeName) {
    super(CLUSTER_NAME_PLACEHOLDER, storeName);
  }
}
