package com.linkedin.venice.controllerapi.request;

public class DiscoverLeaderControllerRequest extends ControllerRequest {
  public DiscoverLeaderControllerRequest(String clusterName) {
    super(clusterName);
  }
}
