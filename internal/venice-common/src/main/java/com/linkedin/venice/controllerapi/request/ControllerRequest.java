package com.linkedin.venice.controllerapi.request;

/**
 * Extend this class to create request objects for the controller
 */
public class ControllerRequest {
  private final String clusterName;

  public ControllerRequest(String clusterName) {
    this.clusterName = clusterName;
  }

  public String getClusterName() {
    return clusterName;
  }
}
