package com.linkedin.venice.controllerapi;

/**
 * Response object for cluster discovery operations.
 * Contains the cluster name and D2 service information for a given store.
 */
public class DiscoverClusterResponse extends ControllerResponse {
  private String storeName;
  private String d2Service;
  private String serverD2Service;

  public String getStoreName() {
    return storeName;
  }

  public void setStoreName(String storeName) {
    this.storeName = storeName;
  }

  public String getD2Service() {
    return d2Service;
  }

  public void setD2Service(String d2Service) {
    this.d2Service = d2Service;
  }

  public String getServerD2Service() {
    return serverD2Service;
  }

  public void setServerD2Service(String serverD2Service) {
    this.serverD2Service = serverD2Service;
  }
}
