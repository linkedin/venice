package com.linkedin.venice.controllerapi;

public class MasterControllerResponse extends ControllerResponse {
  private String cluster;
  private String url;

  public String getCluster() {
    return cluster;
  }

  public void setCluster(String cluster) {
    this.cluster = cluster;
  }

  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }
}
