package com.linkedin.venice.controllerapi;

import com.linkedin.venice.meta.RoutersClusterConfig;


public class RoutersClusterConfigResponse extends ControllerResponse {
  private RoutersClusterConfig config;

  public RoutersClusterConfig getConfig() {
    return config;
  }

  public void setConfig(RoutersClusterConfig config) {
    this.config = config;
  }
}
