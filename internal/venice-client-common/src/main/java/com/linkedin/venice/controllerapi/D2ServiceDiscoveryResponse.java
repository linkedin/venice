package com.linkedin.venice.controllerapi;

import com.fasterxml.jackson.annotation.JsonIgnore;


public class D2ServiceDiscoveryResponse extends ControllerResponse {
  String routerD2Service;
  String serverD2Service;

  public String getRouterD2Service() {
    return routerD2Service;
  }

  public void setRouterD2Service(String routerD2Service) {
    this.routerD2Service = routerD2Service;
  }

  public String getServerD2Service() {
    return serverD2Service;
  }

  public void setServerD2Service(String serverD2Service) {
    this.serverD2Service = serverD2Service;
  }

  @JsonIgnore
  public String toString() {
    return D2ServiceDiscoveryResponse.class.getSimpleName() + "(routerD2Service: " + routerD2Service
        + ", serverD2Service: " + serverD2Service + ", super: " + super.toString() + ")";
  }
}
