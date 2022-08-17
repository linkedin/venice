package com.linkedin.venice.controllerapi;

import com.fasterxml.jackson.annotation.JsonIgnore;


public class D2ServiceDiscoveryResponse extends ControllerResponse {
  String d2Service;

  public String getD2Service() {
    return d2Service;
  }

  public void setD2Service(String d2Service) {
    this.d2Service = d2Service;
  }

  @JsonIgnore
  public String toString() {
    return D2ServiceDiscoveryResponse.class.getSimpleName() + "(d2service: " + d2Service + ", super: "
        + super.toString() + ")";
  }
}
