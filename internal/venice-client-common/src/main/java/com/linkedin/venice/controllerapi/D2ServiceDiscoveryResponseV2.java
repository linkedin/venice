package com.linkedin.venice.controllerapi;

import com.fasterxml.jackson.annotation.JsonIgnore;


@Deprecated
public class D2ServiceDiscoveryResponseV2 extends D2ServiceDiscoveryResponse {
  @Deprecated
  public static final String D2_SERVICE_DISCOVERY_RESPONSE_V2_ENABLED = "d2.service.discovery.response.v2.enabled";

  @JsonIgnore
  public String toString() {
    return D2ServiceDiscoveryResponseV2.class.getSimpleName() + "(super: " + super.toString() + ")";
  }
}
