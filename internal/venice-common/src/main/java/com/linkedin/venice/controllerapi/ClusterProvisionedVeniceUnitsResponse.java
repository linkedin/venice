package com.linkedin.venice.controllerapi;

public class ClusterProvisionedVeniceUnitsResponse extends ControllerResponse {
  private long provisionedVeniceUnits;

  public long getProvisionedVeniceUnits() {
    return provisionedVeniceUnits;
  }

  public void setProvisionedVeniceUnits(long provisionedVeniceUnits) {
    this.provisionedVeniceUnits = provisionedVeniceUnits;
  }
}
