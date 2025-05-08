package com.linkedin.venice.controllerapi;

import com.linkedin.venice.stats.dimensions.VeniceDimensionInterface;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;


public enum ControllerEndpoints implements VeniceDimensionInterface {
  REPUSH_STORE(ControllerRoute.REPUSH_STORE.getPath());

  private final String endpoint;

  ControllerEndpoints(String endpoint) {
    this.endpoint = endpoint;
  }

  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.VENICE_CONTROLLER_ENDPOINT;
  }

  @Override
  public String getDimensionValue() {
    return this.endpoint;
  }
}
