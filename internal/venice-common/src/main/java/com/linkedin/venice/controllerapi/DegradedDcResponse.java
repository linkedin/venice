package com.linkedin.venice.controllerapi;

import com.linkedin.venice.meta.DegradedDcInfo;
import java.util.Map;


public class DegradedDcResponse extends ControllerResponse {
  private Map<String, DegradedDcInfo> degradedDatacenters;

  public Map<String, DegradedDcInfo> getDegradedDatacenters() {
    return degradedDatacenters;
  }

  public void setDegradedDatacenters(Map<String, DegradedDcInfo> degradedDatacenters) {
    this.degradedDatacenters = degradedDatacenters;
  }
}
