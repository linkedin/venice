package com.linkedin.venice.routerapi;

import com.linkedin.venice.controllerapi.ControllerResponse;
import java.util.List;


public class BlobDiscoveryResponse extends ControllerResponse {
  private List<String> liveNodeHostNames;

  public void setLiveNodeNames(List<String> liveNodeHostNames) {
    this.liveNodeHostNames = liveNodeHostNames;
  }

  public List<String> getLiveNodeHostNames() {
    return liveNodeHostNames;
  }
}
