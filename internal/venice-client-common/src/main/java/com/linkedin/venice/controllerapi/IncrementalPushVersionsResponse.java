package com.linkedin.venice.controllerapi;

import java.util.Set;


public class IncrementalPushVersionsResponse extends ControllerResponse {
  private Set<String> incrementalPushVersions;

  public void setIncrementalPushVersions(Set<String> incrementalPushVersions) {
    this.incrementalPushVersions = incrementalPushVersions;
  }

  public Set<String> getIncrementalPushVersions() {
    return incrementalPushVersions;
  }
}
