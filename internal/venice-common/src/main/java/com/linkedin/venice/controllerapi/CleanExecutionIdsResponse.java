package com.linkedin.venice.controllerapi;

import java.util.Map;


public class CleanExecutionIdsResponse extends ControllerResponse {
  private Map<String, Long> cleanedExecutionIds;

  public void setCleanedExecutionIds(Map<String, Long> cleanedExecutionIds) {
    this.cleanedExecutionIds = cleanedExecutionIds;
  }

  public Map<String, Long> getCleanedExecutionIds() {
    return this.cleanedExecutionIds;
  }
}
