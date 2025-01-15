package com.linkedin.venice.controllerapi;

import java.util.Map;


public class CleanExecutionIdsResponse extends ControllerResponse {
  private Map<String, Long> cleanedExecutionIds;
  private Map<String, Long> remainingExecutionIds;

  public void setCleanedExecutionIds(Map<String, Long> cleanedExecutionIds) {
    this.cleanedExecutionIds = cleanedExecutionIds;
  }

  public void setRemainingExecutionIds(Map<String, Long> remainingExecutionIds) {
    this.remainingExecutionIds = remainingExecutionIds;
  }

  public Map<String, Long> getCleanedExecutionIds() {
    return this.cleanedExecutionIds;
  }

  public Map<String, Long> getRemainingExecutionIds() {
    return this.remainingExecutionIds;
  }
}
