package com.linkedin.venice.controller;

import java.util.List;
import java.util.Map;


public class InstanceRemovableStatuses {
  private List<String> stoppableInstances;
  private Map<String, String> nonStoppableInstancesWithReasons;

  private String redirectUrl;

  public void setRedirectUrl(String redirectUrl) {
    this.redirectUrl = redirectUrl;
  }

  public String getRedirectUrl() {
    return redirectUrl;
  }

  public Map<String, String> getNonStoppableInstances() {
    return nonStoppableInstancesWithReasons;
  }

  public void setNonStoppableInstances(Map<String, String> nonStoppableInstancesWithReasons) {
    this.nonStoppableInstancesWithReasons = nonStoppableInstancesWithReasons;
  }

  public List<String> getStoppableInstances() {
    return stoppableInstances;
  }

  public void setStoppableInstances(List<String> remoableInstances) {
    this.stoppableInstances = remoableInstances;
  }

  public enum NonStoppableReason {
    WILL_LOSE_DATA, MIN_ACTIVE_REPLICA_VIOLATION, ONGOING_MAINTENANCE, UNKNOWN_INSTANCE;
  }
}
