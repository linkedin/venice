package com.linkedin.venice.controllerapi;

import java.util.List;
import java.util.Map;


public class StoppableNodeStatusResponse extends ControllerResponse {
  private List<String> stoppableInstances;
  private Map<String, String> nonStoppableInstancesWithReasons;

  public Map<String, String> getNonStoppableInstances() {
    return nonStoppableInstancesWithReasons;
  }

  public void setNonStoppableInstancesWithReason(Map<String, String> nonStoppableInstancesWithReasons) {
    this.nonStoppableInstancesWithReasons = nonStoppableInstancesWithReasons;
  }

  public List<String> getStoppableInstances() {
    return stoppableInstances;
  }

  public void setStoppableInstances(List<String> remoableInstances) {
    this.stoppableInstances = remoableInstances;
  }
}
