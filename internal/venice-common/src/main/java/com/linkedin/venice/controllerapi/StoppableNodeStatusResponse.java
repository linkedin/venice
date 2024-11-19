package com.linkedin.venice.controllerapi;

import java.util.Collections;
import java.util.List;
import java.util.Map;


public class StoppableNodeStatusResponse extends ControllerResponse {
  private List<String> stoppableInstances;
  private Map<String, String> nonStoppableInstancesWithReasons;

  public Map<String, String> getNonStoppableInstancesWithReasons() {
    if (nonStoppableInstancesWithReasons == null) {
      return Collections.emptyMap();
    }

    return nonStoppableInstancesWithReasons;
  }

  public void setNonStoppableInstancesWithReason(Map<String, String> nonStoppableInstancesWithReasons) {
    this.nonStoppableInstancesWithReasons = nonStoppableInstancesWithReasons;
  }

  public List<String> getStoppableInstances() {
    if (stoppableInstances == null) {
      return Collections.emptyList();
    }

    return stoppableInstances;
  }

  public void setStoppableInstances(List<String> stoppableInstances) {
    this.stoppableInstances = stoppableInstances;
  }
}
