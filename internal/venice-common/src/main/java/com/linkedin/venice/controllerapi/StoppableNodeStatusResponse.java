package com.linkedin.venice.controllerapi;

import java.util.List;
import java.util.Map;


public class StoppableNodeStatusResponse extends ControllerResponse {
  private List<String> stoppableInstances;
  private Map<String, String> nonStoppableInstancesStatusMap;

  public Map<String, String> getNonStoppableInstancesStatusMap() {
    return nonStoppableInstancesStatusMap;
  }

  public void setNonStoppableInstancesStatusMap(Map<String, String> nonStoppableInstancesStatusMap) {
    this.nonStoppableInstancesStatusMap = nonStoppableInstancesStatusMap;
  }

  public List<String> getStoppableInstances() {
    return stoppableInstances;
  }

  public void setStoppableInstances(List<String> remoableInstances) {
    this.stoppableInstances = remoableInstances;
  }
}
