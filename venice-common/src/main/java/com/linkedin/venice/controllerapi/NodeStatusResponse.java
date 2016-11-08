package com.linkedin.venice.controllerapi;

/**
 * Response to represent the status of a node
 */
public class NodeStatusResponse extends ControllerResponse {
  private boolean isRemovable;

  public boolean isRemovable() {
    return isRemovable;
  }

  public void setRemovable(boolean removable) {
    isRemovable = removable;
  }
}
