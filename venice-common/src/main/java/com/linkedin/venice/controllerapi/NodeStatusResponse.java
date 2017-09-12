package com.linkedin.venice.controllerapi;

/**
 * Response to represent the status of a node
 */
public class NodeStatusResponse extends ControllerResponse {
  private boolean isRemovable;

  private String details;

  public boolean isRemovable() {
    return isRemovable;
  }

  public void setRemovable(boolean removable) {
    isRemovable = removable;
  }

  public String getDetails() {
    return details;
  }

  public void setDetails(String details) {
    this.details = details;
  }
}
