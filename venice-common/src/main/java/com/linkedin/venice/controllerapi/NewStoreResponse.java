package com.linkedin.venice.controllerapi;

/**
 * Created by mwise on 5/5/16.
 */
public class NewStoreResponse extends ControllerResponse {
  String owner;

  public String getOwner() {
    return owner;
  }

  public void setOwner(String owner) {
    this.owner = owner;
  }
}
