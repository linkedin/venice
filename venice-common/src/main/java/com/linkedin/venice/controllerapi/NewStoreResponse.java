package com.linkedin.venice.controllerapi;

public class NewStoreResponse
    extends ControllerResponse { /* Uses Json Reflective Serializer, get without set may break things */
  String owner;

  public String getOwner() {
    return owner;
  }

  public void setOwner(String owner) {
    this.owner = owner;
  }
}
