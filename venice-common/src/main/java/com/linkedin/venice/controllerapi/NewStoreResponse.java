package com.linkedin.venice.controllerapi;

/**
 * Created by mwise on 5/5/16.
 */
public class NewStoreResponse extends ControllerResponse { /* Uses Json Reflective Serializer, get without set may break things */
  String owner;

  public String getOwner() {
    return owner;
  }

  public void setOwner(String owner) {
    this.owner = owner;
  }
}
