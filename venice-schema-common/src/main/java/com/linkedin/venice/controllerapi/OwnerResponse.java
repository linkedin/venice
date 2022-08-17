package com.linkedin.venice.controllerapi;

import com.fasterxml.jackson.annotation.JsonIgnore;


public class OwnerResponse
    extends ControllerResponse { /* Uses Json Reflective Serializer, get without set may break things */
  String owner;

  public String getOwner() {
    return owner;
  }

  public void setOwner(String owner) {
    this.owner = owner;
  }

  @JsonIgnore
  public String toString() {
    return OwnerResponse.class.getSimpleName() + "(owner: " + owner + ", super: " + super.toString() + ")";
  }
}
