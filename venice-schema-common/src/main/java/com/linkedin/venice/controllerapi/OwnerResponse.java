package com.linkedin.venice.controllerapi;

import org.codehaus.jackson.annotate.JsonIgnore;


public class OwnerResponse extends ControllerResponse { /* Uses Json Reflective Serializer, get without set may break things */
  String owner;

  public String getOwner() {
    return owner;
  }

  public void setOwner(String owner) {
    this.owner = owner;
  }

  @JsonIgnore
  public String toString() {
    return OwnerResponse.class.getSimpleName() + "(owner: " + owner +
        ", super: " + super.toString() + ")";
  }
}
