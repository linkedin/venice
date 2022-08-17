package com.linkedin.venice.controllerapi;

import com.fasterxml.jackson.annotation.JsonIgnore;


public class VersionResponse
    extends ControllerResponse { /* Uses Json Reflective Serializer, get without set may break things */
  private int version;

  public int getVersion() {
    return version;
  }

  public void setVersion(int version) {
    this.version = version;
  }

  @JsonIgnore
  public String toString() {
    return VersionResponse.class.getSimpleName() + "(version: " + version + ", super: " + super.toString() + ")";
  }
}
