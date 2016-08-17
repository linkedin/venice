package com.linkedin.venice.controllerapi;

/**
 * Created by mwise on 5/3/16.
 */
public class VersionResponse extends ControllerResponse { /* Uses Json Reflective Serializer, get without set may break things */
  private int version;

  public int getVersion() {
    return version;
  }

  public void setVersion(int version) {
    this.version = version;
  }
}
