package com.linkedin.venice.controllerapi;

/**
 * Created by mwise on 5/3/16.
 */
public class MultiVersionResponse
    extends TrackableControllerResponse { /* Uses Json Reflective Serializer, get without set may break things */
  private int[] versions;

  public int[] getVersions() {
    return versions;
  }

  public void setVersions(int[] versions) {
    this.versions = versions;
  }
}
