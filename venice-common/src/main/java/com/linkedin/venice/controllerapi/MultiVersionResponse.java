package com.linkedin.venice.controllerapi;

/**
 * Created by mwise on 5/3/16.
 */
public class MultiVersionResponse extends ControllerResponse {
  private int[] versions;

  public int[] getVersions() {
    return versions;
  }

  public void setVersions(int[] versions) {
    this.versions = versions;
  }
}
