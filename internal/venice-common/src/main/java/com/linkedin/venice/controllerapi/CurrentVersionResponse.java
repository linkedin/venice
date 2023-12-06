package com.linkedin.venice.controllerapi;

public class CurrentVersionResponse {
  private int currentVersion;

  public int getCurrentVersion() {
    return currentVersion;
  }

  public void setCurrentVersion(int currentVersion) {
    this.currentVersion = currentVersion;
  }
}
