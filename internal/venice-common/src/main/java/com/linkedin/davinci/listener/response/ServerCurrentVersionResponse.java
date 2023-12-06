package com.linkedin.davinci.listener.response;

public class ServerCurrentVersionResponse {
  private boolean isError;
  private int currentVersion;

  private String message;

  public ServerCurrentVersionResponse() {
  }

  public void setCurrentVersion(int currentVersion) {
    this.currentVersion = currentVersion;
  }

  public void setError(boolean error) {
    this.isError = error;
  }

  public boolean isError() {
    return this.isError;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public String getMessage() {
    return this.message;
  }

  public int getCurrentVersion() {
    return currentVersion;
  }

}
