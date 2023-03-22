package com.linkedin.venice.producer;

public class JobState {
  private boolean isActive;
  private boolean isSucceeded;

  public JobState(boolean isActive, boolean isSucceeded) {
    this.isActive = isActive;
    this.isSucceeded = isSucceeded;
  }

  public boolean isActive() {
    return isActive;
  }

  public void setActive(boolean active) {
    isActive = active;
  }

  public boolean isSucceeded() {
    return isSucceeded;
  }

  public void setSucceeded(boolean succeeded) {
    isSucceeded = succeeded;
  }
}
