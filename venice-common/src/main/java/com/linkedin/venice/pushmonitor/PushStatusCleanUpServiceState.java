package com.linkedin.venice.pushmonitor;

public enum PushStatusCleanUpServiceState {
  RUNNING(1), STOPPED(0), FAILED(-1);

  private final int value;

  PushStatusCleanUpServiceState(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }
}
