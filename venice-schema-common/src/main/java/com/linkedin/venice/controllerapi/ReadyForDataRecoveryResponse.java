package com.linkedin.venice.controllerapi;

import com.fasterxml.jackson.annotation.JsonIgnore;


public class ReadyForDataRecoveryResponse extends ControllerResponse {
  String reason;
  boolean ready;

  public String getReason() {
    return reason;
  }

  public void setReason(String reason) {
    this.reason = reason;
  }

  public boolean isReady() {
    return ready;
  }

  public void setReady(boolean ready) {
    this.ready = ready;
  }

  @JsonIgnore
  public String toString() {
    return ReadyForDataRecoveryResponse.class.getSimpleName() + "(ready: " + ready + ", reason: " + reason + ", super: "
        + super.toString() + ")";
  }
}
