package com.linkedin.venice.controllerapi;

public class SystemStoreHeartbeatResponse extends ControllerResponse {
  private long heartbeatTimestamp;

  public long getHeartbeatTimestamp() {
    return heartbeatTimestamp;
  }

  public void setHeartbeatTimestamp(long heartbeatTimestamp) {
    this.heartbeatTimestamp = heartbeatTimestamp;
  }
}
