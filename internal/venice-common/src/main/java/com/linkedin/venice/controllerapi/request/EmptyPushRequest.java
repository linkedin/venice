package com.linkedin.venice.controllerapi.request;

import java.util.Objects;


public class EmptyPushRequest extends ControllerRequest {
  private final String pushJobId;

  public EmptyPushRequest(String clusterName, String storeName, String pushJobId) {
    super(clusterName, storeName);
    this.pushJobId = Objects.requireNonNull(pushJobId, "pushJobId cannot be null");
  }

  public String getPushJobId() {
    return pushJobId;
  }
}
