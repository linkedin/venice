package com.linkedin.venice.listener.request;

import com.linkedin.venice.meta.Version;
import com.linkedin.venice.read.RequestType;

public abstract class RouterRequest {
  private final String resourceName;
  private final String storeName;

  public RouterRequest(String resourceName) {
    this.resourceName = resourceName;
    this.storeName = Version.parseStoreFromKafkaTopicName(resourceName);
  }

  public String getResourceName() {
    return resourceName;
  }

  public String getStoreName() {
    return storeName;
  }

  public abstract RequestType getRequestType();

  public abstract int getKeyCount();
}
