package com.linkedin.venice.listener.request;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.read.RequestType;
import io.netty.handler.codec.http.HttpRequest;


public abstract class RouterRequest {
  private final boolean isRetryRequest;
  private final String resourceName;
  private final String storeName;

  public RouterRequest(String resourceName, HttpRequest request) {
    this.isRetryRequest = containRetryHeader(request);
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

  public boolean isRetryRequest() {
    return isRetryRequest;
  }

  private static boolean containRetryHeader(HttpRequest request) {
    return request.headers().contains(HttpConstants.VENICE_RETRY);
  }
}
