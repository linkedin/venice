package com.linkedin.venice.listener.request;

import com.linkedin.venice.meta.Version;
import com.linkedin.venice.read.RequestType;


/**
 * {@code RouterRequest} is an abstract base class for single-get and multi-get operations.
 * @see GetRouterRequest
 * @see MultiGetRouterRequestWrapper
 */
public abstract class RouterRequest {
  // Early request termination is not enabled.
  public static final long NO_REQUEST_TIMEOUT = -1;

  private long requestTimeoutInNS = NO_REQUEST_TIMEOUT;
  private final boolean isRetryRequest;
  private final String resourceName;
  private final String storeName;
  private final boolean isStreamingRequest;

  public RouterRequest(String resourceName, boolean isRetryRequest, boolean isStreamingRequest) {
    this.resourceName = resourceName;
    this.storeName = Version.parseStoreFromKafkaTopicName(resourceName);
    this.isRetryRequest = isRetryRequest;
    this.isStreamingRequest = isStreamingRequest;
  }

  public void setRequestTimeoutInNS(long requestTimeoutInNS) {
    this.requestTimeoutInNS = requestTimeoutInNS;
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

  public boolean isStreamingRequest() {
    return isStreamingRequest;
  }

  public boolean shouldRequestBeTerminatedEarly() {
    return requestTimeoutInNS != NO_REQUEST_TIMEOUT && System.nanoTime() > requestTimeoutInNS;
  }
}
