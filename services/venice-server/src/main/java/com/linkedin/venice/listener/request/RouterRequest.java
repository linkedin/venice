package com.linkedin.venice.listener.request;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.streaming.StreamingUtils;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;


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
  private boolean isQuotaRejectedRequest;
  private HttpResponseStatus httpResponseStatus;
  private String errorMessage;
  private long arrivalTimeInNS;

  public RouterRequest(String resourceName, HttpRequest request) {
    this.isRetryRequest = containRetryHeader(request);
    this.isStreamingRequest = StreamingUtils.isStreamingEnabled(request);
    this.resourceName = resourceName;
    this.storeName = Version.parseStoreFromKafkaTopicName(resourceName);
  }

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

  private static boolean containRetryHeader(HttpRequest request) {
    return request.headers().contains(HttpConstants.VENICE_RETRY);
  }

  public boolean shouldRequestBeTerminatedEarly() {
    return requestTimeoutInNS != NO_REQUEST_TIMEOUT && System.nanoTime() > requestTimeoutInNS;
  }

  public void markAsQuotaRejectedRequest(HttpResponseStatus status, String errorMessage) {
    isQuotaRejectedRequest = true;
    httpResponseStatus = status;
    this.errorMessage = errorMessage;
  }

  public boolean isQuotaRejectedRequest() {
    return isQuotaRejectedRequest;
  }

  public HttpResponseStatus getQuotaRejectedResponseStatus() {
    return httpResponseStatus;
  }

  public String getQuotaRejectedErrorMessage() {
    return errorMessage;
  }

  public long getArrivalTimeInNS() {
    return arrivalTimeInNS;
  }

  public void setArrivalTimeInNS(long arrivalTimeInNS) {
    this.arrivalTimeInNS = arrivalTimeInNS;
  }
}
