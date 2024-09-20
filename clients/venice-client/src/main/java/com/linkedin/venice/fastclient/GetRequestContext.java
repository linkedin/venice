package com.linkedin.venice.fastclient;

import com.linkedin.venice.read.RequestType;


public class GetRequestContext extends RequestContext {
  int partitionId;
  /**
   * This field is used to store the request uri to the backend.
   */
  String requestUri;
  RetryContext retryContext; // initialize if needed for retry

  GetRequestContext() {
    partitionId = -1;
    requestUri = null;
    retryContext = null;
  }

  @Override
  public RequestType getRequestType() {
    return RequestType.SINGLE_GET;
  }

  static class RetryContext {
    boolean longTailRetryRequestTriggered;
    boolean errorRetryRequestTriggered;

    // TODO Explore whether adding a new boolean named originalWin to properly differentiate and
    // maybe add more strict tests around these 2 flags will be helpful.
    boolean retryWin;

    RetryContext() {
      longTailRetryRequestTriggered = false;
      errorRetryRequestTriggered = false;
      retryWin = false;
    }
  }
}
