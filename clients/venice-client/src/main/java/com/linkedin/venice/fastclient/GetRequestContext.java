package com.linkedin.venice.fastclient;

public class GetRequestContext extends RequestContext {
  int partitionId;
  /**
   * This field is used to store the request uri to the backend.
   */
  String requestUri;

  boolean longTailRetryRequestTriggered;

  boolean errorRetryRequestTriggered;

  // TODO Explore whether adding a new boolean named originalWin to properly differentiate and
  // maybe add more strict tests around these 2 flags will be helpful.
  boolean retryWin;

  GetRequestContext() {
    partitionId = -1;
    requestUri = null;
    longTailRetryRequestTriggered = false;
    errorRetryRequestTriggered = false;
    retryWin = false;
  }
}
