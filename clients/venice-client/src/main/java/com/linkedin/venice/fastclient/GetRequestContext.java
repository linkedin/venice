package com.linkedin.venice.fastclient;

public class GetRequestContext extends RequestContext {
  int partitionId;
  /**
   * This field is used to store the request uri to the backend.
   */
  String requestUri;

  boolean longTailRetryRequestTriggered;

  boolean errorRetryRequestTriggered;

  boolean retryWin;

  GetRequestContext() {
    partitionId = -1;
    requestUri = null;
    longTailRetryRequestTriggered = false;
    errorRetryRequestTriggered = false;
    retryWin = false;
  }
}
