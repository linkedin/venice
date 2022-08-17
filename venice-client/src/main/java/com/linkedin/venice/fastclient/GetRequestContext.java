package com.linkedin.venice.fastclient;

public class GetRequestContext extends RequestContext {
  int partitionId = -1;
  /**
   * This field is used to store the request uri to the backend.
   */
  String requestUri = null;

  boolean longTailRetryRequestTriggered = false;

  boolean errorRetryRequestTriggered = false;

  boolean retryWin = false;
}
