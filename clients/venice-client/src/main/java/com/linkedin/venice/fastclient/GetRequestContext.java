package com.linkedin.venice.fastclient;

import com.linkedin.venice.read.RequestType;


public class GetRequestContext<K> extends RequestContext {
  int partitionId;
  /**
   * This field is used to store the request uri to the backend.
   */
  String requestUri;
  RetryContext retryContext; // initialize if needed for retry

  byte[] serializedKey;
  String route;
  K key;

  public GetRequestContext() {
    partitionId = -1;
    requestUri = null;
    retryContext = null;
  }

  @Override
  public RequestType getRequestType() {
    return RequestType.SINGLE_GET;
  }

  public void setKey(K key) {
    this.key = key;
  }

  public K getKey() {
    return key;
  }

  public void setSerializedKey(byte[] serializedKey) {
    this.serializedKey = serializedKey;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public void setPartitionId(int partitionId) {
    this.partitionId = partitionId;
  }

  public byte[] getSerializedKey() {
    return serializedKey;
  }

  public void setRoute(String route) {
    this.route = route;
  }

  public GetRequestContext<K> createRetryRequestContext() {
    GetRequestContext<K> retryRequestContext = new GetRequestContext<>();
    retryRequestContext.partitionId = partitionId;
    retryRequestContext.currentVersion = currentVersion;
    retryRequestContext.key = key;
    retryRequestContext.routeRequestMap = routeRequestMap;
    retryRequestContext.serializedKey = serializedKey;
    retryRequestContext.helixGroupId = this.helixGroupId;
    retryRequestContext.retryRequest = true;

    return retryRequestContext;
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
