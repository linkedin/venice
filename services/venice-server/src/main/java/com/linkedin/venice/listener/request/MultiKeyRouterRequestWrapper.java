package com.linkedin.venice.listener.request;

import io.netty.handler.codec.http.HttpRequest;


/**
 * {@code MultiKeyRouterRequestWrapper} is a base class for a multi-key operation.
 */
public abstract class MultiKeyRouterRequestWrapper<K> extends RouterRequest {
  private final Iterable<K> keys;
  protected int keyCount = 0;

  protected MultiKeyRouterRequestWrapper(String resourceName, Iterable<K> keys, HttpRequest request) {
    super(resourceName, request);

    this.keys = keys;
    // TODO: looping through all keys at the beginning would prevent us from using lazy deserializer; refactor this
    this.keys.forEach(key -> ++keyCount);
  }

  protected MultiKeyRouterRequestWrapper(
      String resourceName,
      Iterable<K> keys,
      boolean isRetryRequest,
      boolean isStreamingRequest) {
    super(resourceName, isRetryRequest, isStreamingRequest);

    this.keys = keys;
    this.keys.forEach(key -> ++keyCount);
  }

  public Iterable<K> getKeys() {
    return this.keys;
  }

  @Override
  public int getKeyCount() {
    return this.keyCount;
  }

}
