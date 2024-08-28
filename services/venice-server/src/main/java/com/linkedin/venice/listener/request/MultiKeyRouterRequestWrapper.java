package com.linkedin.venice.listener.request;

import io.netty.handler.codec.http.HttpRequest;
import java.util.List;


/**
 * {@code MultiKeyRouterRequestWrapper} is a base class for a multi-key operation.
 */
public abstract class MultiKeyRouterRequestWrapper<K> extends RouterRequest {
  private final List<K> keys;

  protected MultiKeyRouterRequestWrapper(String resourceName, List<K> keys, HttpRequest request) {
    super(resourceName, request);
    this.keys = keys;
  }

  protected MultiKeyRouterRequestWrapper(
      String resourceName,
      List<K> keys,
      boolean isRetryRequest,
      boolean isStreamingRequest) {
    super(resourceName, isRetryRequest, isStreamingRequest);
    this.keys = keys;
  }

  public List<K> getKeys() {
    return this.keys;
  }

  @Override
  public int getKeyCount() {
    return this.keys.size();
  }

}
