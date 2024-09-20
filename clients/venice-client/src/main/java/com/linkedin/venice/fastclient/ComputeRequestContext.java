package com.linkedin.venice.fastclient;

import com.linkedin.venice.read.RequestType;


/**
 * Keep track of the progress of a compute request . This includes tracking
 * all the scatter requests and utilities to gather responses.
 * @param <K> Key type
 * @param <V> Value type
 */
public class ComputeRequestContext<K, V> extends MultiKeyRequestContext<K, V> {
  public ComputeRequestContext(int numKeysInRequest, boolean isPartialSuccessAllowed) {
    super(numKeysInRequest, isPartialSuccessAllowed);
  }

  @Override
  public RequestType getRequestType() {
    return RequestType.COMPUTE_STREAMING;
  }
}
