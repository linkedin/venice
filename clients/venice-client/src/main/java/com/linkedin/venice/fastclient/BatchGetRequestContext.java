package com.linkedin.venice.fastclient;

/**
 * Keep track of the progress of a batch get request . This includes tracking
 * all the scatter requests and utilities to gather responses.
 * @param <K> Key type
 * @param <V> Value type
 */
public class BatchGetRequestContext<K, V> extends MultiKeyRequestContext<K, V> {
  public BatchGetRequestContext(boolean isPartialSuccessAllowed) {
    super(isPartialSuccessAllowed);
  }
}
