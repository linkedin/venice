package com.linkedin.venice.fastclient;

import static com.linkedin.venice.fastclient.GetRequestContext.STORAGE_QUERY_ACTION;
import static com.linkedin.venice.fastclient.GetRequestContext.URI_SEPARATOR;
import static com.linkedin.venice.meta.QueryAction.STORAGE;

import com.linkedin.venice.meta.QueryAction;
import java.util.Objects;


/**
 * Keep track of the progress of a batch get request . This includes tracking
 * all the scatter requests and utilities to gather responses.
 * @param <K> Key type
 * @param <V> Value type
 */
public class BatchGetRequestContext<K, V> extends MultiKeyRequestContext<K, V> {
  public BatchGetRequestContext(int numKeysInRequest, boolean isPartialSuccessAllowed) {
    super(numKeysInRequest, isPartialSuccessAllowed);
  }

  /**
   * Compute the request URI for the batch get request. Result is cached so that it is computed only once.
   * Call this method only after setting the resourceName.
   * @return the request URI
   */
  @Override
  public String computeRequestUri() {
    if (requestUri != null) {
      return requestUri;
    }
    Objects.requireNonNull(resourceName, "Resource name must be set before calling this method");
    requestUri = URI_SEPARATOR + STORAGE_QUERY_ACTION + URI_SEPARATOR + resourceName;
    return requestUri;
  }

  @Override
  public QueryAction getQueryAction() {
    return STORAGE;
  }
}
