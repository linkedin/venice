package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;


/**
 * @param <K> The store's key type
 * @param <R> The response type returned by this kind of query
 */
public interface ExecutableRequestBuilder<K, R> {
  /**
   * Send compute request to Venice, and this should be the last step of the compute specification.
   * @param keys : keys for the candidate records
   * @return
   * @throws VeniceClientException
   */
  CompletableFuture<R> execute(Set<K> keys) throws VeniceClientException;
}
