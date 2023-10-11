package com.linkedin.venice.fastclient;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ComputeRequestBuilder;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.client.store.streaming.VeniceResponseMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;


/**
 * All the internal implementations of different tiers should extend this class.
 * This class adds in {@link RequestContext} object for the communication among different tiers.
 */

public abstract class InternalAvroStoreClient<K, V> implements AvroGenericStoreClient<K, V> {
  public CompletableFuture<V> get(K key) throws VeniceClientException {
    return get(new GetRequestContext(), key);
  }

  protected abstract CompletableFuture<V> get(GetRequestContext requestContext, K key) throws VeniceClientException;

  /**
   * Use StatsAvroGenericStoreClient to get a client instance.
   * @param keys
   * @return
   * @throws VeniceClientException
   */
  public CompletableFuture<Map<K, V>> batchGet(Set<K> keys) throws VeniceClientException {
    return batchGet(new BatchGetRequestContext<K, V>(), keys);
  }

  protected abstract CompletableFuture<Map<K, V>> batchGet(BatchGetRequestContext<K, V> requestContext, Set<K> keys)
      throws VeniceClientException;

  public void streamingBatchGet(Set<K> keys, StreamingCallback<K, V> callback) throws VeniceClientException {
    streamingBatchGet(new BatchGetRequestContext<K, V>(), keys, callback);
  }

  public CompletableFuture<VeniceResponseMap<K, V>> streamingBatchGet(Set<K> keys) throws VeniceClientException {
    return streamingBatchGet(new BatchGetRequestContext<K, V>(), keys);
  }

  protected abstract void streamingBatchGet(
      BatchGetRequestContext<K, V> requestContext,
      Set<K> keys,
      StreamingCallback<K, V> callback);

  protected abstract CompletableFuture<VeniceResponseMap<K, V>> streamingBatchGet(
      BatchGetRequestContext<K, V> requestContext,
      Set<K> keys);

  public ComputeRequestBuilder<K> compute() {
    throw new VeniceClientException("'compute' is not supported.");
  }
}
