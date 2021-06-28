package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.stats.ClientStats;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.compute.ComputeRequestWrapper;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.avro.generic.GenericRecord;


/**
 * This class includes some necessary functions to deal with certain metric-handling activities that only
 * the client implementation can be aware of. These metrics cannot be tracked from a purely-external
 * perspective (i.e.: from the {@link com.linkedin.venice.client.store.StatTrackingStoreClient}'s point of view).
 *
 * It is intentional for these functions to not be part of {@link AvroGenericStoreClient}, so that the
 * end-user does not see these extra functions on the instances they get back from the
 * {@link com.linkedin.venice.client.store.ClientFactory}.
 */
public abstract class InternalAvroStoreClient<K, V> implements AvroGenericReadComputeStoreClient<K, V> {

  public CompletableFuture<byte[]> getRaw(String requestPath) {
    return getRaw(requestPath, Optional.empty(), 0);
  }

  @Override
  public CompletableFuture<V> get(K key) throws VeniceClientException {
    return get(key, Optional.empty(), 0);
  }

  @Override
  public CompletableFuture<Map<K, V>> batchGet(final Set<K> keys) throws VeniceClientException {
    return batchGet(keys, Optional.empty(), 0);
  }

  @Override
  public ComputeRequestBuilder<K> compute() throws VeniceClientException {
    return compute(Optional.empty(), Optional.empty(), 0);
  }

  public abstract CompletableFuture<V> get(final K key, final Optional<ClientStats> stats,
      final long preRequestTimeInNS) throws VeniceClientException;

  public abstract CompletableFuture<Map<K, V>> batchGet(final Set<K> keys, final Optional<ClientStats> stats,
      final long preRequestTimeInNS) throws VeniceClientException;

  public abstract CompletableFuture<byte[]> getRaw(final String requestPath, final Optional<ClientStats> stats,
      final long preRequestTimeInNS);

  // The following function allows to pass one compute store client
  public abstract ComputeRequestBuilder<K> compute(final Optional<ClientStats> stats, final Optional<ClientStats> streamingStats,
      final InternalAvroStoreClient computeStoreClient, final long preRequestTimeInNS) throws VeniceClientException;

  public Executor getDeserializationExecutor() {
    throw new VeniceClientException("getDeserializationExecutor is not supported!");
  }

  @Override
  public void computeWithKeyPrefixFilter(byte[] prefixBytes, ComputeRequestWrapper computeRequestWrapper, StreamingCallback<K,
      GenericRecord>callback) {
    throw new VeniceClientException("ComputeWithKeyPrefixFilter is not supported by Venice Avro Store Client");
  }
}
