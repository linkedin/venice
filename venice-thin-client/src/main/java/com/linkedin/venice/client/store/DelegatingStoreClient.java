package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;

import com.linkedin.venice.client.stats.ClientStats;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.compute.protocol.request.ComputeRequestV1;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


public class DelegatingStoreClient<K, V> extends InternalAvroStoreClient<K, V> {
  private final InternalAvroStoreClient<K, V> innerStoreClient;

  public DelegatingStoreClient(InternalAvroStoreClient<K, V> innerStoreClient) {
    this.innerStoreClient = innerStoreClient;
  }

  @Override
  public CompletableFuture<V> get(K key) throws VeniceClientException {
    return innerStoreClient.get(key);
  }

  @Override
  public CompletableFuture<V> get(K key, Optional<ClientStats> stats, long preRequestTimeInNS) throws VeniceClientException {
    return innerStoreClient.get(key, stats, preRequestTimeInNS);
  }

  @Override
  public CompletableFuture<byte[]> getRaw(String requestPath) {
    return innerStoreClient.getRaw(requestPath);
  }

  @Override
  public CompletableFuture<byte[]> getRaw(String requestPath, Optional<ClientStats> stats, long preRequestTimeInNS) {
    return innerStoreClient.getRaw(requestPath, stats, preRequestTimeInNS);
  }

  @Override
  public ComputeRequestBuilder<K> compute() {
    return innerStoreClient.compute();
  }

  @Override
  public ComputeRequestBuilder<K> compute(Optional<ClientStats> stats, Optional<ClientStats> streamingStats, long preRequestTimeInNS)
      throws VeniceClientException {
    return innerStoreClient.compute(stats, streamingStats, preRequestTimeInNS);
  }

  @Override
  public ComputeRequestBuilder<K> compute(Optional<ClientStats> stats, Optional<ClientStats> streamingStats,
      InternalAvroStoreClient computeStoreClient, long preRequestTimeInNS) throws VeniceClientException {
    return innerStoreClient.compute(stats, streamingStats, computeStoreClient, preRequestTimeInNS);
  }

  @Override
  public CompletableFuture<Map<K, GenericRecord>> compute(ComputeRequestV1 computeRequest, Set<K> keys,
      Schema resultSchema, Optional<ClientStats> stats, long preRequestTimeInNS) throws VeniceClientException {
    return innerStoreClient.compute(computeRequest, keys, resultSchema, stats, preRequestTimeInNS);
  }

  @Override
  public void compute(ComputeRequestV1 computeRequest, Set<K> keys, Schema resultSchema,
      StreamingCallback<K, GenericRecord> callback, final long preRequestTimeInNS) throws VeniceClientException {
    innerStoreClient.compute(computeRequest, keys, resultSchema, callback, preRequestTimeInNS);
  }

  @Override
  public CompletableFuture<Map<K, V>> batchGet(Set<K> keys) throws VeniceClientException {
    return innerStoreClient.batchGet(keys);
  }

  @Override
  public void streamingBatchGet(Set<K> keys, StreamingCallback<K, V> callback) throws VeniceClientException {
    innerStoreClient.streamingBatchGet(keys, callback);
  }

  @Override
  public CompletableFuture<Map<K, V>> batchGet(Set<K> keys, Optional<ClientStats> stats, long preRequestTimeInNS) throws VeniceClientException {
    return innerStoreClient.batchGet(keys, stats, preRequestTimeInNS);
  }

  @Override
  public void start() throws VeniceClientException {
    innerStoreClient.start();
  }

  @Override
  public void close() {
    innerStoreClient.close();
  }

  @Override
  public String getStoreName() {
    return innerStoreClient.getStoreName();
  }

  @Override
  public Schema getKeySchema() {
    return innerStoreClient.getKeySchema();
  }

  @Override
  public Schema getLatestValueSchema() {
    return innerStoreClient.getLatestValueSchema();
  }

  @Override
  public Executor getDeserializationExecutor() {
    return innerStoreClient.getDeserializationExecutor();
  }

  // for testing
  public InternalAvroStoreClient<K, V> getInnerStoreClient() {
    return this.innerStoreClient;
  }
}
