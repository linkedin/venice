package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.stats.ClientStats;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.compute.ComputeRequestWrapper;
import com.linkedin.venice.schema.SchemaReader;
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

  // for testing
  public InternalAvroStoreClient<K, V> getInnerStoreClient() {
    return innerStoreClient;
  }

  @Override
  public SchemaReader getSchemaReader() {
    return innerStoreClient.getSchemaReader();
  }

  @Override
  public boolean isProjectionFieldValidationEnabled() {
    return innerStoreClient.isProjectionFieldValidationEnabled();
  }

  @Override
  public ComputeRequestBuilder<K> compute(
      Optional<ClientStats> stats,
      Optional<ClientStats> streamingStats,
      AvroGenericReadComputeStoreClient computeStoreClient,
      long preRequestTimeInNS) throws VeniceClientException {
    return innerStoreClient.compute(stats, streamingStats, computeStoreClient, preRequestTimeInNS);
  }

  @Override
  public void compute(
      ComputeRequestWrapper computeRequestWrapper,
      Set<K> keys,
      Schema resultSchema,
      StreamingCallback<K, ComputeGenericRecord> callback,
      long preRequestTimeInNS) throws VeniceClientException {
    innerStoreClient.compute(computeRequestWrapper, keys, resultSchema, callback, preRequestTimeInNS);
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
  @Deprecated
  public Schema getKeySchema() {
    return innerStoreClient.getKeySchema();
  }

  @Override
  @Deprecated
  public Schema getLatestValueSchema() {
    return innerStoreClient.getLatestValueSchema();
  }

  @Override
  public CompletableFuture<V> get(K key, Optional<ClientStats> stats, long preRequestTimeInNS)
      throws VeniceClientException {
    return innerStoreClient.get(key, stats, preRequestTimeInNS);
  }

  @Override
  public CompletableFuture<byte[]> getRaw(String requestPath, Optional<ClientStats> stats, long preRequestTimeInNS) {
    return innerStoreClient.getRaw(requestPath, stats, preRequestTimeInNS);
  }

  @Override
  public Executor getDeserializationExecutor() {
    return innerStoreClient.getDeserializationExecutor();
  }

  @Override
  public void computeWithKeyPrefixFilter(
      byte[] keyPrefix,
      ComputeRequestWrapper computeRequestWrapper,
      StreamingCallback<GenericRecord, GenericRecord> callback) {
    innerStoreClient.computeWithKeyPrefixFilter(keyPrefix, computeRequestWrapper, callback);
  }
}
