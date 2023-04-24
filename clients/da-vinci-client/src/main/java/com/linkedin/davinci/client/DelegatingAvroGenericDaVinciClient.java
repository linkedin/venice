package com.linkedin.davinci.client;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.stats.ClientStats;
import com.linkedin.venice.client.store.AvroGenericReadComputeStoreClient;
import com.linkedin.venice.client.store.ComputeGenericRecord;
import com.linkedin.venice.client.store.ComputeRequestBuilder;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.compute.ComputeRequestWrapper;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


/**
 * Delegating layer for {@link DaVinciClient}.
 */
public class DelegatingAvroGenericDaVinciClient<K, V>
    implements DaVinciClient<K, V>, AvroGenericReadComputeStoreClient<K, V> {
  private final AvroGenericDaVinciClient<K, V> delegate;

  public DelegatingAvroGenericDaVinciClient(AvroGenericDaVinciClient<K, V> delegate) {
    this.delegate = delegate;
  }

  @Override
  public CompletableFuture<Void> subscribeAll() {
    return delegate.subscribeAll();
  }

  @Override
  public CompletableFuture<Void> subscribe(Set<Integer> partitions) {
    return delegate.subscribe(partitions);
  }

  @Override
  public void unsubscribeAll() {
    delegate.unsubscribeAll();
  }

  @Override
  public void unsubscribe(Set<Integer> partitions) {
    delegate.unsubscribe(partitions);
  }

  @Override
  public int getPartitionCount() {
    return delegate.getPartitionCount();
  }

  @Override
  public CompletableFuture<V> get(K key) throws VeniceClientException {
    return delegate.get(key);
  }

  @Override
  public CompletableFuture<V> get(K key, V reusedValue) throws VeniceClientException {
    return delegate.get(key, reusedValue);
  }

  @Override
  public CompletableFuture<Map<K, V>> batchGet(Set<K> keys) throws VeniceClientException {
    return delegate.batchGet(keys);
  }

  @Override
  public void start() throws VeniceClientException {
    delegate.start();
  }

  @Override
  public void close() {
    delegate.close();
  }

  @Override
  public String getStoreName() {
    return delegate.getStoreName();
  }

  @Override
  public Schema getKeySchema() {
    return delegate.getKeySchema();
  }

  @Override
  public Schema getLatestValueSchema() {
    return delegate.getLatestValueSchema();
  }

  @Override
  public ComputeRequestBuilder<K> compute(
      Optional<ClientStats> stats,
      Optional<ClientStats> streamingStats,
      long preRequestTimeInNS) throws VeniceClientException {
    return delegate.compute(stats, streamingStats, preRequestTimeInNS);
  }

  @Override
  public void compute(
      ComputeRequestWrapper computeRequestWrapper,
      Set<K> keys,
      Schema resultSchema,
      StreamingCallback<K, ComputeGenericRecord> callback,
      long preRequestTimeInNS) throws VeniceClientException {
    delegate.compute(computeRequestWrapper, keys, resultSchema, callback, preRequestTimeInNS);
  }

  @Override
  public void computeWithKeyPrefixFilter(
      byte[] prefixBytes,
      ComputeRequestWrapper computeRequestWrapper,
      StreamingCallback<GenericRecord, GenericRecord> callback) {
    delegate.computeWithKeyPrefixFilter(prefixBytes, computeRequestWrapper, callback);
  }
}
