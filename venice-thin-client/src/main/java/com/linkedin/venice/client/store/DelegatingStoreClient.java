package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

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
  public CompletableFuture<byte[]> getRaw(String requestPath) {
    return innerStoreClient.getRaw(requestPath);
  }

  @Override
  public CompletableFuture<Map<K, V>> multiGet(Set<K> keys) throws VeniceClientException {
    return innerStoreClient.multiGet(keys);
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

  // for testing
  public InternalAvroStoreClient<K, V> getInnerStoreClient() {
    return this.innerStoreClient;
  }
}
