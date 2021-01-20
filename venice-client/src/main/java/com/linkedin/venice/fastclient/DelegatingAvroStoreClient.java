package com.linkedin.venice.fastclient;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.avro.Schema;


/**
 * Inside Fast-Client, we choose to use n-tier architecture style to build a pipeline to separate different
 * types of logic in different layer.
 * We should follow this pattern if we don't have any strong concerns in the future development.
 */
public class DelegatingAvroStoreClient<K, V> extends InternalAvroStoreClient<K, V> {
  private final InternalAvroStoreClient<K, V> delegate;

  public DelegatingAvroStoreClient(InternalAvroStoreClient<K, V> delegate) {
    this.delegate = delegate;
  }

  @Override
  protected CompletableFuture<V> get(GetRequestContext requestContext, K key) throws VeniceClientException {
    return delegate.get(requestContext, key);
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
}
