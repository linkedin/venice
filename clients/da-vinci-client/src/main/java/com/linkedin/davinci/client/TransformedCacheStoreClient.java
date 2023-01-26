package com.linkedin.davinci.client;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.avro.Schema;


/**
 * This class exposes a view of cache when customized transformation is enabled.
 */
public class TransformedCacheStoreClient<K, T> implements AvroGenericStoreClient<K, T> {
  private final AvroGenericDaVinciClient daVinciClient;

  public TransformedCacheStoreClient(AvroGenericDaVinciClient daVinciClient) {
    this.daVinciClient = daVinciClient;
  }

  @Override
  public CompletableFuture<T> get(K key) throws VeniceClientException {
    return null;
  }

  @Override
  public CompletableFuture<Map<K, T>> batchGet(Set<K> keys) throws VeniceClientException {
    return null;
  }

  @Override
  public void start() throws VeniceClientException {
    // do nothing
  }

  @Override
  public void close() {
    // do nothing
  }

  @Override
  public String getStoreName() {
    return daVinciClient.getStoreName();
  }

  @Override
  public Schema getKeySchema() {
    return daVinciClient.getKeySchema();
  }

  @Override
  public Schema getLatestValueSchema() {
    return daVinciClient.getLatestValueSchema();
  }
}
