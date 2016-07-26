package com.linkedin.venice.client.store.transport;

import com.linkedin.venice.client.store.DeserializerFetcher;

import java.io.Closeable;
import java.util.concurrent.Future;

/**
 * Define interfaces for TransportClient, and the target customer is the sub-classes of {@link com.linkedin.venice.client.store.AvroGenericStoreClient}
 * @param <V>
 */
public abstract class TransportClient<V> implements Closeable {
  private DeserializerFetcher<V> deserializerFetcher;

  public abstract Future<V> get(String requestPath);

  public abstract Future<byte[]> getRaw(String requestPath);

  /**
   * If the internal client could not be used by its callback function,
   * implementation of this function should return a new copy.
   * The default implementation is to return itself.
   * @return
   */
  public TransportClient<V> getCopyIfNotUsableInCallback() {
    return this;
  }

  public void setDeserializerFetcher(DeserializerFetcher<V> fetcher) {
    this.deserializerFetcher = fetcher;
  }

  protected DeserializerFetcher<V> getDeserializerFetcher() {
    return deserializerFetcher;
  }
}
