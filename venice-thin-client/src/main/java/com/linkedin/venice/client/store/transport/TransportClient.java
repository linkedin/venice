package com.linkedin.venice.client.store.transport;

import com.linkedin.venice.client.store.ClientCallback;
import com.linkedin.venice.client.store.DeserializerFetcher;
import com.linkedin.venice.client.store.AvroGenericStoreClient;

import java.io.Closeable;
import java.util.concurrent.Future;

/**
 * Define interfaces for TransportClient, and the target customer is the sub-classes of {@link AvroGenericStoreClient}
 * @param <V>
 */
public abstract class TransportClient<V> implements Closeable {
  private DeserializerFetcher<V> deserializerFetcher;

  public abstract Future<V> get(String requestPath, ClientCallback callback);

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

  public static String ensureTrailingSlash(String input){
    if (input.endsWith("/")){
      return input;
    } else {
      return input + "/";
    }
  }
}
