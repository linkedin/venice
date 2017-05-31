package com.linkedin.venice.client.store.transport;

import com.linkedin.venice.client.store.AvroGenericStoreClient;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;

/**
 * Define interfaces for TransportClient, and the target customer is the sub-classes of {@link AvroGenericStoreClient}
 */
public abstract class TransportClient implements Closeable {

  protected static final String HTTPS = "https";

  public abstract CompletableFuture<TransportClientResponse> get(String requestPath);

  public abstract CompletableFuture<TransportClientResponse> post(String requestPath, byte[] requestBody);

  /**
   * If the internal client could not be used by its callback function,
   * implementation of this function should return a new copy.
   * The default implementation is to return itself.
   * @return
   */
  public TransportClient getCopyIfNotUsableInCallback() {
    return this;
  }

  public static String ensureTrailingSlash(String input){
    if (input.endsWith("/")){
      return input;
    } else {
      return input + "/";
    }
  }
}
