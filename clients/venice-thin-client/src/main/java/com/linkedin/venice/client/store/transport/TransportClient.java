package com.linkedin.venice.client.store.transport;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import java.io.Closeable;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;


/**
 * Define interfaces for TransportClient, and the target customer is the sub-classes of {@link AvroGenericStoreClient}
 */
public abstract class TransportClient implements Closeable {
  protected static final String HTTPS = "https";

  public CompletableFuture<TransportClientResponse> get(String requestPath) {
    return get(requestPath, Collections.EMPTY_MAP);
  }

  public CompletableFuture<TransportClientResponse> post(String requestPath, byte[] requestBody) {
    return post(requestPath, Collections.EMPTY_MAP, requestBody);
  }

  public abstract CompletableFuture<TransportClientResponse> get(String requestPath, Map<String, String> headers);

  public abstract CompletableFuture<TransportClientResponse> post(
      String requestPath,
      Map<String, String> headers,
      byte[] requestBody);

  public abstract void streamPost(
      String requestPath,
      Map<String, String> headers,
      byte[] requestBody,
      TransportClientStreamingCallback callback,
      int keyCount);
}
