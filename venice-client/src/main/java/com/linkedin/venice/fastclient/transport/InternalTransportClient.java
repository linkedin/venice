package com.linkedin.venice.fastclient.transport;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.client.store.transport.TransportClientStreamingCallback;
import java.util.Map;
import java.util.concurrent.CompletableFuture;


/**
 * Abstract class for Fast-Client transport layer implementation, and this layer is mostly decided which methods
 * need to be implemented at different stages.
 * Eventually this class can be deleted once all the methods defined in {@link TransportClient} are required in Fast-Client.
 */
public abstract class InternalTransportClient extends TransportClient {
  public CompletableFuture<TransportClientResponse> post(String requestPath, Map<String, String> headers,
      byte[] requestBody, int keyCount) {
    throw new VeniceClientException("'post' is not supported.");
  }

  public void streamPost(String requestPath, Map<String, String> headers, byte[] requestBody,
      TransportClientStreamingCallback callback, int keyCount) {
    throw new VeniceClientException("'streamPost' is not supported.");
  }
}
