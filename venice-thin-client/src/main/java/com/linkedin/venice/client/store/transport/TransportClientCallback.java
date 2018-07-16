package com.linkedin.venice.client.store.transport;

import com.linkedin.venice.client.exceptions.VeniceClientHttpException;
import org.apache.http.HttpStatus;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

/**
 * Define the common functions for call back of {@link TransportClient}
 */
public class TransportClientCallback {
  public static String HEADER_VENICE_SCHEMA_ID = "X-VENICE-SCHEMA-ID";

  private final CompletableFuture<TransportClientResponse> valueFuture;

  public TransportClientCallback(CompletableFuture<TransportClientResponse> valueFuture) {
    this.valueFuture = valueFuture;
  }

  protected CompletableFuture<TransportClientResponse> getValueFuture() {
    return valueFuture;
  }

  public void completeFuture(int statusCode,
                             byte[] body,
      int schemaId) {
    if (statusCode == HttpStatus.SC_OK) {
      valueFuture.complete(new TransportClientResponse(schemaId, body));
    } else if (statusCode == HttpStatus.SC_NOT_FOUND) {
      valueFuture.complete(null);
    } else {
      /**
       * Only convert body from `byte[]` to `String` when necessary since it is quite expensive.
       */
      String msg = new String(body, StandardCharsets.UTF_8);
      valueFuture.completeExceptionally(new VeniceClientHttpException(msg, statusCode));
    }
  }
}
