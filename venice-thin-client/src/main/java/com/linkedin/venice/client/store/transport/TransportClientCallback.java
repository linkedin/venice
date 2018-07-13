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
    String msg;

    if (statusCode == HttpStatus.SC_OK) {
      valueFuture.complete(new TransportClientResponse(schemaId, body));
    } else {
      switch (statusCode) {
        case HttpStatus.SC_NOT_FOUND:
          valueFuture.complete(null);
          break;
        case HttpStatus.SC_INTERNAL_SERVER_ERROR:
        case HttpStatus.SC_SERVICE_UNAVAILABLE:
          msg = new String(body, StandardCharsets.UTF_8);
          if (msg != null) {
            valueFuture.completeExceptionally(new VeniceClientHttpException(msg, statusCode));
          } else {
            valueFuture.completeExceptionally(new VeniceClientHttpException(statusCode));
          }
          break;
        case HttpStatus.SC_BAD_REQUEST:
        default:
          msg = new String(body, StandardCharsets.UTF_8);
          if (msg != null) {
            valueFuture.completeExceptionally(new VeniceClientHttpException(msg, statusCode));
          } else {
            valueFuture.completeExceptionally(
                new VeniceClientHttpException("Router responds with status code: " + statusCode, statusCode));
          }
      }
    }
  }
}
