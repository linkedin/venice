package com.linkedin.venice.client.store.transport;

import com.linkedin.venice.client.exceptions.VeniceClientHttpException;
import com.linkedin.venice.client.exceptions.VeniceClientRateExceededException;
import com.linkedin.venice.compression.CompressionStrategy;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import org.apache.http.HttpStatus;


/**
 * Define the common functions for call back of {@link TransportClient}
 */
public class TransportClientCallback {
  private final CompletableFuture<TransportClientResponse> valueFuture;

  public TransportClientCallback(CompletableFuture<TransportClientResponse> valueFuture) {
    this.valueFuture = valueFuture;
  }

  protected CompletableFuture<TransportClientResponse> getValueFuture() {
    return valueFuture;
  }

  public void completeFuture(int statusCode, int schemaId, CompressionStrategy compressionStrategy, byte[] body) {
    if ((statusCode < 300 && statusCode >= 200)) {
      valueFuture.complete(new TransportClientResponse(schemaId, compressionStrategy, body));
    } else if (statusCode == HttpStatus.SC_NOT_FOUND) {
      valueFuture.complete(null);
    } else {
      // Only convert body from `byte[]` to `String` when necessary since it is quite expensive
      String msg = new String(body, StandardCharsets.UTF_8);
      Throwable exception;
      switch (statusCode) {
        case VeniceClientRateExceededException.HTTP_TOO_MANY_REQUESTS:
          exception = new VeniceClientRateExceededException(msg);
          break;
        default:
          exception = new VeniceClientHttpException(msg, statusCode);
          break;
      }
      valueFuture.completeExceptionally(exception);
    }
  }
}
