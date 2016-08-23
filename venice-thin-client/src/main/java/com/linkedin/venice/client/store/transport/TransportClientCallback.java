package com.linkedin.venice.client.store.transport;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.exceptions.VeniceServerException;
import com.linkedin.venice.client.serializer.RecordDeserializer;
import com.linkedin.venice.client.store.ClientCallback;
import com.linkedin.venice.client.store.DeserializerFetcher;
import org.apache.http.HttpStatus;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

/**
 * Define the common functions for call back of {@link TransportClient}
 * @param <T>
 */
public class TransportClientCallback<T> {
  public static String HEADER_VENICE_SCHEMA_ID = "X-VENICE-SCHEMA-ID";

  private final CompletableFuture<T> valueFuture;
  private final DeserializerFetcher<T> deserializerFetcher;
  private final boolean needRawResult;

  protected ClientCallback callback;

  public TransportClientCallback(CompletableFuture<T> valueFuture, DeserializerFetcher<T> fetcher, ClientCallback callback) {
    this.valueFuture = valueFuture;
    this.deserializerFetcher = fetcher;
    this.needRawResult = false;
    this.callback = callback;
  }

  public TransportClientCallback(CompletableFuture<T> valueFuture) {
    this.valueFuture = valueFuture;
    this.needRawResult = true;
    this.deserializerFetcher = null;
  }

  protected boolean isNeedRawResult() {
    return needRawResult;
  }

  protected CompletableFuture<T> getValueFuture() {
    return valueFuture;
  }

  public void completeFuture(int statusCode,
                             byte[] body,
                             String schemaId) {
    String msg = new String(body, StandardCharsets.UTF_8);

    switch (statusCode) {
      case HttpStatus.SC_OK:
        if (needRawResult) {
          valueFuture.complete((T) body);
        } else {
          try {
            RecordDeserializer<T> deserializer = deserializerFetcher.fetch(Integer.parseInt(schemaId));
            T result = deserializer.deserialize(body);
            valueFuture.complete(result);
          } catch (VeniceClientException e) {
            valueFuture.completeExceptionally(e);
          } finally {
            callback.executeOnSuccess();
          }
        }
        break;
      case HttpStatus.SC_NOT_FOUND:
        valueFuture.complete(null);
        callback.executeOnSuccess();
        break;
      case HttpStatus.SC_INTERNAL_SERVER_ERROR:
      case HttpStatus.SC_SERVICE_UNAVAILABLE:
        if (msg != null) {
          valueFuture.completeExceptionally(new VeniceServerException(msg));
        } else {
          valueFuture.completeExceptionally(new VeniceServerException());
        }
        callback.executeOnError();
        break;
      case HttpStatus.SC_BAD_REQUEST:
      default:
        if (msg != null) {
          valueFuture.completeExceptionally(new VeniceClientException(msg));
        } else {
          valueFuture
              .completeExceptionally(new VeniceClientException("Router responds with status code: " + statusCode));
        }
        callback.executeOnError();
    }
  }
}
