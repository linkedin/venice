package com.linkedin.venice.client.store.transport;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.exceptions.VeniceServerException;
import com.linkedin.venice.client.serializer.RecordDeserializer;
import com.linkedin.venice.client.store.ClientHttpCallback;
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

  protected final ClientHttpCallback callback;

  private TransportClientCallback(CompletableFuture<T> valueFuture, DeserializerFetcher<T> fetcher, ClientHttpCallback callback, boolean needRawResult) {
    this.valueFuture = valueFuture;
    this.deserializerFetcher = fetcher;
    this.callback = callback;
    this.needRawResult = needRawResult;
  }

  public TransportClientCallback(CompletableFuture<T> valueFuture, DeserializerFetcher<T> fetcher, ClientHttpCallback callback) {
    this(valueFuture, fetcher, callback, false);
  }

  public TransportClientCallback(CompletableFuture<T> valueFuture) {
    this(valueFuture, null, NO_OP_CALLBACK, true);
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

    if (statusCode == HttpStatus.SC_OK) {
      if (needRawResult) {
        valueFuture.complete((T) body);
      } else {
        try {
          RecordDeserializer<T> deserializer = deserializerFetcher.fetch(Integer.parseInt(schemaId));
          T result = deserializer.deserialize(body);
          valueFuture.complete(result);
        } catch (Exception e) {
          valueFuture.completeExceptionally(e);
        } finally {
          callback.executeOnSuccess();
        }
      }
    } else {
      callback.executeOnError(statusCode);
      switch (statusCode) {
        case HttpStatus.SC_NOT_FOUND:
          valueFuture.complete(null);
          break;
        case HttpStatus.SC_INTERNAL_SERVER_ERROR:
        case HttpStatus.SC_SERVICE_UNAVAILABLE:
          if (msg != null) {
            valueFuture.completeExceptionally(new VeniceServerException(msg));
          } else {
            valueFuture.completeExceptionally(new VeniceServerException());
          }
          break;
        case HttpStatus.SC_BAD_REQUEST:
        default:
          if (msg != null) {
            valueFuture.completeExceptionally(new VeniceClientException(msg));
          } else {
            valueFuture.completeExceptionally(new VeniceClientException("Router responds with status code: " + statusCode));
          }
      }
    }
  }

  private static final ClientHttpCallback NO_OP_CALLBACK = new ClientHttpCallback() {
    @Override
    public void executeOnSuccess() {
      // no-op
    }

    @Override
    public void executeOnError(int httpStatus) {
      // no-op
    }

    @Override
    public void executeOnError() {
      // no-op
    }
  };
}
