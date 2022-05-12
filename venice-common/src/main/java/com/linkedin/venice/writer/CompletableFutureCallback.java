package com.linkedin.venice.writer;

import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;


/**
 * Compose a CompletableFuture and Callback together to be a {@code CompletableFutureCallback} type.
 * When the {@code CompletableFutureCallback} is called, the {@code CompletableFuture} internal state will be
 * changed and the callback will be called. The caller can pass a {@code CompletableFutureCallback} to a function
 * accepting a {@code Callback} parameter to get a {@code CompletableFuture} after the function returns.
 */
public class CompletableFutureCallback implements Callback {
  private final CompletableFuture<Void> completableFuture;
  private Callback callback = null;

  public CompletableFutureCallback(CompletableFuture<Void> completableFuture) {
    this.completableFuture = completableFuture;
  }

  @Override
  public void onCompletion(RecordMetadata recordMetadata, Exception e) {
    callback.onCompletion(recordMetadata, e);
    if (e == null) {
      completableFuture.complete(null);
    } else {
      completableFuture.completeExceptionally(e);
    }
  }

  public Callback getCallback() {
    return callback;
  }

  public void setCallback(Callback callback) {
    this.callback = callback;
  }
}
