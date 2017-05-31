package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;

public abstract class InternalAvroStoreClient<K, V> implements AvroGenericStoreClient<K, V>{

  public abstract CompletableFuture<byte[]> getRaw(String requestPath);

  public static void handleStoreExceptionInternally(Throwable throwable) {
    if (null == throwable) {
      return;
    }
    /**
     * {@link CompletionException} could be thrown by {@link CompletableFuture#handle(BiFunction)}
     *
     * Eventually, {@link CompletableFuture#get()} will throw {@link ExecutionException}, which will replace
     * {@link CompletionException}, and its cause is the real root cause instead of {@link CompletionException}
     */
    if (throwable instanceof CompletionException) {
      throw (CompletionException)throwable;
    }
    /**
     * {@link VeniceClientException} could be thrown by {@link CompletableFuture#completeExceptionally(Throwable)}
     */
    if (throwable instanceof VeniceClientException) {
      throw (VeniceClientException)throwable;
    }
    throw new VeniceClientException(throwable);
  }
}
