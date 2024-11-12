package com.linkedin.venice.writer;

import static com.linkedin.venice.memory.ClassSizeEstimator.getClassOverhead;

import com.linkedin.venice.memory.Measurable;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import java.util.concurrent.CompletableFuture;


/**
 * Compose a CompletableFuture and Callback together to be a {@code CompletableFutureCallback} type.
 * When the {@code CompletableFutureCallback} is called, the {@code CompletableFuture} internal state will be
 * changed and the callback will be called. The caller can pass a {@code CompletableFutureCallback} to a function
 * accepting a {@code Callback} parameter to get a {@code CompletableFuture} after the function returns.
 */
public class CompletableFutureCallback implements PubSubProducerCallback, Measurable {
  private static final int SHALLOW_CLASS_OVERHEAD = getClassOverhead(CompletableFutureCallback.class);
  private static final int COMPLETABLE_FUTURE_SHALLOW_CLASS_OVERHEAD = getClassOverhead(CompletableFuture.class);

  private final CompletableFuture<Void> completableFuture;
  private PubSubProducerCallback callback = null;

  public CompletableFutureCallback(CompletableFuture<Void> completableFuture) {
    this.completableFuture = completableFuture;
  }

  @Override
  public void onCompletion(PubSubProduceResult produceResult, Exception e) {
    callback.onCompletion(produceResult, e);
    if (e == null) {
      completableFuture.complete(null);
    } else {
      completableFuture.completeExceptionally(e);
    }
  }

  public PubSubProducerCallback getCallback() {
    return callback;
  }

  public void setCallback(PubSubProducerCallback callback) {
    this.callback = callback;
  }

  @Override
  public int getHeapSize() {
    int size = SHALLOW_CLASS_OVERHEAD;
    if (this.completableFuture != null) {
      size += COMPLETABLE_FUTURE_SHALLOW_CLASS_OVERHEAD;
    }
    if (this.callback instanceof Measurable) {
      size += ((Measurable) this.callback).getHeapSize();
    }
    return size;
  }
}
