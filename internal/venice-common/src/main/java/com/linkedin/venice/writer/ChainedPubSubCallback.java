package com.linkedin.venice.writer;

import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import java.util.List;


/**
 * This class bundles a main {@link PubSubProducerCallback} and a list of dependent {@link PubSubProducerCallback}.
 * When it completes either successfully or exceptionally, it will pass on the same result to the main callback and the
 * list of the dependent callbacks.
 * This class is intended to be used inside {@link BatchingVeniceWriter} for compacting buffered messages with the same key.
 */
public class ChainedPubSubCallback implements PubSubProducerCallback {
  private final List<PubSubProducerCallback> dependentCallbackList;
  private final PubSubProducerCallback callback;

  public ChainedPubSubCallback(
      PubSubProducerCallback mainCallback,
      List<PubSubProducerCallback> dependentCallbackList) {
    this.callback = mainCallback;
    this.dependentCallbackList = dependentCallbackList;
  }

  @Override
  public void onCompletion(PubSubProduceResult produceResult, Exception exception) {
    callback.onCompletion(produceResult, exception);
    for (PubSubProducerCallback producerCallback: dependentCallbackList) {
      producerCallback.onCompletion(produceResult, exception);
    }
  }

  /**
   * Insert an internal {@link PubSubProducerCallback} when the main callback and the dependent callbacks are the type
   * of {@link CompletableFutureCallback}
   */
  public void maybeSetInternalCallback(PubSubProducerCallback internalCallback) {
    if (callback instanceof CompletableFutureCallback) {
      ((CompletableFutureCallback) callback).setCallback(internalCallback);
    }
    for (PubSubProducerCallback dependentCallback: dependentCallbackList) {
      if (dependentCallback instanceof CompletableFutureCallback) {
        ((CompletableFutureCallback) dependentCallback).setCallback(internalCallback);
      }
    }
  }

  PubSubProducerCallback getCallback() {
    return callback;
  }

  List<PubSubProducerCallback> getDependentCallbackList() {
    return dependentCallbackList;
  }
}
