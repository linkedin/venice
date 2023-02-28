package com.linkedin.venice.writer;

import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;


/**
 * Wraps another {@link PubSubProducerCallback} and propagates exceptions to it, but swallows successful completions.
 */
class ErrorPropagationCallback implements PubSubProducerCallback {
  private final PubSubProducerCallback callback;

  public ErrorPropagationCallback(PubSubProducerCallback callback) {
    this.callback = callback;
  }

  @Override
  public void onCompletion(PubSubProduceResult produceResult, Exception exception) {
    if (exception != null) {
      callback.onCompletion(null, exception);
    } // else, no-op
  }
}
