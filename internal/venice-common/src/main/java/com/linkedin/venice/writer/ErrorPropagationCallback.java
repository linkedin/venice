package com.linkedin.venice.writer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;


/**
 * Wraps another {@link Callback} and propagates exceptions to it, but swallows successful completions.
 */
class ErrorPropagationCallback implements Callback {
  private final Callback callback;

  public ErrorPropagationCallback(Callback callback) {
    this.callback = callback;
  }

  @Override
  public void onCompletion(RecordMetadata metadata, Exception exception) {
    if (exception != null) {
      callback.onCompletion(null, exception);
    } // else, no-op
  }
}
