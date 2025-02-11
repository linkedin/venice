package com.linkedin.venice.pubsub.adapter;

import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;


/**
 * A simple implementation of PubSubProducerCallback interface for testing purposes.
 */
public class PubSubProducerCallbackSimpleImpl implements PubSubProducerCallback {
  private PubSubProduceResult produceResult;
  private Exception exception;
  private boolean isInvoked;

  @Override
  public void onCompletion(PubSubProduceResult produceResult, Exception exception) {
    this.isInvoked = true;
    this.produceResult = produceResult;
    this.exception = exception;
  }

  public boolean isInvoked() {
    return isInvoked;
  }

  public PubSubProduceResult getProduceResult() {
    return produceResult;
  }

  public Exception getException() {
    return exception;
  }
}
