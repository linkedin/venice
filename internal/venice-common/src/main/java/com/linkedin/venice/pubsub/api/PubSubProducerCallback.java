package com.linkedin.venice.pubsub.api;

import java.util.concurrent.CompletableFuture;


/**
 * A callback interface that users of PubSubProducerAdapter should implement if they want
 * to execute some code once PubSubProducerAdapter#sendMessage request is completed.
 */
public abstract class PubSubProducerCallback extends CompletableFuture<PubSubProduceResult> {
  /**
   * exception will be null if request was completed without an error.
   */
  public abstract void onCompletion(PubSubProduceResult produceResult, Exception exception);
}
