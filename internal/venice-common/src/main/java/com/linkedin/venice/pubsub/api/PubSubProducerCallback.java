package com.linkedin.venice.pubsub.api;

/**
 * A callback interface that users of PubSubProducerAdapter should implement if they want
 * to execute some code once PubSubProducerAdapter#sendMessage request is completed.
 */
public interface PubSubProducerCallback {
  /**
   * exception will be null if request was completed without an error.
   */

  void onCompletion(PubSubProduceResult produceResult, Exception exception);

}
