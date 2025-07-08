package com.linkedin.venice.writer;

import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import java.util.List;


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

  PubSubProducerCallback getCallback() {
    return callback;
  }

  List<PubSubProducerCallback> getDependentCallbackList() {
    return dependentCallbackList;
  }
}
