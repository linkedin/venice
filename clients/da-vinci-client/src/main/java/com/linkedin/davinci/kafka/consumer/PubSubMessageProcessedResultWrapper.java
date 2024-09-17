package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.pubsub.api.PubSubMessage;


public class PubSubMessageProcessedResultWrapper<K, V, OFFSET> {
  private final PubSubMessage<K, V, OFFSET> message;
  private PubSubMessageProcessedResult processedResult;

  public PubSubMessageProcessedResultWrapper(PubSubMessage<K, V, OFFSET> message) {
    this.message = message;
  }

  public PubSubMessage<K, V, OFFSET> getMessage() {
    return message;
  }

  public PubSubMessageProcessedResult getProcessedResult() {
    return processedResult;
  }

  public void setProcessedResult(PubSubMessageProcessedResult transformedResult) {
    this.processedResult = transformedResult;
  }
}
