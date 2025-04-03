package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;


public class PubSubMessageProcessedResultWrapper {
  private final DefaultPubSubMessage message;
  private PubSubMessageProcessedResult processedResult;
  private long beforeProcessingPerRecordTimestampNs;

  public PubSubMessageProcessedResultWrapper(DefaultPubSubMessage message) {
    this.message = message;
  }

  public DefaultPubSubMessage getMessage() {
    return message;
  }

  public PubSubMessageProcessedResult getProcessedResult() {
    return processedResult;
  }

  public void setProcessedResult(PubSubMessageProcessedResult processedResult) {
    this.processedResult = processedResult;
  }

  public long getBeforeProcessingPerRecordTimestampNs() {
    return beforeProcessingPerRecordTimestampNs;
  }

  public void setBeforeProcessingPerRecordTimestampNs(long beforeProcessingPerRecordTimestampNs) {
    this.beforeProcessingPerRecordTimestampNs = beforeProcessingPerRecordTimestampNs;
  }
}
