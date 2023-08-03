package com.linkedin.venice.pubsub.api.exceptions;

public class PubSubUnknownTopicOrPartitionException extends PubSubClientRetriableException {
  public PubSubUnknownTopicOrPartitionException(String topicName, int partition, Throwable cause) {
    super(String.format("Topic: %s partition: %d is unknown or does not exists", topicName, partition), cause);
  }

  public PubSubUnknownTopicOrPartitionException(String message) {
    super(message);
  }

  public PubSubUnknownTopicOrPartitionException(String message, Throwable cause) {
    super(message, cause);
  }
}
