package com.linkedin.venice.exceptions;

public class UnsubscribedTopicPartitionException extends VeniceException {
  public UnsubscribedTopicPartitionException(String topic, int partition) {
    super("Topic: " + topic + ", partition: " + partition + " is not being subscribed");
  }
}
