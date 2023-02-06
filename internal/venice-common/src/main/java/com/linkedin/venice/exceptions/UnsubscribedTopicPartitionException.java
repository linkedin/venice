package com.linkedin.venice.exceptions;

import com.linkedin.venice.pubsub.api.PubSubTopicPartition;


public class UnsubscribedTopicPartitionException extends VeniceException {
  public UnsubscribedTopicPartitionException(PubSubTopicPartition pubSubTopicPartition) {
    super(
        "Topic: " + pubSubTopicPartition.getPubSubTopic().getName() + ", partition: "
            + pubSubTopicPartition.getPartitionNumber() + " is not being subscribed");
  }
}
