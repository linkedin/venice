package com.linkedin.venice.pubsub.api.exceptions;

import com.linkedin.venice.pubsub.api.PubSubTopicPartition;


public class PubSubUnsubscribedTopicPartitionException extends PubSubClientException {
  public PubSubUnsubscribedTopicPartitionException(PubSubTopicPartition pubSubTopicPartition) {
    super(
        "Topic: " + pubSubTopicPartition.getPubSubTopic().getName() + ", partition: "
            + pubSubTopicPartition.getPartitionNumber() + " has not been subscribed");
  }
}
