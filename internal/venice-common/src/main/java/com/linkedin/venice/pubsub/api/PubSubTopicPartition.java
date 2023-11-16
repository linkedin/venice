package com.linkedin.venice.pubsub.api;

public interface PubSubTopicPartition {
  /**
   * @return the topic associated with this topic-partition
   */
  PubSubTopic getPubSubTopic();

  /**
   * @return the partition number of this topic-partition
   */
  int getPartitionNumber();

  default String getTopicName() {
    return getPubSubTopic().getName();
  }
}
