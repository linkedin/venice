package com.linkedin.venice.pubsub.kafka;

import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import org.apache.kafka.common.TopicPartition;


public class KafkaTopicPartition implements PubSubTopicPartition {
  private final PubSubTopic topic;
  private final TopicPartition topicPartition;

  KafkaTopicPartition(PubSubTopic topic, TopicPartition topicPartition) {
    this.topic = topic;
    this.topicPartition = topicPartition;
  }

  KafkaTopicPartition(PubSubTopic topic, int partition) {
    this(topic, new TopicPartition(topic.getName(), partition));
  }

  @Override
  public PubSubTopic getPubSubTopic() {
    return topic;
  }

  @Override
  public int getPartitionNumber() {
    return topicPartition.partition();
  }

  public TopicPartition getTopicPartition() {
    return topicPartition;
  }
}
