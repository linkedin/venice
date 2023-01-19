package com.linkedin.venice.pubsub.api;

public interface PubSubMessageDeserializer<K, V, OFFSET, INPUT, OUTPUT extends PubSubMessage<K, V, OFFSET>> {
  OUTPUT deserialize(INPUT input, PubSubTopicPartition topicPartition);
}
