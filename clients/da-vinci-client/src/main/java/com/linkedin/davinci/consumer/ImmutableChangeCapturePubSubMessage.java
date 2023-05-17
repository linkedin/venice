package com.linkedin.davinci.consumer;

import com.linkedin.venice.pubsub.adapter.kafka.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import java.util.Objects;


public class ImmutableChangeCapturePubSubMessage<K, V> implements PubSubMessage<K, V, VeniceChangeCoordinate> {
  private final K key;
  private final V value;
  private final PubSubTopicPartition topicPartition;
  private final VeniceChangeCoordinate offset;
  private final long timestamp;
  private final int payloadSize;

  public ImmutableChangeCapturePubSubMessage(
      K key,
      V value,
      PubSubTopicPartition topicPartition,
      long offset,
      long timestamp,
      int payloadSize) {
    this.key = key;
    this.value = value;
    this.topicPartition = Objects.requireNonNull(topicPartition);
    this.timestamp = timestamp;
    this.payloadSize = payloadSize;
    this.offset = new VeniceChangeCoordinate(
        this.topicPartition.getPubSubTopic().getName(),
        new ApacheKafkaOffsetPosition(offset),
        this.topicPartition.getPartitionNumber());
  }

  @Override
  public K getKey() {
    return key;
  }

  @Override
  public V getValue() {
    return value;
  }

  @Override
  public PubSubTopicPartition getTopicPartition() {
    return topicPartition;
  }

  @Override
  public VeniceChangeCoordinate getOffset() {
    return offset;
  }

  @Override
  public long getPubSubMessageTime() {
    return timestamp;
  }

  @Override
  public int getPayloadSize() {
    return payloadSize;
  }
}
