package com.linkedin.venice.pubsub;

import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import java.util.Objects;


public class ImmutablePubSubMessage<K, V> implements PubSubMessage<K, V, Long> {
  private final K key;
  private final V value;
  private final PubSubTopicPartition topicPartition;
  private final long offset;
  private final long timestamp;
  private final int payloadSize;

  public ImmutablePubSubMessage(
      K key,
      V value,
      PubSubTopicPartition topicPartition,
      long offset,
      long timestamp,
      int payloadSize) {
    this.key = key;
    this.value = value;
    this.topicPartition = Objects.requireNonNull(topicPartition);
    this.offset = offset;
    this.timestamp = timestamp;
    this.payloadSize = payloadSize;
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
  public Long getOffset() {
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
