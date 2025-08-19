package com.linkedin.davinci.consumer;

import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import java.util.Objects;


public class ImmutableChangeCapturePubSubMessage<K, V> implements PubSubMessage<K, V, VeniceChangeCoordinate> {
  private final K key;
  private final V value;
  private final PubSubTopicPartition topicPartition;
  private final VeniceChangeCoordinate offset;
  private final long timestamp;
  private final int payloadSize;
  private final boolean isEndOfBootstrap;

  public ImmutableChangeCapturePubSubMessage(
      K key,
      V value,
      PubSubTopicPartition topicPartition,
      PubSubPosition pubSubPosition,
      long timestamp,
      int payloadSize,
      boolean isEndOfBootstrap,
      long consumerSequenceId) {
    this.key = key;
    this.value = value;
    this.topicPartition = Objects.requireNonNull(topicPartition);
    this.timestamp = timestamp;
    this.payloadSize = payloadSize;
    this.offset = new VeniceChangeCoordinate(
        this.topicPartition.getPubSubTopic().getName(),
        pubSubPosition,
        this.topicPartition.getPartitionNumber(),
        consumerSequenceId);
    this.isEndOfBootstrap = isEndOfBootstrap;
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

  @Override
  public boolean isEndOfBootstrap() {
    return isEndOfBootstrap;
  }

  @Override
  public String toString() {
    return "PubSubMessage{" + topicPartition + ", offset=" + offset + ", timestamp=" + timestamp + ", isEndOfBootstrap="
        + isEndOfBootstrap + '}';
  }

  @Override
  public int getHeapSize() {
    throw new UnsupportedOperationException("getHeapSize is not supported on " + this.getClass().getSimpleName());
  }
}
