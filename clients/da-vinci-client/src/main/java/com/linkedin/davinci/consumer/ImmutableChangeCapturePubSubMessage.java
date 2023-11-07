package com.linkedin.davinci.consumer;

import static com.linkedin.venice.writer.VeniceWriter.EMPTY_MSG_HEADERS;

import com.linkedin.venice.pubsub.adapter.kafka.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubMessageHeaders;
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
      long offset,
      long timestamp,
      int payloadSize,
      boolean isEndOfBootstrap) {
    this.key = key;
    this.value = value;
    this.topicPartition = Objects.requireNonNull(topicPartition);
    this.timestamp = timestamp;
    this.payloadSize = payloadSize;
    this.offset = new VeniceChangeCoordinate(
        this.topicPartition.getPubSubTopic().getName(),
        new ApacheKafkaOffsetPosition(offset),
        this.topicPartition.getPartitionNumber());
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
  public PubSubMessageHeaders getPubSubMessageHeaders() {
    return EMPTY_MSG_HEADERS;
  }
}
