package com.linkedin.venice.pubsub;

import com.linkedin.venice.memory.ClassSizeEstimator;
import com.linkedin.venice.memory.InstanceSizeEstimator;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubMessageHeaders;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import java.util.Objects;


public class ImmutablePubSubMessage<K, V> implements PubSubMessage<K, V, Long> {
  private static final int SHALLOW_CLASS_OVERHEAD = ClassSizeEstimator.getClassOverhead(ImmutablePubSubMessage.class);
  private final K key;
  private final V value;
  private final PubSubTopicPartition topicPartition;
  private final long offset;
  private final long timestamp;
  private final int payloadSize;

  private final PubSubMessageHeaders pubSubMessageHeaders;

  public ImmutablePubSubMessage(
      K key,
      V value,
      PubSubTopicPartition topicPartition,
      long offset,
      long timestamp,
      int payloadSize) {
    this(key, value, topicPartition, offset, timestamp, payloadSize, null);
  }

  public ImmutablePubSubMessage(
      K key,
      V value,
      PubSubTopicPartition topicPartition,
      long offset,
      long timestamp,
      int payloadSize,
      PubSubMessageHeaders pubSubMessageHeaders) {
    this.key = key;
    this.value = value;
    this.topicPartition = Objects.requireNonNull(topicPartition);
    this.offset = offset;
    this.timestamp = timestamp;
    this.payloadSize = payloadSize;
    this.pubSubMessageHeaders = pubSubMessageHeaders;
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

  @Override
  public boolean isEndOfBootstrap() {
    return false;
  }

  @Override
  public PubSubMessageHeaders getPubSubMessageHeaders() {
    return pubSubMessageHeaders;
  }

  @Override
  public String toString() {
    return "PubSubMessage{" + topicPartition + ", offset=" + offset + ", timestamp=" + timestamp + '}';
  }

  @Override
  public int getHeapSize() {
    /** The {@link #topicPartition} is supposed to be a shared instance, and is therefore ignored. */
    return SHALLOW_CLASS_OVERHEAD + InstanceSizeEstimator.getObjectSize(key)
        + InstanceSizeEstimator.getObjectSize(value);
  }
}
