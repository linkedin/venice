package com.linkedin.venice.pubsub;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.memory.ClassSizeEstimator;
import com.linkedin.venice.memory.InstanceSizeEstimator;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubMessageHeaders;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import java.util.Objects;


public class ImmutablePubSubMessage implements DefaultPubSubMessage {
  private static final int SHALLOW_CLASS_OVERHEAD = ClassSizeEstimator.getClassOverhead(ImmutablePubSubMessage.class);
  private final KafkaKey key;
  private final KafkaMessageEnvelope value;
  private final PubSubTopicPartition topicPartition;
  private final PubSubPosition pubSubPosition;
  private final long timestamp;
  private final int payloadSize;

  private final PubSubMessageHeaders pubSubMessageHeaders;

  public ImmutablePubSubMessage(
      KafkaKey key,
      KafkaMessageEnvelope value,
      PubSubTopicPartition topicPartition,
      PubSubPosition pubSubPosition,
      long timestamp,
      int payloadSize) {
    this(key, value, topicPartition, pubSubPosition, timestamp, payloadSize, null);
  }

  public ImmutablePubSubMessage(
      KafkaKey key,
      KafkaMessageEnvelope value,
      PubSubTopicPartition topicPartition,
      PubSubPosition pubSubPosition,
      long timestamp,
      int payloadSize,
      PubSubMessageHeaders pubSubMessageHeaders) {
    this.key = key;
    this.value = value;
    this.topicPartition = Objects.requireNonNull(topicPartition);
    this.pubSubPosition = pubSubPosition;
    this.timestamp = timestamp;
    this.payloadSize = payloadSize;
    this.pubSubMessageHeaders = pubSubMessageHeaders;
  }

  @Override
  public KafkaKey getKey() {
    return key;
  }

  @Override
  public KafkaMessageEnvelope getValue() {
    return value;
  }

  @Override
  public PubSubTopicPartition getTopicPartition() {
    return topicPartition;
  }

  @Override
  public PubSubPosition getPosition() {
    return pubSubPosition;
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
    return "PubSubMessage{" + topicPartition + ", position=" + pubSubPosition + ", timestamp=" + timestamp + '}';
  }

  @Override
  public int getHeapSize() {
    /** The {@link #topicPartition} is supposed to be a shared instance, and is therefore ignored. */
    int size = SHALLOW_CLASS_OVERHEAD + InstanceSizeEstimator.getObjectSize(key)
        + InstanceSizeEstimator.getObjectSize(value) + InstanceSizeEstimator.getObjectSize(pubSubPosition);
    /*
     * pubSubMessageHeaders can hold non-trivial bytes (e.g. 'vpm' view-partition-map values
     * or future custom keys). It is captured at construction and not mutated thereafter, so
     * it is safe to include here. Without this, downstream capacity accounting (e.g.
     * StoreBufferService.QueueNode.getHeapSize, which delegates to this method) underestimates
     * per-record retention and the buffer queue can grow well past its byte budget.
     */
    if (pubSubMessageHeaders != null) {
      size += pubSubMessageHeaders.getHeapSize();
    }
    return size;
  }
}
