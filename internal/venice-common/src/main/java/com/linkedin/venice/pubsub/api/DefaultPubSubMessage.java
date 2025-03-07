package com.linkedin.venice.pubsub.api;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;


/**
 * A default implementation of {@link PubSubMessage} that represents a message in a pub-sub system
 * with a {@link KafkaKey} as the key, a {@link KafkaMessageEnvelope} as the value, and a
 * {@link PubSubPosition} to track the message's offset within the topic-partition.
 */
public interface DefaultPubSubMessage extends PubSubMessage<KafkaKey, KafkaMessageEnvelope, PubSubPosition> {
  /**
   * Retrieves the key associated with this message.
   *
   * @return the {@link KafkaKey} representing the message key.
   */
  KafkaKey getKey();

  /**
   * Retrieves the value payload of this message.
   *
   * @return the {@link KafkaMessageEnvelope} containing the message data.
   */
  KafkaMessageEnvelope getValue();

  /**
   * Retrieves the position of this message within the underlying topic-partition.
   *
   * @return the {@link PubSubPosition} representing the message offset.
   */
  PubSubPosition getPosition();

  /**
   * @Deprecated use {@link #getPosition()} instead.
   * @return position of this message within the underlying topic-partition.
   */
  default PubSubPosition getOffset() {
    return getPosition();
  }
}
