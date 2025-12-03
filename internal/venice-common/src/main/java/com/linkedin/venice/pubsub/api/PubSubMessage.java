package com.linkedin.venice.pubsub.api;

import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.memory.Measurable;


public interface PubSubMessage<K, V, POSITION> extends Measurable {
  /**
   * @return the key part of this message
   */
  K getKey();

  /**
   * @return the value part of this message
   */
  V getValue();

  /**
   * @return the topic-partition this message belongs to
   */
  PubSubTopicPartition getTopicPartition();

  /**
   * @return the offset of this message in the underlying topic-partition
   * @Deprecated use {@link #getPosition()} instead.
   */
  default POSITION getOffset() {
    return getPosition();
  }

  POSITION getPosition();

  /**
   * @return the timestamp at which the message was persisted in the pub sub system
   */
  long getPubSubMessageTime();

  /**
   * @return the size in bytes of the key + value.
   */
  int getPayloadSize();

  default String getTopicName() {
    return getTopicPartition().getPubSubTopic().getName();
  }

  default PubSubTopic getTopic() {
    return getTopicPartition().getPubSubTopic();
  }

  default int getPartition() {
    return getTopicPartition().getPartitionNumber();
  }

  /**
   * @return whether this message marks the end of bootstrap.
   */
  boolean isEndOfBootstrap();

  default PubSubMessageHeaders getPubSubMessageHeaders() {
    return EmptyPubSubMessageHeaders.SINGLETON;
  }

  /**
   * If this message carries a control message, return it. Otherwise, return null.
   */
  default ControlMessage getControlMessage() {
    return null;
  }
}
