package com.linkedin.venice.pubsub.api;

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
   * @return the best-available message timestamp. This is the pub-sub system timestamp when available and
   *         non-zero, otherwise the Venice producer timestamp embedded in the message envelope.
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
}
