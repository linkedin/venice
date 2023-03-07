package com.linkedin.venice.pubsub.consumer;

import com.linkedin.venice.exceptions.UnsubscribedTopicPartitionException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Set;


public interface PubSubConsumer extends AutoCloseable, Closeable {
  void subscribe(PubSubTopicPartition pubSubTopicPartition, long lastReadOffset);

  void unSubscribe(PubSubTopicPartition pubSubTopicPartition);

  void batchUnsubscribe(Set<PubSubTopicPartition> pubSubTopicPartitionSet);

  void resetOffset(PubSubTopicPartition pubSubTopicPartition) throws UnsubscribedTopicPartitionException;

  void close();

  Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> poll(long timeoutMs);

  /**
   * @return True if this consumer has subscribed any pub sub topic partition at all and vice versa.
   */
  boolean hasAnySubscription();

  boolean hasSubscription(PubSubTopicPartition pubSubTopicPartition);

  void pause(PubSubTopicPartition pubSubTopicPartition);

  void resume(PubSubTopicPartition pubSubTopicPartition);

  Set<PubSubTopicPartition> getAssignment();

  /**
   * Get consuming offset lag for a pub sub topic partition
   * @return an offset lag of zero or above if a valid lag was collected by the consumer, or -1 otherwise
   */
  default long getOffsetLag(PubSubTopicPartition pubSubTopicPartition) {
    return -1;
  }

  /**
   * Get the latest offset for a topic partition
   * @return the latest offset (zero or above) if an offset was collected by the consumer, or -1 otherwise
   */
  default long getLatestOffset(PubSubTopicPartition pubSubTopicPartition) {
    return -1;
  }

}
